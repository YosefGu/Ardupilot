import os
import mmap
import struct
from multiprocessing import Pool, cpu_count
from typing import Dict, List, Tuple, Iterator, Any
from functools import partial


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
START_SYNC_MARKER = b'\xA3\x95'
FMT_MSG_TYPE = 128
FMT_LENGTH = 89
BLOCK_SIZE = 15 * 1024 * 1024  # 15 MiB

AP_TO_STRUCT = {
    'a': '32h', 'b': 'b', 'B': 'B', 'h': 'h', 'H': 'H',
    'i': 'i', 'I': 'I', 'f': 'f', 'd': 'd',
    'n': '4s', 'N': '16s', 'Z': '64s',
    'c': 'h', 'C': 'H', 'e': 'i', 'E': 'I',
    'L': 'i', 'M': 'B', 'q': 'q', 'Q': 'Q',
}
CHAR_TO_MULTIPLE = {'c', 'C', 'e', 'E'}
BINARY_FIELDS = {"Data", "Data0", "Data1"}


def _decode_str(b: bytes) -> str:
    """Fast ASCII decode + strip NULs."""
    return b.decode('ascii', errors='ignore').rstrip('\x00')


def _ap_fmt_to_struct(fmt_chars: List[str]) -> str:
    """Convert list of format chars → struct format string."""
    return '<' + ''.join(AP_TO_STRUCT.get(c, '') for c in fmt_chars)


# ----------------------------------------------------------------------
# Worker function – runs in child process
# ----------------------------------------------------------------------
def _process_block(
    path: str,
    start: int,
    end: int,
    fmt_cache_raw: Dict[int, Dict[str, Any]],
    wanted_type: int | None,
) -> List[Dict[str, Any]]:
    """
    Parse one block of the file.
    Rebuilds struct.Struct objects locally to avoid pickling.
    """
    messages: List[Dict[str, Any]] = []

    # Rebuild struct objects inside worker (not picklable across processes)
    fmt_cache = {}
    for typ, info in fmt_cache_raw.items():
        struct_fmt = _ap_fmt_to_struct(info["format_chars"])
        fmt_cache[typ] = {
            "Length": info["Length"],
            "name": info["name"],
            "struct_obj": struct.Struct(struct_fmt),
            "columns": info["columns"],
            "format_chars": info["format_chars"],
        }

    with open(path, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
        pos = start
        while pos < end:
            pos = mm.find(START_SYNC_MARKER, pos)
            if pos == -1 or pos + 3 > end:
                break

            msg_type = mm[pos + 2]
            fmt = fmt_cache.get(msg_type)
            if fmt is None:
                pos += 1
                continue

            if wanted_type is not None and wanted_type != msg_type:
                pos += fmt["Length"]
                continue

            try:
                values = fmt["struct_obj"].unpack_from(mm, pos + 3)
            except struct.error:
                pos += 1
                continue

            msg = dict(zip(fmt["columns"], values))

            # Decode string fields
            for col, val in msg.items():
                if isinstance(val, bytes) and col not in BINARY_FIELDS:
                    msg[col] = _decode_str(val)

            # Apply scaling
            for i, col in enumerate(fmt["columns"]):
                val = msg[col]
                if not isinstance(val, (int, float)):
                    continue
                fmt_char = fmt["format_chars"][i]
                if fmt_char in CHAR_TO_MULTIPLE:
                    msg[col] = val / 100.0
                elif fmt_char == 'L':
                    msg[col] = val / 1e7

            msg["mavpackettype"] = fmt["name"]
            messages.append(msg)

            pos += fmt["Length"]

    return messages


# ----------------------------------------------------------------------
# Main Parser Class
# ----------------------------------------------------------------------
class ParserMultiprocessing:
    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        self._fmt_cache: Dict[int, Dict[str, Any]] = {}
        self._build_fmt_cache()

    # ------------------------------------------------------------------
    # Public API: Generator for messages
    # ------------------------------------------------------------------
    def recv_match(self, msg_name: str | None = None) -> Iterator[Dict[str, Any]]:
        """
        Yield messages in file order.
        If msg_name is given, only return messages of that type.
        """
        wanted_type = None
        if msg_name:
            for typ, info in self._fmt_cache.items():
                if info["name"] == msg_name:
                    wanted_type = typ
                    break

        blocks = self._make_blocks()

        # Picklable version of fmt_cache (no struct objects)
        fmt_cache_raw = {
            typ: {
                "Length": info["Length"],
                "name": info["name"],
                "columns": info["columns"],
                "format_chars": info["format_chars"],
            }
            for typ, info in self._fmt_cache.items()
        }

        # Use apply_async to avoid partial/None hacks
        with Pool(processes=cpu_count()) as pool:
            futures = [
                pool.apply_async(
                    _process_block,
                    args=(self.path, start, end, fmt_cache_raw, wanted_type)
                )
                for start, end in blocks
            ]
            # Collect results in order
            for future in futures:
                yield from future.get()

    # ------------------------------------------------------------------
    # Build FMT message cache (runs once at init)
    # ------------------------------------------------------------------
    def _build_fmt_cache(self) -> None:
        marker = START_SYNC_MARKER + bytes([FMT_MSG_TYPE])  # b'\xA3\x95\x80'
        fmt_struct = struct.Struct("<BB4s16s64s")

        with open(self.path, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            pos = 0
            while True:
                pos = mm.find(marker, pos)
                if pos == -1:
                    break
                try:
                    typ, length, name_b, fmt_b, cols_b = fmt_struct.unpack_from(mm, pos + 3)
                except struct.error:
                    pos += 1
                    continue

                name = _decode_str(name_b)
                if not name.isalnum():
                    pos += 1
                    continue

                fmt_raw = _decode_str(fmt_b)
                cols_raw = _decode_str(cols_b)

                struct_fmt = _ap_fmt_to_struct(list(fmt_raw))
                struct_obj = struct.Struct(struct_fmt)

                self._fmt_cache[typ] = {
                    "Length": length,
                    "name": name,
                    "struct_obj": struct_obj,
                    "columns": cols_raw.split(','),
                    "format_chars": list(fmt_raw),
                }
                pos += FMT_LENGTH

        # Add FMT message itself
        if FMT_MSG_TYPE not in self._fmt_cache:
            self._fmt_cache[FMT_MSG_TYPE] = {
                "Length": FMT_LENGTH,
                "name": "FMT",
                "struct_obj": fmt_struct,
                "columns": ["Type", "Length", "Name", "Format", "Columns"],
                "format_chars": [],
            }

    # ------------------------------------------------------------------
    # Split file into safe blocks (ends on sync marker)
    # ------------------------------------------------------------------
    def _make_blocks(self) -> List[Tuple[int, int]]:
        file_size = os.path.getsize(self.path)
        blocks: List[Tuple[int, int]] = []
        start = 0

        with open(self.path, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            while start < file_size:
                end = min(start + BLOCK_SIZE, file_size)
                next_marker = mm.find(START_SYNC_MARKER, end)
                if next_marker != -1:
                    end = next_marker
                blocks.append((start, end))
                start = end

        return blocks