# multy_threading.py
import os
import mmap
import struct
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Iterator, Any

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
START_SYNC_MARKER = b'\xA3\x95'
FMT_MSG_TYPE = 128
FMT_LENGTH = 89
DEFAULT_BLOCK_SIZE = 10 * 1024 * 1024   # 10 MiB – tune if you like

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
    """Fast ASCII decode + strip NUL bytes."""
    return b.decode('ascii', errors='ignore').rstrip('\x00')


def _ap_fmt_to_struct(fmt_chars: str) -> str:
    """Convert ArduPilot format string → struct format string."""
    return '<' + ''.join(AP_TO_STRUCT.get(c, '') for c in fmt_chars)


# ----------------------------------------------------------------------
# Thread worker – runs inside the ThreadPoolExecutor
# ----------------------------------------------------------------------
def _process_block(
    path: str,
    start: int,
    end: int,
    fmt_cache: Dict[int, Dict[str, Any]],
    wanted_type: int | None,
) -> List[Dict[str, Any]]:
    """Parse one block of the file. Returns a plain list of messages."""
    messages: List[Dict[str, Any]] = []

    # Pre-compile struct objects for this block (very cheap)
    structs = {
        typ: struct.Struct(info["struct_fmt"])
        for typ, info in fmt_cache.items()
    }

    with open(path, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
        pos = start
        while pos < end:
            pos = mm.find(START_SYNC_MARKER, pos)
            if pos == -1 or pos + 3 > end:
                break

            msg_type = mm[pos + 2]
            info = fmt_cache.get(msg_type)
            if info is None:                     # unknown → skip one byte
                pos += 1
                continue

            if wanted_type is not None and wanted_type != msg_type:
                pos += info["Length"]
                continue

            # unpack
            try:
                values = structs[msg_type].unpack_from(mm, pos + 3)
            except struct.error:
                pos += 1
                continue

            msg = dict(zip(info["columns"], values))

            # decode string fields
            for col, val in msg.items():
                if isinstance(val, bytes) and col not in BINARY_FIELDS:
                    msg[col] = _decode_str(val)

            # scaling
            for i, col in enumerate(info["columns"]):
                val = msg[col]
                if not isinstance(val, (int, float)):
                    continue
                fmt_char = info["format_chars"][i]
                if fmt_char in CHAR_TO_MULTIPLE:
                    msg[col] = val / 100.0
                elif fmt_char == 'L':
                    msg[col] = val / 1e7

            msg["mavpackettype"] = info["name"]
            messages.append(msg)

            pos += info["Length"]

    return messages


# ----------------------------------------------------------------------
# Public class – ThreadPool version
# ----------------------------------------------------------------------
class ParserThreadPool:
    def __init__(self, path: str, block_size: int = DEFAULT_BLOCK_SIZE, max_workers: int = 6):
        self.path = os.path.abspath(path)
        self.block_size = block_size
        self.max_workers = max_workers
        self._fmt_cache: Dict[int, Dict[str, Any]] = {}
        self._build_fmt_cache()                     # fills self._fmt_cache

    # ------------------------------------------------------------------
    # Public generator
    # ------------------------------------------------------------------
    def recv_match(self, msg_name: str | None = None) -> Iterator[Dict[str, Any]]:
        """
        Yield messages in *file order*.
        If *msg_name* is given, only messages of that type are returned.
        """
        wanted_type = None
        if msg_name:
            for typ, info in self._fmt_cache.items():
                if info["name"] == msg_name:
                    wanted_type = typ
                    break

        blocks = self._make_blocks()

        # Build a *picklable* version of the cache (only raw data, no struct objects)
        fmt_cache_raw = {
            typ: {
                "Length": info["Length"],
                "name": info["name"],
                "columns": info["columns"],
                "format_chars": info["format_chars"],
                "struct_fmt": info["struct_fmt"],      # pre-computed format string
            }
            for typ, info in self._fmt_cache.items()
        }

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all blocks – keep mapping future → block index
            future_to_idx = {
                executor.submit(
                    _process_block,
                    self.path,
                    start,
                    end,
                    fmt_cache_raw,
                    wanted_type,
                ): idx
                for idx, (start, end) in enumerate(blocks, start=1)
            }

            waiting: Dict[int, List[Dict[str, Any]]] = {}
            expected_idx = 1
            finished = 0
            total = len(blocks)

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                waiting[idx] = future.result()

                # Emit blocks as soon as they are the next expected one
                while expected_idx in waiting:
                    for msg in waiting.pop(expected_idx):
                        yield msg
                    expected_idx += 1
                    finished += 1

    # ------------------------------------------------------------------
    # Build FMT cache (once at init)
    # ------------------------------------------------------------------
    def _build_fmt_cache(self) -> None:
        marker = START_SYNC_MARKER + bytes([FMT_MSG_TYPE])   # b'\xA3\x95\x80'
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

                struct_fmt = _ap_fmt_to_struct(fmt_raw)

                self._fmt_cache[typ] = {
                    "Length": length,
                    "name": name,
                    "struct_fmt": struct_fmt,
                    "columns": cols_raw.split(','),
                    "format_chars": list(fmt_raw),
                }
                pos += FMT_LENGTH

        # Add the FMT message definition itself
        if FMT_MSG_TYPE not in self._fmt_cache:
            self._fmt_cache[FMT_MSG_TYPE] = {
                "Length": FMT_LENGTH,
                "name": "FMT",
                "struct_fmt": "<BB4s16s64s",
                "columns": ["Type", "Length", "Name", "Format", "Columns"],
                "format_chars": [],
            }

    # ------------------------------------------------------------------
    # Split file into safe blocks (ends on a sync marker)
    # ------------------------------------------------------------------
    def _make_blocks(self) -> List[Tuple[int, int]]:
        file_size = os.path.getsize(self.path)
        blocks: List[Tuple[int, int]] = []
        start = 0

        with open(self.path, "rb") as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            while start < file_size:
                end = min(start + self.block_size, file_size)
                nxt = mm.find(START_SYNC_MARKER, end)
                if nxt != -1:
                    end = nxt
                blocks.append((start, end))
                start = end

        return blocks