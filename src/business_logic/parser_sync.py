# parser_sync.py
import os
import struct
import mmap
from typing import Dict, List, Iterator, Any

from config import (
    START_SYNC_MARKER,
    FMT_MSG_TYPE,
    FMT_LENGTH,
    AP_TO_STRUCT,
    CHAR_TO_MULTIPLE,
    BINARY_FIELDS,
)

def _decode_str(b: bytes) -> str:
    """Fast ASCII decode + strip NULs."""
    return b.decode('ascii', errors='ignore').rstrip('\x00')


# ----------------------------------------------------------------------
# Synchronous Parser – no threads, no processes
# ----------------------------------------------------------------------
class ParserSync:
    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        self.file_size = os.path.getsize(self.path)

        # One mmap for the whole lifetime
        self._file = open(self.path, "rb")
        self._mm = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)

        self._fmt_cache: Dict[int, Dict[str, Any]] = {}
        self._build_fmt_cache()

    def __del__(self):
        try:
            self._mm.close()
            self._file.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Public API
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

        yield from self._parse_all(wanted_type)

    # ------------------------------------------------------------------
    # Build FMT cache (once at init)
    # ------------------------------------------------------------------
    def _build_fmt_cache(self) -> None:
        marker = START_SYNC_MARKER + bytes([FMT_MSG_TYPE])
        fmt_struct = struct.Struct("<BB4s16s64s")

        pos = 0
        while True:
            pos = self._mm.find(marker, pos)
            if pos == -1:
                break
            try:
                typ, length, name_b, fmt_b, cols_b = fmt_struct.unpack_from(self._mm, pos + 3)
            except struct.error:
                pos += 1
                continue

            name = _decode_str(name_b)
            if not name.isalnum():
                pos += 1
                continue

            fmt_raw = _decode_str(fmt_b)
            cols_raw = _decode_str(cols_b)

            struct_fmt = '<' + ''.join(AP_TO_STRUCT.get(c, '') for c in fmt_raw)
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
    # Main parsing loop – fully synchronous, very fast
    # ------------------------------------------------------------------
    def _parse_all(self, wanted_type: int | None) -> Iterator[Dict[str, Any]]:
        pos = 0
        end = self.file_size

        while pos < end:
            pos = self._mm.find(START_SYNC_MARKER, pos)
            if pos == -1 or pos + 3 > end:
                break

            msg_type = self._mm[pos + 2]
            fmt = self._fmt_cache.get(msg_type)
            if fmt is None:
                pos += 1
                continue

            if wanted_type is not None and wanted_type != msg_type:
                pos += fmt["Length"]
                continue

            try:
                values = fmt["struct_obj"].unpack_from(self._mm, pos + 3)
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
            yield msg

            pos += fmt["Length"]