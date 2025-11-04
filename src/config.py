from pathlib import Path
from typing import Dict, List, Set

# ----------------------------------------------------------------------
# File / parsing settings
# ----------------------------------------------------------------------
START_SYNC_MARKER = b'\xA3\x95'          # 2-byte header
FMT_MSG_TYPE = 128                      # Message type of FMT records
FMT_LENGTH = 89                         # Fixed size of a FMT message

# ----------------------------------------------------------------------
# ArduPilot → struct format mapping
# ----------------------------------------------------------------------
AP_TO_STRUCT: Dict[str, str] = {
    'a': '32h',   # int16_t[16]
    'b': 'b',     # int8_t
    'B': 'B',     # uint8_t
    'h': 'h',     # int16_t
    'H': 'H',     # uint16_t
    'i': 'i',     # int32_t
    'I': 'I',     # uint32_t
    'f': 'f',     # float
    'd': 'd',     # double
    'n': '4s',    # char[4]
    'N': '16s',   # char[16]
    'Z': '64s',   # char[64]
    'c': 'h',     # int16_t[100]
    'C': 'H',     # uint16_t[100]
    'e': 'i',     # int32_t[100]
    'E': 'I',     # uint32_t[100]
    'L': 'i',     # lat/lon (scaled)
    'M': 'B',     # flight mode
    'q': 'q',     # int64_t
    'Q': 'Q',     # uint64_t
}

# ----------------------------------------------------------------------
# Scaling & special handling
# ----------------------------------------------------------------------
CHAR_TO_MULTIPLE: Set[str] = {'c', 'C', 'e', 'E'}   # divide by 100
BINARY_FIELDS: Set[str] = {"Data", "Data0", "Data1"}   # keep raw bytes

# ----------------------------------------------------------------------
# Default parser behaviour
# ----------------------------------------------------------------------
DEFAULT_BLOCK_SIZE = 15 * 1024 * 1024   # 15 MiB
DEFAULT_MAX_WORKERS = 6                # for threaded version