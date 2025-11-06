import pytest
from typing import Any, Dict, List, Tuple
import os
import tempfile

from business_logic.multi_processing import (
    ParserMultiprocessing,
    _ap_fmt_to_struct,
    _apply_scaling_and_decode,
    _decode_str,
)

def test_ap_fmt_to_struct(subtests: Any) -> None:
    from config import AP_TO_STRUCT

    all_chars: List[str] = list(AP_TO_STRUCT.keys())
    assert all_chars, "AP_TO_STRUCT is empty!"

    with subtests.test("Starts with '<'"):
        result: str = _ap_fmt_to_struct(all_chars)
        assert result.startswith("<"), f"Got: {result!r}"

    with subtests.test("Returns string"):
        result = _ap_fmt_to_struct(["b", "i"])
        assert isinstance(result, str)
        assert result == "<bi", f"Expected '<bi', got {result!r}"

    with subtests.test("Handles all known chars"):
        expected = "<" + "".join(AP_TO_STRUCT.get(c, "") for c in all_chars)
        result = _ap_fmt_to_struct(all_chars)
        assert result == expected

    with subtests.test("Ignores unknown char"):
        result = _ap_fmt_to_struct(["b", "X", "i"])  # X לא קיים
        assert result == "<bi"  # ← רק b ו-i


def test_decode_str(subtests: Any) -> None:
    with subtests.test("Regular string with nulls"):
        assert _decode_str(b"Hello\x00\x00") == "Hello"

    with subtests.test("String without nulls"):
        assert _decode_str(b"World") == "World"

    with subtests.test("String with non-ASCII bytes"):
        assert _decode_str(b"TEST\x00\x00") == "TEST"

    with subtests.test("Empty byte string"):
        assert _decode_str(b"") == ""

    with subtests.test("String with only null bytes"):
        assert _decode_str(b"\x00\x00\x00") == ""

    with subtests.test("String with mixed content"):
        assert _decode_str(b"\x00Hello\x00World\x00") == "\x00Hello\x00World"


def test_apply_scaling_and_decode(subtests: Any) -> None:
    from config import BINARY_FIELDS, CHAR_TO_DIVIDE

    with subtests.test("Confirm NUMBERS_TO_DIVIDE is defined"):
        assert CHAR_TO_DIVIDE, "NUMBERS_TO_DIVIDE is empty!"

    with subtests.test("Confirm BINARY_FIELDS is defined"):
        assert BINARY_FIELDS, "BINARY_FIELDS is empty!"

    fmt: Dict[str, Any] = {
        "columns": ["A", "B", "C", "D"],
        "format_chars": ["c", "L", "f", "E"],
    }
    msg: Dict[str, Any] = {"A": 100, "B": 100000000, "C": b"TEST", "D": 5000}
    result = _apply_scaling_and_decode(msg, fmt)

    with subtests.test("Scaling and decoding applied correctly"):
        assert result["A"] == 1.0

    with subtests.test("Scaling for 'L' applied correctly"):
        assert result["B"] == 10.0

    with subtests.test("Decoding for bytes applied correctly"):
        assert result["C"] == "TEST"

    with subtests.test("Scaling and decoding applied correctly"):
        assert result["D"] == 50.0


def test_make_blocks(subtests: Any) -> None:
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_path = tmp_file.name
        tmp_file.write(b"\x00" * (5 * 1024 * 1024))  # 5 MiB קובץ פיקטיבי

    try:
        parser = ParserMultiprocessing(tmp_path)

        with subtests.test("File smaller than BLOCK_SIZE"):
            blocks: List[Tuple[int, int]] = parser._make_blocks()
            file_size = os.path.getsize(tmp_path)
            assert blocks == [(0, file_size)], f"Got: {blocks!r}"

    finally:
        os.remove(tmp_path)


    

   
