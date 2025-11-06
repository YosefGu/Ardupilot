import pytest
from business_logic.multy_processing import (
    ParserMultiprocessing,
    _decode_str, 
    _ap_fmt_to_struct, 
    _apply_scaling_and_decode
)

# def test_ap_fmt_to_struct(subtests):
#     from config import AP_TO_STRUCT

#     all_chars = list(AP_TO_STRUCT.keys())
#     assert all_chars, "AP_TO_STRUCT is empty!"

#     with subtests.test("Starts with '<'"):
#         result = _ap_fmt_to_struct(all_chars)
#         assert result.startswith('<'), f"Got: {result!r}"

#     with subtests.test("Returns string"):
#         result = _ap_fmt_to_struct(['b', 'i'])
#         assert isinstance(result, str)
#         assert result == '<bi', f"Expected '<bi', got {result!r}"

#     with subtests.test("Handles all known chars"):
#         expected = '<' + ''.join(AP_TO_STRUCT.get(c, '') for c in all_chars)
#         result = _ap_fmt_to_struct(all_chars)
#         assert result == expected

#     with subtests.test("Ignores unknown char"):
#         result = _ap_fmt_to_struct(['b', 'X', 'i'])  # X לא קיים
#         assert result == '<bi'  # ← רק b ו-i

# def test_decode_str(subtests):

#     with subtests.test("Regular string with nulls"):
#         assert _decode_str(b"Hello\x00\x00") == "Hello"

#     with subtests.test("String without nulls"):
#         assert _decode_str(b"World") == "World"

#     with subtests.test("String with non-ASCII bytes"):
#         assert _decode_str(b"TEST\x00\x00") == "TEST"

#     with subtests.test("Empty byte string"):
#         assert _decode_str(b"") == ""

#     with subtests.test("String with only null bytes"):
#         assert _decode_str(b"\x00\x00\x00") == ""

#     with subtests.test("String with mixed content"):
#         assert _decode_str(b"\x00Hello\x00World\x00") == "\x00Hello\x00World"       
   
# def test_apply_scaling_and_decode(subtests):
#     from config import NUMBERS_TO_DIVIDE, BINARY_FIELDS
    
#     with subtests.test("Confirm NUMBERS_TO_DIVIDE is defined"):
#         assert NUMBERS_TO_DIVIDE, "NUMBERS_TO_DIVIDE is empty!"

#     with subtests.test("Confirm BINARY_FIELDS is defined"):
#         from config import BINARY_FIELDS
#         assert BINARY_FIELDS, "BINARY_FIELDS is empty!"
    
#     fmt = {
#         "columns": ["A", "B", "C", "D"],
#         "format_chars": ["c", "L", "f", "E"]
#     }
#     msg = {
#         "A": 100,
#         "B": 100000000,
#         "C": b"TEST",
#         "D": 5000
#     }
#     result = _apply_scaling_and_decode(msg, fmt)
#     with subtests.test("Scaling and decoding applied correctly"):
#         assert result["A"] == 1.0 
    
#     with subtests.test("Scaling for 'L' applied correctly"):    
#         assert result["B"] == 10.0 
    
#     with subtests.test("Decoding for bytes applied correctly"): 
#         assert result["C"] == "TEST"
    
#     with subtests.test("Scaling and decoding applied correctly"):
#         assert result["D"] == 50.0 

def test_make_blocks(subtests):
    parser = ParserMultiprocessing("fake_path.tlog")
    
    with subtests.test("File smaller than BLOCK_SIZE"):
        blocks = list(parser._make_blocks(0, 5 * 1024 * 1024))  # 5 MiB
        assert blocks == [(0, 5 * 1024 * 1024)], f"Got: {blocks!r}"

    # with subtests.test("File exactly BLOCK_SIZE"):
    #     blocks = list(parser._make_blocks(0, parser.BLOCK_SIZE))
    #     assert blocks == [(0, parser.BLOCK_SIZE)], f"Got: {blocks!r}"

    # with subtests.test("File slightly larger than BLOCK_SIZE"):
    #     blocks = list(parser._make_blocks(0, parser.BLOCK_SIZE + 1))
    #     expected = [(0, parser.BLOCK_SIZE), (parser.BLOCK_SIZE, parser.BLOCK_SIZE + 1)]
    #     assert blocks == expected, f"Got: {blocks!r}"

    # with subtests.test("File much larger than BLOCK_SIZE"):
    #     total_size = parser.BLOCK_SIZE * 3 + 5000
    #     blocks = list(parser._make_blocks(0, total_size))
    #     expected = [
    #         (0, parser.BLOCK_SIZE),
    #         (parser.BLOCK_SIZE, parser.BLOCK_SIZE * 2),
    #         (parser.BLOCK_SIZE * 2, parser.BLOCK_SIZE * 3),
    #         (parser.BLOCK_SIZE * 3, total_size)
    #     ]
    #     assert blocks == expected, f"Got: {blocks!r}"
# def test_recv_match_runs(tmp_path):
#     # נניח שיש לך קובץ בינארי קטן לדוגמה
#     fake_file = tmp_path / "test.tlog"
#     fake_file.write_bytes(b'\xA3\x95\x80' + b'\x00'*100)  # תוכן מינימלי

#     parser = ParserMultiprocessing(str(fake_file))
#     list(parser.recv_match())