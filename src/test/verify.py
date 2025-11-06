import math
import time

from typing import Any, Dict, Generator, Optional

from pymavlink import mavutil

from business_logic.multi_processing import ParserMultiprocessing


class VerifyTests:

    def __init__(self, path : str = r"C:\Users\yos77\Documents\ness\overlap\log_file_test_01.bin") -> None:
        self.path = path

    def _compare_data(self, mav_msg: Dict[str, Any], par_msg : Dict[str, Any]) -> bool:
        all_match = True
        for key, value in mav_msg.items():
            if key not in par_msg:
                print(f"Missing key in par_msg: {key}")
                all_match = False
                break
            if par_msg[key] != value:
                if (
                    isinstance(value, float)
                    and isinstance(par_msg[key], float)
                    and math.isnan(value)
                    and math.isnan(par_msg[key])
                ):
                    continue
                print(
                    "key:",
                    key,
                    "\nmav value:",
                    value,
                    "\npar value:",
                    par_msg[key],
                    "\ntype value:",
                    type(value),
                    type(par_msg[key]),
                )
                all_match = False
        return all_match

    def verify_count_messages(self, msg_type : Optional[str] = None) -> None:
        start_time = time.time()
        mavlink_conn = mavutil.mavlink_connection(self.path)
        parser = ParserMultiprocessing(self.path)
        parser_gen = parser.recv_match(msg_type)
        count_parser = 0
        count_mav = 0
        while True:
            mav_msg = mavlink_conn.recv_match(type=msg_type, blocking=False)
            if not mav_msg:
                break
            count_mav += 1
            par_msg = next(parser_gen)
            count_parser += 1
        if count_mav != count_parser:
            print("The count is not currect, \ndifferent count between mavlink and parser.")
            print("Count mavlink:", count_mav, "\nCount parser:", count_parser)
        else:
            print("The count message is match.\nCount:", count_parser)
        end_time = time.time()
        print(f"Runtime: {end_time - start_time:.3f} seconds.")

    def verify_message_consistency(self, msg_type : Optional[str] = None) -> None:
        start_time = time.time()
        mavlink_conn = mavutil.mavlink_connection(self.path)
        all_match = True
        parser = ParserMultiprocessing(self.path)
        parser_gen = parser.recv_match(msg_type)
        counter = 0
        while True:
            mav_msg = mavlink_conn.recv_match(type=msg_type, blocking=False)
            if not mav_msg:
                break
            counter += 1
            par_msg = next(parser_gen)
            match = self._compare_data(mav_msg.to_dict(), par_msg)
            if not match:
                all_match = False
                break

        end_time = time.time()
        if all_match:
            print("All messages are match.")
        print("Count messages:", counter)
        print(f"Runtime: {end_time - start_time:.3f}")
