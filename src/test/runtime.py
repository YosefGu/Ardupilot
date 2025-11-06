from pymavlink import mavutil
import time
from business_logic.parser_sync import ParserSync
from business_logic.multy_processing import ParserMultiprocessing
from business_logic.thread_parser import ParserThreadPool

class RuntimeTests:

    def __init__(self, path=r"C:\Users\yos77\Documents\ness\overlap\log_file_test_01.bin"):
        self.path = path

    def mavlink_runtime(self, msg_type=None):
        start_time = time.time()
        mavlink_conn = mavutil.mavlink_connection(self.path)
        while True:
            mav_msg = mavlink_conn.recv_match(type=msg_type, blocking=False) 
            if not mav_msg:
                break 
        end_time = time.time()
        print(f"Mavlink runtime:{end_time - start_time:.3f}")

    def parsor_runtime(self, msg_type=None):
        start_time = time.time()
        parsor = ParserSync(self.path)
        parsor_gen = parsor.recv_match(msg_type)
        for msg in parsor_gen:
            pass
        end_time = time.time()
        print(f"Parsor runtime:{end_time - start_time:.3f}")

    def threads_runtime(self, msg_name=None):
        start_time = time.time()
        parsor = ParserThreadPool(self.path)
        for msg in parsor.recv_match(msg_name):
            pass
        end_time = time.time()
        print(f"Threading runtime:{end_time - start_time:.3f}")
    
    def multiprocessing_runtime(self, msg_type=None):
        start_time = time.time()
        parsor = ParserMultiprocessing(self.path)
        for msg in parsor.recv_match(msg_type):
            pass
        end_time = time.time()
        print(f"Multiprocessing runtime:{end_time - start_time:.3f}")