from pymavlink import mavutil
import math
import time
from parser import Parser
from multy_proccessing import ParserMultiprocessing
from thread_parser import ParserThreadPool

class Test:

    def __init__(self, path=r"C:\Users\yos77\Documents\ness\overlap\log_file_test_01.bin"):
        self.path = path
        self.mavlink_conn = mavutil.mavlink_connection(self.path)

    def _compare_data(self, mav_msg, par_msg):
        all_match = True
        for key, value in mav_msg.items():
            if key not in par_msg:
                print(f"Missing key in par_msg: {key}")
                all_match = False
                break
            if par_msg[key] != value:
                if (isinstance(value, float) and isinstance(par_msg[key], float)
                    and math.isnan(value) and math.isnan(par_msg[key])):
                    continue
                print("key:", key, "\nmav value:", value, "\npar value:", par_msg[key], "\ntype value:", type(value), type(par_msg[key]))
                all_match = False          
        return all_match

    def verify_count_messages(self, msg_type=None):
        start_time = time.time()
        parsor = Parser(self.path)
        parsor_gen = parsor.recv_match(msg_type)
        count_parsor = 0
        count_mav = 0
        for msg in parsor_gen:
            count_parsor += 1   
        while True:
            mav_msg = self.mavlink_conn.recv_match(type=msg_type, blocking=False) 
            if not mav_msg:
                break 
            count_mav += 1
        if count_mav != count_parsor:
            print("The count is not currect, \ndifferent count between mavlink and parsor.")
            print("count mavlink:", count_mav, "\ncount parsor:", count_parsor)
        else:
            print("The count message is match.\ncount:", count_parsor)
        end_time = time.time()
        print(f"Runtime:{end_time - start_time:.3f}")

    def verify_message_consistency(self, msg_type=None):
        start_time = time.time()
        all_match = True
        parsor = Parser(self.path)
        parsor_gen = parsor.recv_match(msg_type)
        counter = 0
        while True:
            mav_msg = self.mavlink_conn.recv_match(type=msg_type, blocking=False)
            if not mav_msg:
                break 
            counter += 1
            par_msg = next(parsor_gen)
            match = self._compare_data(mav_msg.to_dict(), par_msg)
            if not match:
                all_match = False
                break
                
        end_time = time.time()
        print(f"Runtime:{end_time - start_time:.3f}")
        print("Count messages:",counter)
        if all_match:
            print("All messages are match.")

    def parsor_runtime(self, msg_type=None):
        start_time = time.time()
        parsor = Parser(self.path)
        parsor_gen = parsor.recv_match(msg_type)
        for msg in parsor_gen:
            pass
        end_time = time.time()
        print(f"Parsor runtime:{end_time - start_time:.3f}")

    def mavlink_runtime(self, msg_type=None):
        start_time = time.time()
        while True:
            mav_msg = self.mavlink_conn.recv_match(type=msg_type, blocking=False) 
            if not mav_msg:
                break 
        end_time = time.time()
        print(f"Mavlink runtime:{end_time - start_time:.3f}")
    
    def multiprocessing_runtime(self, msg_type=None):
        start_time = time.time()
        parsor = ParserMultiprocessing(self.path)
        for msg in parsor.recv_match(msg_type):
            pass
        end_time = time.time()
        print(f"Multiprocessing runtime:{end_time - start_time:.3f}")

    def threads_runtime(self, msg_name=None):
        start_time = time.time()
        parsor = ParserThreadPool(self.path)
        for msg in parsor.recv_match(msg_name):
            pass
        end_time = time.time()
        print(f"Threading runtime:{end_time - start_time:.3f}")
        

if __name__ == "__main__":
    test_class = Test()
    # test_class.verify_count_messages()
    test_class.verify_message_consistency('GPS')
    # test_class.mavlink_runtime()
    # test_class.parsor_runtime()
    # test_class.multiprocessing_runtime()
    # test_class.threads_runtime()







