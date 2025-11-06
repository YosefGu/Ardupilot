from test.verify import VerifyTests
from test.runtime import RuntimeTests   


if __name__ == "__main__":
    verify_tests_class = VerifyTests()
    runtime_tests_class = RuntimeTests()

    # Example usage:    
    verify_tests_class.verify_count_messages('GPS')
    verify_tests_class.verify_message_consistency('GPS')
    # runtime_tests_class.mavlink_runtime('GPS')
    # runtime_tests_class.parsor_runtime('GPS')
    # runtime_tests_class.threads_runtime('GPS')
    # runtime_tests_class.multiprocessing_runtime('GPS')
    