import time
from typing import Callable, Any, List

class StreamProcessor:
    """
    Processes a stream of data from a Consumer with windowing support.
    """
    def __init__(self, consumer, producer=None, output_topic=None):
        self.consumer = consumer
        self.producer = producer
        self.output_topic = output_topic
        self.buffer = []
        self.window_size = 0
        self.last_window_close = time.time()

    def window(self, seconds: int):
        """
        Set tumbling window size in seconds.
        """
        self.window_size = seconds
        return self

    def start(self, process_func: Callable[[List[Any]], Any], timeout_ms=500, max_empty_polls=5):
        """
        Starts processing loop.
        - Accumulates messages.
        - When window closes, calls process_func(buffer).
        - If producer is set, sends result to output_topic.
        """
        print(f"Starting Stream Processor (Window: {self.window_size}s)...")
        empty_polls = 0
        
        # Simple loop for demo purposes
        while empty_polls < max_empty_polls:
            msgs = list(self.consumer.poll(timeout_ms=timeout_ms))
            
            if msgs:
                self.buffer.extend(msgs)
                empty_polls = 0
            else:
                empty_polls += 1
            
            current_time = time.time()
            if self.window_size > 0 and (current_time - self.last_window_close >= self.window_size):
                # Window closed
                if self.buffer:
                    print(f"[Window] Closing window with {len(self.buffer)} events.")
                    result = process_func(self.buffer)
                    
                    if self.producer and self.output_topic:
                         self.producer.send(self.output_topic, result)
                         print(f"[Window] Result produced: {result}")
                    else:
                        print(f"[Window] Result: {result}")
                        
                    self.buffer = [] # Clear buffer
                
                self.last_window_close = current_time
                
        print("Stream Processor stopped.")
