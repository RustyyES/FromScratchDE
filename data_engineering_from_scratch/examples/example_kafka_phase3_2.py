import sys
import os
import time
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka_like.topic import Topic
from kafka_like.producer import Producer
from kafka_like.consumer import Consumer
from kafka_like.stream_processor import StreamProcessor

def main():
    print("--- Mini-Kafka Phase 3.2: Stream Processing Demo ---")
    
    topic = Topic("clicks", num_partitions=1)
    producer = Producer()
    consumer = Consumer([topic])
    
    # 1. Start Producer Thread (Simulate continuous stream)
    def produce_clicks():
        for i in range(6):
            producer.send(topic, {'click_id': i}, key='u1')
            time.sleep(0.5) 
            # Total 3 seconds of data.
            # 0.0s: msg 0
            # 0.5s: msg 1
            # 1.0s: msg 2
            # 1.5s: msg 3
            # 2.0s: msg 4
            # 2.5s: msg 5
            
    p_thread = threading.Thread(target=produce_clicks)
    p_thread.start()
    
    # 2. Start Stream Processor
    # Window 2 seconds.
    processor = StreamProcessor(consumer).window(seconds=2)
    
    def count_clicks(buffer):
        return len(buffer)
    
    # Needs to run long enough to catch windows
    # Window 1 (0-2s): Should catch msg 0, 1, 2, 3 -> (4 msgs)
    # Window 2 (2-4s): Should catch msg 4, 5 -> (2 msgs)
    
    processor.start(count_clicks, timeout_ms=500, max_empty_polls=8)
    
    p_thread.join()

if __name__ == "__main__":
    main()
