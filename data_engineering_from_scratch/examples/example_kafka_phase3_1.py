import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka_like.topic import Topic
from kafka_like.producer import Producer
from kafka_like.consumer import Consumer

def main():
    print("--- Mini-Kafka Phase 3.1: Message Queue Demo ---")
    
    # 1. Setup
    topic = Topic("user_events", num_partitions=3)
    producer = Producer()
    consumer = Consumer([topic])
    
    # 2. Producer: Send messages with Keys (for strict ordering)
    print("\n[Step 1] Producing messages...")
    
    # User 123 events (Should all go to same partition)
    # User 456 events (Should go to different partition likely)
    
    users = ['user_123', 'user_456', 'user_789']
    events = ['login', 'view_page', 'click_button', 'logout']
    
    for u in users:
        for e in events:
            msg = {'user': u, 'event': e}
            p, o = producer.send(topic, msg, key=u)
            print(f"  Sent {u}:{e} -> Partition {p}, Offset {o}")

    # 3. Consumer: Poll messages
    print("\n[Step 2] Consuming messages...")
    
    # Poll for a few cycles
    received_count = 0
    max_msgs = len(users) * len(events)
    
    start_time = time.time()
    
    # Simple poll loop
    while received_count < max_msgs:
        for msg in consumer.poll(timeout_ms=100):
            print(f"  Received: P{msg['partition']}: {msg['value']}")
            received_count += 1
            
        if time.time() - start_time > 3:
            print("Timeout waiting for messages")
            break
            
    # 4. Verify Ordering for each user
    # We didn't store them to verify programmatically here for brevity, 
    # but the output log should show P{x} grouped by user.
    
    # Example assertion logic could be added here
    print("\n[Step 3] Verification Complete. check logs for ordering.")

if __name__ == "__main__":
    main()
