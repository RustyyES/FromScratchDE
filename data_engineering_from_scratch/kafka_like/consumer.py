from typing import List, Dict, Iterator, Any
import time
from .topic import Topic

class Consumer:
    """
    Consumes messages from topics.
    Manages offsets manually (simple version of Consumer Group).
    """
    def __init__(self, topics: List[Topic]):
        self.topics = {t.name: t for t in topics}
        # Current internal state: {topic_name: {partition_id: next_offset}}
        self.offsets = {t.name: {p: 0 for p in range(t.num_partitions)} for t in topics}

    def subscribe(self, topics: List[Topic]):
        for t in topics:
            if t.name not in self.topics:
                self.topics[t.name] = t
                self.offsets[t.name] = {p: 0 for p in range(t.num_partitions)}

    def poll(self, timeout_ms: int = 100) -> Iterator[Dict]:
        """
        Polls for new messages across all assigned topics/partitions.
        Yields messages one by one.
        """
        start = time.time()
        
        # Round robin polling or just iterate all
        found_any = False
        
        for topic_name, topic in self.topics.items():
            for p_id in range(topic.num_partitions):
                current_offset = self.offsets[topic_name][p_id]
                messages = topic.read(p_id, current_offset, limit=10)
                
                for msg in messages:
                    found_any = True
                    # Update offset
                    self.offsets[topic_name][p_id] = msg['offset'] + 1
                    
                    # Yield wrapper
                    yield {
                        'topic': topic_name,
                        'partition': p_id,
                        'key': msg['key'],
                        'value': msg['value'],
                        'offset': msg['offset']
                    }
        
        if not found_any:
            # Simple simulation of waiting
            time.sleep(timeout_ms / 1000.0)

    def commit(self):
        """
        Persist offsets. 
        In this mini-version, offsets are just in-memory in self.offsets.
        Real Kafka would send OffsetCommitRequest to broker.
        """
        pass
        # print("[Consumer] Offsets committed.")
