from typing import List, Dict, Any, Tuple
import threading

class Topic:
    """
    Represents a Kafka Topic with multiple partitions.
    Each partition is an ordered list of messages (Commit Log).
    """
    def __init__(self, name: str, num_partitions: int = 3):
        self.name = name
        self.num_partitions = num_partitions
        # List of lists. index is partition_id.
        # Each item is (offset, message_value, timestamp)
        self.partitions: List[List[Dict]] = [[] for _ in range(num_partitions)]
        self.locks = [threading.Lock() for _ in range(num_partitions)]

    def _get_partition(self, key: Any) -> int:
        if key is None:
            # Round-robin or random could be used, but for now default to 0
            return 0
        return hash(key) % self.num_partitions

    def append(self, message: Any, key: Any = None) -> Tuple[int, int]:
        """
        Appends a message to the appropriate partition.
        Returns (partition_id, offset).
        """
        partition_id = self._get_partition(key)
        
        with self.locks[partition_id]:
            offset = len(self.partitions[partition_id])
            record = {
                'offset': offset,
                'key': key,
                'value': message,
                # 'timestamp': time.time()
            }
            self.partitions[partition_id].append(record)
            
        return partition_id, offset

    def read(self, partition_id: int, offset: int, limit: int = 100) -> List[Dict]:
        """
        Reads messages from a partition starting at offset.
        """
        if partition_id < 0 or partition_id >= self.num_partitions:
            raise ValueError(f"Invalid partition {partition_id}")
            
        # No lock needed for reading (assuming distinct list append isn't reallocating messily in Python logic)
        # But to be safe in this simulator:
        log = self.partitions[partition_id]
        if offset >= len(log):
            return []
            
        end_offset = min(offset + limit, len(log))
        return log[offset:end_offset]
