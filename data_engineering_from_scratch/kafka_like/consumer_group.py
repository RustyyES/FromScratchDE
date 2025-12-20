from typing import List, Dict
from .consumer import Consumer
from .topic import Topic

class ConsumerGroup:
    """
    Manages a group of consumers and assigns partitions to them.
    """
    def __init__(self, group_id: str, topic: Topic, members: List[Consumer]):
        self.group_id = group_id
        self.topic = topic
        self.members = members
        self.assignments: Dict[Consumer, List[int]] = {}

    def add_member(self, consumer: Consumer):
        self.members.append(consumer)
        self.rebalance()

    def rebalance(self):
        """
        Assigns partitions to members using Round Robin or Range.
        Here: Round Robin.
        """
        print(f"[ConsumerGroup: {self.group_id}] Rebalancing {len(self.members)} members for {self.topic.name} ({self.topic.num_partitions} partitions)...")
        
        # Clear current assignments
        for m in self.members:
            # We assume Consumer has a method to ASSIGN specific partitions
            # In our simple Consumer, subscribe() takes Topics, not specific partitions.
            # We'll just print the assignment for simulation or modify Consumer if needed.
            # Let's assume we just calculate it here.
            pass
            
        partitions = list(range(self.topic.num_partitions))
        # Round robin
        self.assignments = {m: [] for m in self.members}
        
        for i, p in enumerate(partitions):
            member = self.members[i % len(self.members)]
            self.assignments[member].append(p)
            
        for i, (m, parts) in enumerate(self.assignments.items()):
            print(f"  Consumer {i+1} assigned Partitions: {parts}")
            # Real implementation would call m.assign(parts)
