from typing import Any
from .topic import Topic

class Producer:
    """
    Produces messages to topics.
    """
    def send(self, topic: Topic, message: Any, key: Any = None):
        """
        Send a message to the topic.
        Returns metadata (partition, offset).
        """
        partition, offset = topic.append(message, key)
        # print(f"[Producer] Sent to {topic.name}[{partition}] @ {offset}")
        return partition, offset
