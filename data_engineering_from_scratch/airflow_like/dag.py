from typing import Dict, List, Set
from collections import deque
import heapq
from .task import Task

class DAG:
    """
    Directed Acyclic Graph of tasks.
    """
    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.tasks: Dict[str, Task] = {}

    def add_task(self, task: Task):
        self.tasks[task.task_id] = task

    def get_task(self, task_id: str) -> Task:
        return self.tasks[task_id]

    def topological_sort(self) -> List[Task]:
        """
        Returns a list of tasks in execution order.
        Uses Kahn's algorithm adapted for Priority Queue.
        """
        in_degree = {t_id: 0 for t_id in self.tasks}
        graph = {t_id: [] for t_id in self.tasks}
        
        # Build graph and valid checks
        for task in self.tasks.values():
            for downstream_id in task.downstream_task_ids:
                if downstream_id not in self.tasks:
                    raise ValueError(f"Task {downstream_id} not in DAG")
                
                graph[task.task_id].append(downstream_id)
                in_degree[downstream_id] += 1
        
        # Priority Queue for ready tasks
        # We want higher priority first. heapq is min-heap.
        # So we store (-priority, task_id).
        # Include task_id for stable tie-breaking if priorities are equal.
        queue = []
        
        for t_id, degree in in_degree.items():
            if degree == 0:
                task = self.tasks[t_id]
                heapq.heappush(queue, (-task.priority_weight, t_id))
                
        sorted_tasks = []
        
        while queue:
            _, u_id = heapq.heappop(queue)
            sorted_tasks.append(self.tasks[u_id])
            
            for v_id in graph[u_id]:
                in_degree[v_id] -= 1
                if in_degree[v_id] == 0:
                    task = self.tasks[v_id]
                    heapq.heappush(queue, (-task.priority_weight, v_id))
                    
        if len(sorted_tasks) != len(self.tasks):
            raise ValueError("DAG has a cycle")
            
        return sorted_tasks
