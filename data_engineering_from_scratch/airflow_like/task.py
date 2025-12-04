from typing import Callable, Set, Any, Dict
import time

class Task:
    """
    Represents a unit of work in a DAG.
    """
    def __init__(
        self, 
        task_id: str, 
        python_callable: Callable, 
        priority_weight: int = 1, 
        retries: int = 0,
        retry_delay: int = 1
    ):
        self.task_id = task_id
        self.python_callable = python_callable
        self.priority_weight = priority_weight
        self.retries = retries
        self.retry_delay = retry_delay
        
        self.upstream_task_ids: Set[str] = set()
        self.downstream_task_ids: Set[str] = set()
        
    def set_upstream(self, task: 'Task'):
        """
        Sets this task to depend on 'task'. 
        task -> self
        """
        self.upstream_task_ids.add(task.task_id)
        task.downstream_task_ids.add(self.task_id)

    def set_downstream(self, task: 'Task'):
        """
        Sets 'task' to depend on this task.
        self -> task
        """
        self.downstream_task_ids.add(task.task_id)
        task.upstream_task_ids.add(self.task_id)
        
    def execute(self, context: Dict[str, Any]):
        """
        Executes the python_callable with retries.
        """
        attempt = 0
        while attempt <= self.retries:
            try:
                print(f"[{self.task_id}] Executing...")
                result = self.python_callable(context)
                print(f"[{self.task_id}] Success.")
                return result
            except Exception as e:
                attempt += 1
                print(f"[{self.task_id}] Failed (Attempt {attempt}/{self.retries + 1}): {e}")
                if attempt <= self.retries:
                    time.sleep(self.retry_delay)
                else:
                    raise e
