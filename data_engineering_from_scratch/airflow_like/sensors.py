import time
import os
from .task import Task

class Sensor(Task):
    """
    A Task that waits for a condition to be true.
    """
    def __init__(self, task_id, poke_interval=1, timeout=60, **kwargs):
        super().__init__(task_id, python_callable=self._execute_sensor, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout
        
    def poke(self, context):
        """
        Override this method. Return True if condition is met.
        """
        raise NotImplementedError
        
    def _execute_sensor(self, context):
        start_time = time.time()
        while True:
            if self.poke(context):
                print(f"[{self.task_id}] Criteria met.")
                return True
            
            if time.time() - start_time > self.timeout:
                raise TimeoutError(f"Sensor {self.task_id} timed out.")
                
            print(f"[{self.task_id}] Criteria not met. Sleeping {self.poke_interval}s...")
            time.sleep(self.poke_interval)

class FileSensor(Sensor):
    """
    Waits for a file to exist.
    """
    def __init__(self, task_id, filepath, **kwargs):
        super().__init__(task_id, **kwargs)
        self.filepath = filepath
        
    def poke(self, context):
        return os.path.exists(self.filepath)

class TimeSensor(Sensor):
    """
    Waits until a specific epoch time (simulated).
    """
    def __init__(self, task_id, target_time, **kwargs):
        super().__init__(task_id, **kwargs)
        self.target_time = target_time
        
    def poke(self, context):
        return time.time() >= self.target_time
