import json
import time
import os
from typing import Dict, List
from .dag import DAG
from .executor import SequentialExecutor

class Scheduler:
    """
    Simulates a scheduler that triggers DAGs based on intervals.
    Persists state to a JSON file.
    """
    def __init__(self, dags: List[DAG], state_file: str = "scheduler_state.json"):
        self.dags = {dag.dag_id: dag for dag in dags}
        self.state_file = state_file
        self.executor = SequentialExecutor()
        self.state = self.load_state()

    def load_state(self) -> Dict:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save_state(self):
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)

    def run(self, ticks: int = 5, interval_seconds: int = 1):
        """
        Run the scheduler loop for a limited number of 'ticks' (for simulation).
        """
        print(f"Scheduler starting. State file: {self.state_file}")
        
        for i in range(ticks):
            print(f"\n[Tick {i+1}/{ticks}] Checking DAGs...")
            current_time = time.time()
            
            for dag_id, dag in self.dags.items():
                # In a real system, we'd check Cron expressions.
                # Here we just check if it has ever run or simple forced run for demo.
                
                last_run = self.state.get(dag_id, {}).get('last_run', 0)
                
                # Simple logic: Run every 5 seconds (simulation)
                if current_time - last_run > 5:
                    print(f"Triggering DAG: {dag_id}")
                    
                    try:
                        self.executor.execute(dag)
                        
                        # Update state
                        if dag_id not in self.state:
                            self.state[dag_id] = {}
                        self.state[dag_id]['last_run'] = current_time
                        self.state[dag_id]['total_runs'] = self.state[dag_id].get('total_runs', 0) + 1
                        self.save_state()
                        
                    except Exception as e:
                        print(f"DAG {dag_id} failed: {e}")
                else:
                    print(f"Skipping {dag_id} (Too soon)")
            
            time.sleep(interval_seconds)
