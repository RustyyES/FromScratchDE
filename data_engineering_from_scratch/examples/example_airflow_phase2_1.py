import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow_like.task import Task
from airflow_like.dag import DAG
from airflow_like.executor import SequentialExecutor

def dummy_task(context):
    return "done"

def main():
    print("--- Mini-Airflow Phase 2.1: DAG & Prioritization Demo ---")
    
    dag = DAG("priority_test")
    
    # 1. Create Tasks
    # Structure:
    #      Start
    #     /     \
    #  Low(1)   High(10)
    #     \     /
    #      End
    
    task_start = Task("start", dummy_task)
    task_low = Task("low_priority", dummy_task, priority_weight=1)
    task_high = Task("high_priority", dummy_task, priority_weight=10)
    task_end = Task("end", dummy_task)
    
    dag.add_task(task_start)
    dag.add_task(task_low)
    dag.add_task(task_high)
    dag.add_task(task_end)
    
    # 2. Set Dependencies
    task_low.set_upstream(task_start)
    task_high.set_upstream(task_start)
    
    task_end.set_upstream(task_low)
    task_end.set_upstream(task_high)
    
    # 3. Verify Sort Order
    # Expected: start -> high_priority -> low_priority -> end
    # Note: 'high' must come before 'low' because both become ready after 'start', 
    # and 'high' has weight 10 vs 1.
    
    print("\n[Step 1] Verifying Topological Sort with Priorities...")
    sorted_tasks = dag.topological_sort()
    order = [t.task_id for t in sorted_tasks]
    print(f"Execution Order: {order}")
    
    expected = ['start', 'high_priority', 'low_priority', 'end']
    if order == expected:
        print("SUCCESS: Priorities respected!")
    else:
        print(f"FAILURE: Expected {expected}")
        
    # 4. Execute
    print("\n[Step 2] Running Executor...")
    executor = SequentialExecutor()
    executor.execute(dag)

if __name__ == "__main__":
    main()
