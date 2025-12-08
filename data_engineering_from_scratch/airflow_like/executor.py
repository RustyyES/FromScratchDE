from typing import Dict, Any
from .dag import DAG

class SequentialExecutor:
    """
    Executes tasks sequentially in the local process.
    """
    def __init__(self):
        pass

    def execute(self, dag: DAG):
        """
        Run all tasks in the DAG in topological order.
        """
        print(f"Starting execution of DAG: {dag.dag_id}")
        execution_order = dag.topological_sort()
        
        # Shared context (like XComs)
        context = {
            'dag_run_id': f"run_{dag.dag_id}_manual",
            'execution_date': 'now',
            'ti': {}, # Task Instances results
            'skipped_tasks': set() # Track skipped tasks
        }
        
        for task in execution_order:
            # Check if task is in skipped list or if any of its upstream were skipped (propagation)
            # Simple skip propagation: if ALL upstream were skipped, I am skipped?
            # Or explicit skip: BranchTask adds my ID to skipped_tasks.
            
            if task.task_id in context['skipped_tasks']:
                print(f"--- Task: {task.task_id} (SKIPPED) ---")
                continue
                
            print(f"--- Task: {task.task_id} ---")
            try:
                # Pass context to task
                result = task.execute(context)
                
                # Store result
                context['ti'][task.task_id] = result
                
                # Check for Branching Result (Convention: BranchPythonOperator returns task_id to follow)
                # If task returns a string that IS a downstream task_id, we assume it's a branch choice
                # and we SKIP the others.
                if isinstance(result, str) and result in task.downstream_task_ids:
                    chosen_branch = result
                    # Skip all other immediate downstream tasks
                    for downstream in task.downstream_task_ids:
                        if downstream != chosen_branch:
                            context['skipped_tasks'].add(downstream)
                            print(f"   [Branching] Skipping {downstream}")
                            
            except Exception as e:
                print(f"Task {task.task_id} failed. Stopping DAG execution.")
                raise e
                
        print(f"DAG {dag.dag_id} finished successfully.")
