import sys
import os
import threading
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow_like.task import Task
from airflow_like.dag import DAG
from airflow_like.scheduler import Scheduler
from airflow_like.sensors import FileSensor

def create_trigger_file():
    time.sleep(3)
    print("\n[External Event] Creating trigger file...")
    with open("trigger.txt", "w") as f:
        f.write("go")

def process_file(context):
    print("Processing file data...")
    return "done"

def main():
    print("--- Mini-Airflow Phase 2.2/2.3: Scheduler & Sensors Demo ---")
    
    # Cleanup
    if os.path.exists("trigger.txt"):
        os.remove("trigger.txt")
        
    dag = DAG("sensor_dag")
    
    sensor = FileSensor("wait_for_file", "trigger.txt", poke_interval=1)
    processor = Task("process_data", process_file)
    
    dag.add_task(sensor)
    dag.add_task(processor)
    processor.set_upstream(sensor)
    
    # Start thread to create file after 3 seconds
    t = threading.Thread(target=create_trigger_file)
    t.start()
    
    # Run Scheduler
    scheduler = Scheduler([dag])
    scheduler.run(ticks=2, interval_seconds=1) 
    
    # Verify file created
    t.join()
    
    # Clean up
    if os.path.exists("trigger.txt"):
        os.remove("trigger.txt")
    if os.path.exists("scheduler_state.json"):
        os.remove("scheduler_state.json")

if __name__ == "__main__":
    main()
