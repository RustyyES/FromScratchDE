import sys
import os
import urllib.request
import csv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow_like.task import Task
from airflow_like.dag import DAG
from airflow_like.executor import SequentialExecutor
from airflow_like.sensors import FileSensor
from spark_like.partition import PartitionedDataFrame
from spark_like.io import read_csv

DATA_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
LOCAL_FILE = "iris.csv"

def download_data(context):
    print(f"Downloading data from {DATA_URL}...")
    # The iris dataset has no header, so we'll add one
    columns = ["sepal_length", "sepal_width", "petal_length", "petal_width", "class"]
    
    response = urllib.request.urlretrieve(DATA_URL, "iris_raw.csv")
    
    with open("iris_raw.csv", "r") as f_in, open(LOCAL_FILE, "w", newline="") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(columns)
        for line in f_in:
            if line.strip():
                writer.writerow(line.strip().split(','))
                
    os.remove("iris_raw.csv")
    print(f"Data saved to {LOCAL_FILE}")
    return LOCAL_FILE

def process_data(context):
    print("Starting Mini-Spark Job...")
    
    # 1. Read Data (Lazy)
    source = read_csv(LOCAL_FILE)
    
    # 2. Partition
    df = PartitionedDataFrame(source, num_partitions=2)
    
    # 3. Transform: Filter 'Iris-setosa'
    setosa_df = df.filter(lambda row: row['class'] == 'Iris-setosa')
    
    # 4. Aggregation: Average Sepal Width for Setosa
    # Map to float first
    mapped_df = setosa_df.map(lambda row: {'width': float(row['sepal_width']), 'id': 1})
    
    result = mapped_df.groupBy(lambda x: 'all_setosa').avg('width')
    
    print("Mini-Spark Job Finished.")
    return result

def report_results(context):
    # Retrieve result from upstream task (via Task Instance XCom simulation)
    # Our SequentialExecutor stores results in context['ti']
    spark_result = context['ti']['process_spark']
    
    print("\n" + "="*40)
    print("FINAL REPORT")
    print("="*40)
    print(f"Average Sepal Width for Iris-setosa: {spark_result['all_setosa']:.4f}")
    print("="*40 + "\n")

def main():
    print("--- Integration Demo: Airflow + Spark + Real Data ---")
    
    # Clean prev run
    if os.path.exists(LOCAL_FILE):
        os.remove(LOCAL_FILE)
        
    dag = DAG("integration_pipeline")
    
    # Task 1: Download Real Data
    t_download = Task("download_data", download_data)
    
    # Task 2: Sensor (Wait for file to exist - sanity check integration)
    t_sensor = FileSensor("wait_for_data", LOCAL_FILE, poke_interval=1)
    
    # Task 3: Mini-Spark Processing
    t_process = Task("process_spark", process_data)
    
    # Task 4: Report
    t_report = Task("report", report_results)
    
    # Dependencies
    dag.add_task(t_download)
    dag.add_task(t_sensor)
    dag.add_task(t_process)
    dag.add_task(t_report)
    
    # download -> wait -> process -> report
    t_sensor.set_upstream(t_download)
    t_process.set_upstream(t_sensor)
    t_report.set_upstream(t_process)
    
    # Execute
    executor = SequentialExecutor()
    executor.execute(dag)
    
    # Cleanup
    if os.path.exists(LOCAL_FILE):
        os.remove(LOCAL_FILE)

if __name__ == "__main__":
    main()
