import sys
import os
import urllib.request
import json
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from airflow_like.task import Task
from airflow_like.dag import DAG
from airflow_like.executor import SequentialExecutor
from kafka_like.topic import Topic
from kafka_like.producer import Producer
from kafka_like.consumer import Consumer
from spark_like.partition import PartitionedDataFrame

# Source: USGS All Earthquakes from the Past 7 Days (Large enough for demo, small enough for quick download)
DATA_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson"
EARTHQUAKE_FILE = "earthquakes.json"
TOPIC_NAME = "earthquake_events"
NUM_PARTITIONS = 4

def fetch_data(context):
    print(f"Fetching data from {DATA_URL}...")
    try:
        with urllib.request.urlopen(DATA_URL) as url:
            data = json.loads(url.read().decode())
            features = data.get('features', [])
            print(f"Downloaded {len(features)} earthquake events.")
            
            with open(EARTHQUAKE_FILE, 'w') as f:
                json.dump(features, f)
    except Exception as e:
        print(f"Download failed: {e}")
        raise e
    return EARTHQUAKE_FILE

def ingest_to_kafka(context):
    print("Ingesting data into Kafka...")
    if not os.path.exists(EARTHQUAKE_FILE):
        raise FileNotFoundError("Data file not found")
        
    with open(EARTHQUAKE_FILE, 'r') as f:
        events = json.load(f)
        
    topic = Topic(TOPIC_NAME, num_partitions=NUM_PARTITIONS)
    producer = Producer()
    
    # Store topic in context for downstream tasks (Simulation hack)
    # In real world, downstream connects to broker
    context['kafka_topic'] = topic
    
    for event in events:
        props = event['properties']
        place = props.get('place', 'Unknown')
        # Use 'place' as key to partition by region
        producer.send(topic, props, key=place)
        
    print(f"Ingested {len(events)} events into {TOPIC_NAME}.")
    return TOPIC_NAME

# Complex Calculation Simulation
def expensive_calculation(row):
    # Simulate meaningful CPU work (e.g., geospatial processing)
    # Just a small sleep to represent complexity that benefits from parallelism
    time.sleep(0.005) 
    return row

def process_spark(context):
    print("\n--- Mini-Spark Distributed Processing ---")
    topic = context['kafka_topic'] # Get topic ref
    
    # 1. Consumer (Read all)
    consumer = Consumer([topic])
    all_msgs = []
    start_poll = time.time()
    while True:
        batch = list(consumer.poll(timeout_ms=100))
        if not batch:
            if time.time() - start_poll > 2.0: # Stop if no data for 2s
                break
        else:
            all_msgs.extend(batch)
            
    print(f"Spark Config: {NUM_PARTITIONS} Partitions. Processing {len(all_msgs)} events.")
    raw_data = [m['value'] for m in all_msgs]
    
    start_time = time.time()
    
    # 2. Parallel Processing
    df = PartitionedDataFrame(raw_data, num_partitions=NUM_PARTITIONS)
    
    # Filter: Magnitude > 4.0
    # Add debug print inside filter (not ideal for lazy but helpful for debug)
    def filter_mag(x):
        try:
             val = float(x.get('mag', 0) or 0)
             return val > 4.0
        except:
             return False

    significant = df.filter(filter_mag)
    
    # Map: Expensive transform
    processed = significant.map(expensive_calculation)
    
    # Aggregation: Count by Place (simplified region)
    # To aggregate, we group by a simplified region string (e.g. after comma)
    def region_extractor(row):
        place = row['place']
        if ',' in place:
            return place.split(',')[-1].strip()
        return "Other"
        
    grouped = processed.groupBy(region_extractor)
    counts = grouped.count()
    
    duration = time.time() - start_time
    print(f"Spark Processing Time: {duration:.4f}s")
    
    # Report Top 5 Regions
    sorted_regions = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:5]
    print("Top 5 Active Earthquake Regions (> 4.0 mag):")
    for region, count in sorted_regions:
        print(f"  {region}: {count}")
        
    return duration

def benchmark_baseline(context):
    print("\n--- Baseline Single-Threaded Processing ---")
    if not os.path.exists(EARTHQUAKE_FILE):
        return
        
    with open(EARTHQUAKE_FILE, 'r') as f:
        events = json.load(f)
    
    start_time = time.time()
    
    data = [e['properties'] for e in events]
    
    # Filter
    significant = [x for x in data if x['mag'] is not None and x['mag'] > 4.0]
    
    # Map (Expensive)
    processed = [expensive_calculation(x) for x in significant]
    
    # Aggregate
    counts = {}
    for row in processed:
        place = row['place']
        if ',' in place:
            region = place.split(',')[-1].strip()
        else:
            region = "Other"
            
        counts[region] = counts.get(region, 0) + 1
        
    duration = time.time() - start_time
    print(f"Baseline Processing Time: {duration:.4f}s")
    return duration

def compare_results(context):
    spark_time = context['ti']['process_spark']
    base_time = context['ti']['benchmark_baseline']
    
    if base_time > 0:
        speedup = base_time / spark_time
        print("\n" + "="*40)
        print("BENCHMARK RESULTS")
        print("="*40)
        print(f"Spark Time   : {spark_time:.4f}s")
        print(f"Baseline Time: {base_time:.4f}s")
        print(f"Speedup      : {speedup:.2f}x")
        print("="*40)
        
        if speedup > 1.0:
            print("CONCLUSION: Mini-Spark Parallelism WINS! 🚀")
        else:
            print("CONCLUSION: Overhead dominated (Dataset too small or task too simple).")

def main():
    dag = DAG("usgs_benchmark")
    
    t_fetch = Task("fetch_data", fetch_data)
    t_ingest = Task("ingest_kafka", ingest_to_kafka)
    t_spark = Task("process_spark", process_spark)
    t_base = Task("benchmark_baseline", benchmark_baseline)
    t_compare = Task("compare_results", compare_results)
    
    dag.add_task(t_fetch)
    dag.add_task(t_ingest)
    dag.add_task(t_spark)
    dag.add_task(t_base)
    dag.add_task(t_compare)
    
    t_ingest.set_upstream(t_fetch)
    # Spark and Baseline depend on data availability (Ingest puts it in Topic, File exists)
    t_spark.set_upstream(t_ingest)
    t_base.set_upstream(t_ingest)
    t_compare.set_upstream(t_spark)
    t_compare.set_upstream(t_base)
    
    executor = SequentialExecutor()
    executor.execute(dag)
    
    # Cleanup
    if os.path.exists(EARTHQUAKE_FILE):
        os.remove(EARTHQUAKE_FILE)

if __name__ == "__main__":
    main()
