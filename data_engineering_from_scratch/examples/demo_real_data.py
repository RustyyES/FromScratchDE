import sys
import os
import random
import csv
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_like.partition import PartitionedDataFrame
from spark_like.io import read_csv

def generate_sample_csv(filename, rows=10000):
    print(f"Generating {rows} rows of sample data into {filename}...")
    cities = ['New York', 'London', 'Paris', 'Tokyo', 'Berlin', 'Cairo']
    products = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor']
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'city', 'product', 'amount', 'age'])
        
        for i in range(rows):
            writer.writerow([
                i,
                random.choice(cities),
                random.choice(products),
                random.randint(100, 2000),
                random.randint(18, 80)
            ])
    print("Generation complete.")

def main():
    csv_file = 'sales_data.csv'
    
    # 1. Generate Data
    if not os.path.exists(csv_file):
        generate_sample_csv(csv_file, rows=50000)
    
    print("\n--- Real Dataset Demo with Mini-Spark ---")
    
    # 2. Load Data Lazily
    # read_csv returns a generator, so we are not loading 50k rows into memory yet!
    # PartitionedDataFrame will iterate and chunk it.
    print("[Step 1] Loading CSV into PartitionedDataFrame (4 partitions)...")
    source_data = read_csv(csv_file)
    
    # Note: PartitionedDataFrame DOES materialize the initial split into memory lists currently
    # to distribute them. In a real distributed system, we'd read file splits.
    # But this proves we can ingest external data.
    start_time = time.time()
    df = PartitionedDataFrame(source_data, num_partitions=4)
    print(f"Data loaded in {time.time() - start_time:.4f}s")
    
    # 3. Define Analysis
    # "Calculate total revenue per city for people over 30"
    
    print("\n[Step 2] Defining transformations (Lazy)...")
    
    # Filter: Age > 30
    # Note: CSV reads as strings, so we need to cast
    filtered = df.filter(lambda x: int(x['age']) > 30)
    
    # Map: Convert amount to int (part of processing)
    mapped = filtered.map(lambda x: {
        'city': x['city'], 
        'amount': int(x['amount'])
    })
    
    # GroupBy City
    grouped = mapped.groupBy(lambda x: x['city'])
    
    print("\n[Step 3] Executing Action: Sum Revenue per City...")
    start_time = time.time()
    result = grouped.sum('amount')
    duration = time.time() - start_time
    
    print(f"Analysis complete in {duration:.4f}s")
    print("-" * 30)
    for city, revenue in result.items():
        print(f"{city}: ${revenue:,}")
    print("-" * 30)

    # Cleanup
    # os.remove(csv_file) 

if __name__ == "__main__":
    main()
