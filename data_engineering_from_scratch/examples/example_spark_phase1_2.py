import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_like.partition import PartitionedDataFrame

def main():
    print("--- Mini-Spark Phase 1.2: Partitioning Demo ---")
    
    data = list(range(1, 11)) # 1 to 10
    print(f"Data: {data}")
    
    # 1. Create Partitioned DF
    print("\n[Step 1] Creating PartitionedDataFrame (2 partitions)...")
    df = PartitionedDataFrame(data, num_partitions=2)
    
    print("Partitions info:")
    for p in df.partitions:
        # Note: We can peek at them because they are lists initially, 
        # but if we iterate them we might exhaust them if they were generators?
        # In current init implementation, they are Partition objects wrapping lists.
        # But let's be careful. The __init__ makes lists.
        print(f"  Partition {p.index}: {list(p)}")

    # 2. Transformation
    print("\n[Step 2] Map: x * 2 (Lazy)...")
    df_mapped = df.map(lambda x: x * 2)
    
    # 3. Collect
    print("[Step 3] Collect results:")
    results = df_mapped.collect()
    print(f"  Result: {results}")
    
    # 4. Repartition
    print("\n[Step 4] Repartition to 3 partitions...")
    df_repart = df_mapped.repartition(3)
    for p in df_repart.partitions:
         print(f"  Partition {p.index}: {list(p)}") # Materialize to view

    # 5. ReduceByKey
    print("\n[Step 5] ReduceByKey...")
    # Create (key, value) pairs: (is_even, number)
    kv_data = [(x % 2 == 0, x) for x in range(1, 6)] 
    # [(False, 1), (True, 2), (False, 3), (True, 4), (False, 5)]
    # Keys: False (odds): 1, 3, 5 -> Sum = 9
    # Keys: True (evens): 2, 4 -> Sum = 6
    
    kv_df = PartitionedDataFrame(kv_data, num_partitions=2)
    print(f"  KV Data: {kv_df.collect()}")
    
    result_df = kv_df.reduce_by_key(lambda a, b: a + b)
    print(f"  Reduced: {result_df.collect()}")
    
    # 6. Map Partitions
    print("\n[Step 6] Map Partitions (Sum of each partition)...")
    # For this to work well, we need to know what's in each partition.
    # Let's create a new fresh DF where we know the split
    df_nums = PartitionedDataFrame([1, 1, 1, 2, 2, 2], num_partitions=2)
    # P0: [1, 1, 1], P1: [2, 2, 2]
    
    def sum_partition(iterator):
        yield sum(iterator)
        
    partition_sums = df_nums.map_partitions(sum_partition).collect()
    print(f"  Partition Sums: {partition_sums}")

if __name__ == "__main__":
    main()
