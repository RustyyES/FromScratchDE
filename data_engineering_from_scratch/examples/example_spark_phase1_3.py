import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_like.partition import PartitionedDataFrame

def main():
    print("--- Mini-Spark Phase 1.3: Aggregation Demo ---")
    
    # Dataset: Sales data
    data = [
        {'id': 1, 'category': 'Electronics', 'revenue': 100},
        {'id': 2, 'category': 'Clothing', 'revenue': 50},
        {'id': 3, 'category': 'Electronics', 'revenue': 200},
        {'id': 4, 'category': 'Home', 'revenue': 150},
        {'id': 5, 'category': 'Clothing', 'revenue': 80},
        {'id': 6, 'category': 'Electronics', 'revenue': 120},
    ]
    
    df = PartitionedDataFrame(data, num_partitions=2)
    
    # 1. GroupBy Category
    print("\n[Step 1] GroupBy 'category'...")
    grouped = df.groupBy(lambda x: x['category'])
    
    # 2. Count
    print("\n[Step 2] Count per group:")
    counts = grouped.count()
    print(f"  Counts: {counts}") 
    # Exp: Electronics: 3, Clothing: 2, Home: 1
    
    # 3. Sum Revenue
    print("\n[Step 3] Sum 'revenue' per group:")
    sums = grouped.sum('revenue')
    print(f"  Revenue Sums: {sums}")
    # Exp: Electronics: 420, Clothing: 130, Home: 150
    
    # 4. Avg Revenue
    print("\n[Step 4] Avg 'revenue' per group:")
    avgs = grouped.avg('revenue')
    print(f"  Revenue Avgs: {avgs}")
    # Exp: Electronics: 140, Clothing: 65, Home: 150
    
    # 5. Multi Agg
    print("\n[Step 5] .agg({'revenue': 'sum', 'id': 'count'})")
    multi = grouped.agg({'revenue': 'sum', 'id': 'count'}) # id count is effectively generic count
    print(f"  Multi Agg: {multi}")

if __name__ == "__main__":
    main()
