import sys
import os
import time

# Add the project root to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from spark_like.lazy_df import LazyDataFrame

def main():
    print("--- Mini-Spark Phase 1.1: Lazy Evaluation Demo ---")
    
    # 1. Create a data source
    data = [
        {'id': 1, 'name': 'Alice', 'age': 28, 'score': 85},
        {'id': 2, 'name': 'Bob', 'age': 22, 'score': 90},
        {'id': 3, 'name': 'Charlie', 'age': 35, 'score': 75},
        {'id': 4, 'name': 'David', 'age': 40, 'score': 88},
        {'id': 5, 'name': 'Eve', 'age': 29, 'score': 92},
    ]
    
    print("\n[Step 1] Initalizing LazyDataFrame...")
    df = LazyDataFrame(data)
    
    # Define functions with side effects (print) to prove laziness
    def filter_senior(x):
        print(f"  [Filter] Checking {x['name']}")
        return x['age'] > 25

    def map_status(x):
        print(f"  [Map] Processing {x['name']}")
        res = x.copy()
        res['status'] = 'Senior'
        return res

    # 2. Apply transformations
    print("\n[Step 2] Applying .filter() and .map() (Intent only)...")
    transformed_df = df.filter(filter_senior).map(map_status)
    
    print(">>> Notice NO processing prints appeared above! This proves laziness.")
    
    # 3. Action 1: Show
    print("\n[Step 3] Calling .show(2) (Action - Triggers execution)...")
    transformed_df.show(2)
    
    # 4. Action 2: Collect
    print("\n[Step 4] Calling .collect() (Action - Triggers re-execution)...")
    results = transformed_df.collect()
    print(f"Results collected: {results}")

if __name__ == "__main__":
    main()
