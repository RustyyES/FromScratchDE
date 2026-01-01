from typing import List, Dict, Any, Tuple

def expect_column_to_exist(dataset: List[Dict], column: str) -> Dict:
    """
    Checks if a column exists in the dataset schema (checking first row/all rows).
    """
    if not dataset:
        return {"success": False, "result": "Empty dataset"}
        
    failures = 0
    for row in dataset:
        if column not in row:
            failures += 1
            
    success = failures == 0
    return {
        "success": success,
        "unexpected_count": failures,
        "unexpected_percent": (failures / len(dataset)) * 100
    }

def expect_column_values_to_not_be_null(dataset: List[Dict], column: str) -> Dict:
    failures = 0
    unexpected_list = []
    
    for i, row in enumerate(dataset):
        val = row.get(column)
        if val is None:
            failures += 1
            if len(unexpected_list) < 5:
                unexpected_list.append({"index": i, "value": val})
                
    return {
        "success": failures == 0,
        "unexpected_count": failures,
        "unexpected_list": unexpected_list
    }

def expect_column_values_to_be_between(dataset: List[Dict], column: str, min_value: Any, max_value: Any) -> Dict:
    failures = 0
    unexpected_list = []
    
    for i, row in enumerate(dataset):
        val = row.get(column)
        if val is None:
            continue # Nulls handled by separate check usually, but strict check fails? 
                     # Let's say this check skips nulls.
            
        if not (min_value <= val <= max_value):
            failures += 1
            if len(unexpected_list) < 5:
                unexpected_list.append({"index": i, "value": val})

    return {
        "success": failures == 0,
        "unexpected_count": failures,
        "unexpected_list": unexpected_list
    }

def expect_column_values_to_be_in_set(dataset: List[Dict], column: str, allowed_set: set) -> Dict:
    failures = 0
    unexpected_list = []
    
    for i, row in enumerate(dataset):
        val = row.get(column)
        if val not in allowed_set:
            failures += 1
            if len(unexpected_list) < 5:
                unexpected_list.append({"index": i, "value": val})
                
    return {
        "success": failures == 0,
        "unexpected_count": failures,
        "unexpected_list": unexpected_list
    }
