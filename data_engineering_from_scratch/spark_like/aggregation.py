from typing import Any, Callable, Dict, List, Union
from collections import defaultdict

class GroupedDataFrame:
    """
    Represents the result of a groupBy operation.
    Holds data grouped by keys, ready for aggregation.
    """
    def __init__(self, grouped_data: Dict[Any, List[Any]]):
        self.grouped_data = grouped_data

    def count(self) -> Dict[Any, int]:
        """
        Count number of elements per group.
        """
        return {k: len(v) for k, v in self.grouped_data.items()}

    def sum(self, field: str = None) -> Dict[Any, Union[int, float]]:
        """
        Sum values per group.
        If data is a dict, sum by `field`.
        If data is atomic (int/float), sum directly.
        """
        result = {}
        for key, values in self.grouped_data.items():
            if not values:
                result[key] = 0
                continue
            
            if isinstance(values[0], dict) and field:
                val_sum = sum(item[field] for item in values)
            else:
                val_sum = sum(values)
            result[key] = val_sum
        return result

    def avg(self, field: str = None) -> Dict[Any, float]:
        """
        Average value per group.
        """
        result = {}
        for key, values in self.grouped_data.items():
            if not values:
                result[key] = 0.0
                continue
            
            count = len(values)
            if isinstance(values[0], dict) and field:
                total = sum(item[field] for item in values)
            else:
                total = sum(values)
            result[key] = total / count
        return result

    def min(self, field: str = None) -> Dict[Any, Any]:
        """
        Min value per group.
        """
        result = {}
        for key, values in self.grouped_data.items():
            if not values:
                result[key] = None
                continue
            
            if isinstance(values[0], dict) and field:
                val = min(item[field] for item in values)
            else:
                val = min(values)
            result[key] = val
        return result

    def max(self, field: str = None) -> Dict[Any, Any]:
        """
        Max value per group.
        """
        result = {}
        for key, values in self.grouped_data.items():
            if not values:
                result[key] = None
                continue
            
            if isinstance(values[0], dict) and field:
                val = max(item[field] for item in values)
            else:
                val = max(values)
            result[key] = val
        return result
    
    def agg(self, agg_map: Dict[str, str]) -> Dict[Any, Dict[str, Any]]:
        """
        Apply multiple aggregations.
        agg_map example: {'revenue': 'sum', 'orders': 'count'}
        Returns: {group_key: {'revenue': 1000, 'orders': 50}}
        """
        result = {}
        for key, values in self.grouped_data.items():
            group_res = {}
            for field, func_name in agg_map.items():
                if not values:
                    group_res[field] = None
                    continue

                # Extract values for the field
                if isinstance(values[0], dict):
                    field_values = [v[field] for v in values]
                else:
                    # If data is not dict, field name might be ignored or error
                    # Assuming dicts if fields are specified
                    field_values = values # Fallback or error?
                
                if func_name == 'sum':
                    group_res[field] = sum(field_values)
                elif func_name == 'count':
                    group_res[field] = len(field_values)
                elif func_name == 'avg':
                    group_res[field] = sum(field_values) / len(field_values)
                elif func_name == 'min':
                    group_res[field] = min(field_values)
                elif func_name == 'max':
                    group_res[field] = max(field_values)
            
            result[key] = group_res
        return result
