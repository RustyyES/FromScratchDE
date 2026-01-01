from typing import List, Dict, Callable
import json

class Validator:
    def __init__(self, dataset: List[Dict]):
        self.dataset = dataset
        self.expectations = []
        self.results = []

    def expect(self, func: Callable, **kwargs):
        """
        Registers an expectation to be run.
        Usage: validator.expect(expect_column_values_to_not_be_null, column='id')
        """
        self.expectations.append((func, kwargs))
        return self

    def validate(self) -> Dict:
        """
        Runs all registered expectations.
        """
        print(f"Running validation on {len(self.dataset)} rows...")
        self.results = []
        all_success = True
        
        for func, kwargs in self.expectations:
            print(f"  Running {func.__name__} ({kwargs})...")
            res = func(self.dataset, **kwargs)
            res['expectation_type'] = func.__name__
            res['kwargs'] = kwargs
            
            if not res['success']:
                all_success = False
            
            self.results.append(res)
            
        return {
            "success": all_success,
            "results": self.results
        }
    
    def print_report(self):
        print("\n" + "="*40)
        print("VALIDATION REPORT")
        print("="*40)
        
        for res in self.results:
            icon = "✅" if res['success'] else "❌"
            name = res['expectation_type']
            col = res['kwargs'].get('column', 'Dataset')
            print(f"{icon} {name} on '{col}'")
            if not res['success']:
                print(f"   Unexpected Count: {res.get('unexpected_count', 0)}")
                if 'unexpected_list' in res and res['unexpected_list']:
                    print(f"   Sample: {res['unexpected_list']}")
        print("="*40 + "\n")
