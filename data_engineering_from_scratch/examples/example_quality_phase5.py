import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from quality.validator import Validator
from quality.expectations import (
    expect_column_to_exist,
    expect_column_values_to_not_be_null,
    expect_column_values_to_be_between,
    expect_column_values_to_be_in_set
)

def main():
    print("--- Mini-DQ Framework Phase 5: Verification ---")
    
    # Dirty Dataset
    data = [
        {"id": 1, "age": 25, "role": "admin"},
        {"id": 2, "age": 30, "role": "user"},
        {"id": 3, "age": None, "role": "user"}, # Null Age
        {"id": 4, "age": 15, "role": "guest"},  # Underage (suppose 18+)
        {"id": 5, "age": 40, "role": "hacker"}  # Invalid Role
    ]
    
    validator = Validator(data)
    
    # Define Suite
    (validator
     .expect(expect_column_to_exist, column="id")
     .expect(expect_column_values_to_not_be_null, column="age")
     .expect(expect_column_values_to_be_between, column="age", min_value=18, max_value=100)
     .expect(expect_column_values_to_be_in_set, column="role", allowed_set={"admin", "user", "guest"})
    )
    
    # Validate
    report = validator.validate()
    validator.print_report()
    
    # Assertions
    results = {r['expectation_type']: r for r in report['results']}
    
    # 1. ID exists -> Success
    assert results['expect_column_to_exist']['success'] == True
    
    # 2. Age Not Null -> Fail (1 null)
    assert results['expect_column_values_to_not_be_null']['success'] == False
    assert results['expect_column_values_to_not_be_null']['unexpected_count'] == 1
    
    # 3. Age Between 18-100 -> Fail (15 is < 18)
    assert results['expect_column_values_to_be_between']['success'] == False
    
    # 4. Role in Set -> Fail ('hacker')
    assert results['expect_column_values_to_be_in_set']['success'] == False
    
    print("Verification Successful: Errors correctly detected.")

if __name__ == "__main__":
    main()
