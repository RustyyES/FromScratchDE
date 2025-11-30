import csv
import json
from typing import Iterator, Dict, Any

def read_csv(path: str) -> Iterator[Dict[str, Any]]:
    """
    Lazily reads a CSV file and yields rows as dictionaries.
    """
    with open(path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row

def read_json(path: str) -> Iterator[Dict[str, Any]]:
    """
    Lazily reads a JSON file (line-delimited or standard list of objects).
    """
    with open(path, mode='r', encoding='utf-8') as f:
        # Check first char to see if it's a list or newline delimited
        first_char = f.read(1)
        f.seek(0)
        
        if first_char == '[':
            # Standard JSON array - requires loading full file
            data = json.load(f)
            for item in data:
                yield item
        else:
            # Assume Line-delimited JSON (NDJSON) - fully lazy!
            for line in f:
                if line.strip():
                    yield json.loads(line)
