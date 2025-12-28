from typing import Iterator, Dict, Any, List
from .client import APIClient

class Paginator:
    """
    Handles pagination logic for an API endpoint.
    """
    def __init__(self, client: APIClient, endpoint: str, limit: int = 10):
        self.client = client
        self.endpoint = endpoint
        self.limit = limit

    def iter_pages(self) -> Iterator[List[Dict]]:
        """
        Yields pages of results.
        Automatic Retry logic should ideally be here or in client.
        """
        offset = 0
        while True:
            params = {'limit': self.limit, 'offset': offset}
            
            # Simple simulation of retry on 500
            for attempt in range(3):
                response = self.client.get(self.endpoint, params)
                if response.status_code == 200:
                    break
                elif response.status_code == 500:
                    print(f"Server Error. Retrying ({attempt+1}/3)...")
                    continue
                else:
                    raise Exception(f"API Error: {response.status_code}")
            else:
                raise Exception("Max retries exceeded")

            data = response.json()
            items = data.get('data', [])
            
            if not items:
                break
                
            yield items
            
            # Check next cursor/offset
            meta = data.get('meta', {})
            next_offset = meta.get('next_offset')
            
            if next_offset is None:
                break
                
            offset = next_offset

    def iter_items(self) -> Iterator[Dict]:
        """
        Yields individual items from all pages.
        """
        for page in self.iter_pages():
            for item in page:
                yield item
