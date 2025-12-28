import time
import random
from typing import Dict, Any, Optional

class Response:
    def __init__(self, status_code: int, json_data: Any = None):
        self.status_code = status_code
        self.json_data = json_data

    def json(self):
        return self.json_data

class APIClient:
    """
    Simulates a REST API Client.
    """
    def __init__(self, base_url: str):
        self.base_url = base_url
        # Simulated database/state
        self.data = [
            {'id': i, 'name': f"User_{i}"} for i in range(100)
        ]

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Response:
        """
        Simulate GET request.
        """
        # Simulate Network Latency
        time.sleep(random.uniform(0.05, 0.2))
        
        # Simulate Random Server Error (5%)
        if random.random() < 0.05:
            return Response(500, {"error": "Internal Server Error"})

        if endpoint == "/users":
            return self._get_users(params)
            
        return Response(404, {"error": "Not Found"})

    def _get_users(self, params: Optional[Dict]) -> Response:
        params = params or {}
        limit = int(params.get('limit', 10))
        offset = int(params.get('offset', 0))
        
        # Slice data
        result = self.data[offset : offset + limit]
        
        return Response(200, {
            "data": result,
            "meta": {
                "limit": limit,
                "offset": offset,
                "total": len(self.data),
                "next_offset": offset + limit if offset + limit < len(self.data) else None
            }
        })

    def post(self, endpoint: str, data: Dict) -> Response:
        # Simulate POST
        time.sleep(0.1)
        return Response(201, {"message": "Created", "id": 999})
