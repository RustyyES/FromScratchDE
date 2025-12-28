import sys
import os
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from api_like.client import APIClient
from api_like.rate_limiter import RateLimiter, rate_limit, RateLimitExceeded
from api_like.pagination import Paginator

def test_rate_limiting():
    print("\n--- Testing Rate Limiting ---")
    # Limit: 5 requests per second
    limiter = RateLimiter(max_requests=5, period=1.0)
    
    @rate_limit(limiter)
    def make_request(i):
        print(f"Request {i} passed.")
        return True

    success_count = 0
    start = time.time()
    for i in range(10):
        try:
            make_request(i)
            success_count += 1
        except RateLimitExceeded:
            print(f"Request {i} THROTTLED.")
            
    print(f"Made {success_count} requests in {time.time()-start:.2f}s (Expected ~5 passed).")

def test_pagination():
    print("\n--- Testing Pagination & Retries ---")
    client = APIClient(base_url="http://mock-api.com")
    paginator = Paginator(client, "/users", limit=20)
    
    all_users = []
    print("Fetching users...")
    
    try:
        for user in paginator.iter_items():
            all_users.append(user)
    except Exception as e:
        print(f"Pagination failed: {e}")
        
    print(f"Total Users Fetched: {len(all_users)} (Expected 100)")
    if len(all_users) > 0:
        print(f"First: {all_users[0]}")
        print(f"Last: {all_users[-1]}")

def main():
    test_rate_limiting()
    test_pagination()

if __name__ == "__main__":
    main()
