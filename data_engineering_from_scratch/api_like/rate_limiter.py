import time
import threading
from functools import wraps

class RateLimitExceeded(Exception):
    pass

class RateLimiter:
    """
    Token Bucket Rate Limiter.
    """
    def __init__(self, max_requests: int, period: float):
        self.max_requests = max_requests
        self.period = period
        self.tokens = max_requests
        self.last_update = time.time()
        self.lock = threading.Lock()

    def _add_tokens(self):
        now = time.time()
        elapsed = now - self.last_update
        # Calculate new tokens
        new_tokens = elapsed * (self.max_requests / self.period)
        if new_tokens > 0:
            self.tokens = min(self.max_requests, self.tokens + new_tokens)
            self.last_update = now

    def acquire(self):
        with self.lock:
            self._add_tokens()
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

def rate_limit(limiter: RateLimiter):
    """
    Decorator to apply rate limiting to a function/method.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not limiter.acquire():
                # For simulation, we can raise or sleep. 
                # Raising simulates a 429 response.
                raise RateLimitExceeded("Too many requests")
            return func(*args, **kwargs)
        return wrapper
    return decorator
