"""Rate limiting and retry utilities for scrapers."""
import time
from functools import wraps
from typing import Any, Callable, TypeVar
import random

T = TypeVar('T')

def with_retry_and_backoff(
    retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,)
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that implements exponential backoff and retry logic.
    
    Args:
        retries: Maximum number of retries
        base_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        exceptions: Tuple of exceptions to catch and retry
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            for attempt in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == retries:
                        raise
                    
                    # Calculate delay with exponential backoff and jitter
                    delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                    time.sleep(delay)
            
            raise last_exception  # Should never reach here
        return wrapper
    return decorator

def rate_limit(min_delay: float, max_delay: float) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that ensures a minimum delay between function calls with jitter.
    
    Args:
        min_delay: Minimum delay between calls in seconds
        max_delay: Maximum delay between calls in seconds
    """
    last_call = 0.0
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            nonlocal last_call
            now = time.time()
            
            # Add random jitter between min_delay and max_delay
            delay = random.uniform(min_delay, max_delay)
            
            # If not enough time has passed since last call, wait
            time_since_last = now - last_call
            if time_since_last < delay:
                time.sleep(delay - time_since_last)
            
            result = func(*args, **kwargs)
            last_call = time.time()
            return result
            
        return wrapper
    return decorator
