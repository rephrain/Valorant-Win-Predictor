import time
from functools import wraps
from config.settings import SCRAPE_DELAY

def rate_limit(func):
    """Decorator to add delay between requests"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        time.sleep(SCRAPE_DELAY)
        return func(*args, **kwargs)
    return wrapper