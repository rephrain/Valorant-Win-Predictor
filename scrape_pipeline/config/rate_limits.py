"""
Rate limiting configuration for different scraping sources.
Implements various rate limiting strategies to avoid getting blocked.
"""

import time
import asyncio
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from collections import defaultdict, deque
import random


class RateLimitStrategy(Enum):
    """Different rate limiting strategies"""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"
    EXPONENTIAL_BACKOFF = "exponential_backoff"


@dataclass
class RateLimitConfig:
    """Configuration for a specific rate limiter"""
    requests_per_minute: int
    requests_per_hour: int
    burst_size: int = 5  # Max burst requests
    strategy: RateLimitStrategy = RateLimitStrategy.SLIDING_WINDOW
    backoff_factor: float = 1.5  # For exponential backoff
    max_backoff: int = 300  # Max backoff in seconds
    jitter: bool = True  # Add randomness to avoid thundering herd


@dataclass
class SourceRateLimits:
    """Rate limiting configuration for different data sources"""
    
    # VLR.gg - More conservative due to CloudFlare protection
    VLR: RateLimitConfig = field(default_factory=lambda: RateLimitConfig(
        requests_per_minute=20,
        requests_per_hour=800,
        burst_size=3,
        strategy=RateLimitStrategy.SLIDING_WINDOW,
        backoff_factor=2.0,
        max_backoff=600,
        jitter=True
    ))
    
    # TheSpike.gg - Moderate limits
    THESPIKE: RateLimitConfig = field(default_factory=lambda: RateLimitConfig(
        requests_per_minute=40,
        requests_per_hour=1500,
        burst_size=5,
        strategy=RateLimitStrategy.SLIDING_WINDOW,
        backoff_factor=1.5,
        max_backoff=300,
        jitter=True
    ))
    
    # Liquipedia - More generous but still respectful
    LIQUIPEDIA: RateLimitConfig = field(default_factory=lambda: RateLimitConfig(
        requests_per_minute=60,
        requests_per_hour=2000,
        burst_size=8,
        strategy=RateLimitStrategy.SLIDING_WINDOW,
        backoff_factor=1.3,
        max_backoff=180,
        jitter=True
    ))
    
    # Riot API - If available, follows their rate limits
    RIOT_API: RateLimitConfig = field(default_factory=lambda: RateLimitConfig(
        requests_per_minute=100,
        requests_per_hour=3000,
        burst_size=10,
        strategy=RateLimitStrategy.TOKEN_BUCKET,
        backoff_factor=2.0,
        max_backoff=300,
        jitter=False  # API is more predictable
    ))
    
    # Default for unknown sources
    DEFAULT: RateLimitConfig = field(default_factory=lambda: RateLimitConfig(
        requests_per_minute=30,
        requests_per_hour=1000,
        burst_size=3,
        strategy=RateLimitStrategy.SLIDING_WINDOW,
        backoff_factor=1.5,
        max_backoff=300,
        jitter=True
    ))


class RateLimiter:
    """
    Rate limiter implementation with multiple strategies
    Thread-safe and supports both sync and async usage
    """
    
    def __init__(self, config: RateLimitConfig, source_name: str = "unknown"):
        self.config = config
        self.source_name = source_name
        self.lock = Lock()
        
        # Request tracking
        self.request_times: deque = deque()
        self.hourly_request_times: deque = deque()
        
        # Token bucket state
        self.tokens = config.burst_size
        self.last_refill = time.time()
        
        # Backoff state
        self.consecutive_errors = 0
        self.last_error_time: Optional[float] = None
        
        # Statistics
        self.total_requests = 0
        self.blocked_requests = 0
        self.errors = 0
        
    def _clean_old_requests(self):
        """Remove old request timestamps"""
        now = time.time()
        minute_ago = now - 60
        hour_ago = now - 3600
        
        # Clean minute window
        while self.request_times and self.request_times[0] < minute_ago:
            self.request_times.popleft()
            
        # Clean hour window
        while self.hourly_request_times and self.hourly_request_times[0] < hour_ago:
            self.hourly_request_times.popleft()
    
    def _refill_tokens(self):
        """Refill tokens for token bucket strategy"""
        now = time.time()
        time_passed = now - self.last_refill
        
        # Add tokens based on rate (requests per minute / 60 seconds)
        tokens_to_add = time_passed * (self.config.requests_per_minute / 60)
        self.tokens = min(self.config.burst_size, self.tokens + tokens_to_add)
        self.last_refill = now
    
    def _calculate_backoff(self) -> float:
        """Calculate backoff delay based on consecutive errors"""
        if self.consecutive_errors == 0:
            return 0
        
        base_delay = 2 ** min(self.consecutive_errors - 1, 10)  # Cap at 2^10
        delay = min(base_delay * self.config.backoff_factor, self.config.max_backoff)
        
        if self.config.jitter:
            # Add ±25% jitter
            jitter_range = delay * 0.25
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)
    
    def _can_make_request(self) -> Tuple[bool, float]:
        """
        Check if request can be made and return (can_proceed, delay_needed)
        """
        with self.lock:
            self._clean_old_requests()
            now = time.time()
            
            # Check if in backoff period
            if self.last_error_time:
                backoff_delay = self._calculate_backoff()
                time_since_error = now - self.last_error_time
                if time_since_error < backoff_delay:
                    return False, backoff_delay - time_since_error
            
            # Strategy-specific checks
            if self.config.strategy == RateLimitStrategy.TOKEN_BUCKET:
                self._refill_tokens()
                if self.tokens < 1:
                    # Calculate time until next token
                    time_per_token = 60 / self.config.requests_per_minute
                    return False, time_per_token
                    
            elif self.config.strategy in [RateLimitStrategy.SLIDING_WINDOW, RateLimitStrategy.FIXED_WINDOW]:
                # Check minute limit
                if len(self.request_times) >= self.config.requests_per_minute:
                    oldest_request = self.request_times[0]
                    time_until_available = 60 - (now - oldest_request)
                    if time_until_available > 0:
                        return False, time_until_available
                
                # Check hour limit
                if len(self.hourly_request_times) >= self.config.requests_per_hour:
                    oldest_hourly = self.hourly_request_times[0]
                    time_until_available = 3600 - (now - oldest_hourly)
                    if time_until_available > 0:
                        return False, time_until_available
            
            return True, 0
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire permission to make a request (blocking)
        Returns True if acquired, False if timeout exceeded
        """
        start_time = time.time()
        
        while True:
            can_proceed, delay = self._can_make_request()
            
            if can_proceed:
                with self.lock:
                    now = time.time()
                    
                    # Record the request
                    self.request_times.append(now)
                    self.hourly_request_times.append(now)
                    
                    # Update token bucket
                    if self.config.strategy == RateLimitStrategy.TOKEN_BUCKET:
                        self.tokens -= 1
                    
                    # Reset error count on successful acquire
                    self.consecutive_errors = 0
                    self.last_error_time = None
                    
                    self.total_requests += 1
                    
                return True
            
            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    self.blocked_requests += 1
                    return False
                
                delay = min(delay, timeout - elapsed)
            
            # Add jitter to delay if enabled
            if self.config.jitter and delay > 0:
                jitter_amount = delay * 0.1  # 10% jitter
                delay += random.uniform(-jitter_amount, jitter_amount)
                delay = max(0, delay)
            
            time.sleep(delay)
    
    async def acquire_async(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire permission to make a request (async)
        Returns True if acquired, False if timeout exceeded
        """
        start_time = time.time()
        
        while True:
            can_proceed, delay = self._can_make_request()
            
            if can_proceed:
                with self.lock:
                    now = time.time()
                    
                    # Record the request
                    self.request_times.append(now)
                    self.hourly_request_times.append(now)
                    
                    # Update token bucket
                    if self.config.strategy == RateLimitStrategy.TOKEN_BUCKET:
                        self.tokens -= 1
                    
                    # Reset error count on successful acquire
                    self.consecutive_errors = 0
                    self.last_error_time = None
                    
                    self.total_requests += 1
                    
                return True
            
            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    self.blocked_requests += 1
                    return False
                
                delay = min(delay, timeout - elapsed)
            
            # Add jitter to delay if enabled
            if self.config.jitter and delay > 0:
                jitter_amount = delay * 0.1  # 10% jitter
                delay += random.uniform(-jitter_amount, jitter_amount)
                delay = max(0, delay)
            
            await asyncio.sleep(delay)
    
    def record_error(self, error_type: str = "generic"):
        """Record an error for backoff calculation"""
        with self.lock:
            self.consecutive_errors += 1
            self.last_error_time = time.time()
            self.errors += 1
    
    def record_success(self):
        """Record a successful request"""
        with self.lock:
            self.consecutive_errors = 0
            self.last_error_time = None
    
    def get_stats(self) -> Dict:
        """Get rate limiter statistics"""
        with self.lock:
            self._clean_old_requests()
            
            return {
                "source": self.source_name,
                "total_requests": self.total_requests,
                "blocked_requests": self.blocked_requests,
                "errors": self.errors,
                "consecutive_errors": self.consecutive_errors,
                "current_minute_requests": len(self.request_times),
                "current_hour_requests": len(self.hourly_request_times),
                "tokens_available": self.tokens if self.config.strategy == RateLimitStrategy.TOKEN_BUCKET else None,
                "requests_per_minute_limit": self.config.requests_per_minute,
                "requests_per_hour_limit": self.config.requests_per_hour,
            }
    
    def reset(self):
        """Reset all rate limiting state"""
        with self.lock:
            self.request_times.clear()
            self.hourly_request_times.clear()
            self.tokens = self.config.burst_size
            self.last_refill = time.time()
            self.consecutive_errors = 0
            self.last_error_time = None


class RateLimiterManager:
    """
    Manages rate limiters for different sources
    Singleton pattern to ensure consistent rate limiting across the application
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.rate_limits = SourceRateLimits()
            self.limiters: Dict[str, RateLimiter] = {}
            self.initialized = True
    
    def get_limiter(self, source: str) -> RateLimiter:
        """Get or create a rate limiter for a source"""
        if source not in self.limiters:
            # Map source names to configurations
            config_map = {
                'vlr': self.rate_limits.VLR,
                'vlr.gg': self.rate_limits.VLR,
                'thespike': self.rate_limits.THESPIKE,
                'thespike.gg': self.rate_limits.THESPIKE,
                'liquipedia': self.rate_limits.LIQUIPEDIA,
                'riot_api': self.rate_limits.RIOT_API,
                'riot': self.rate_limits.RIOT_API,
            }
            
            config = config_map.get(source.lower(), self.rate_limits.DEFAULT)
            self.limiters[source] = RateLimiter(config, source)
        
        return self.limiters[source]
    
    def get_all_stats(self) -> Dict[str, Dict]:
        """Get statistics for all rate limiters"""
        return {source: limiter.get_stats() for source, limiter in self.limiters.items()}
    
    def reset_all(self):
        """Reset all rate limiters"""
        for limiter in self.limiters.values():
            limiter.reset()


# Global rate limiter manager instance
rate_limiter_manager = RateLimiterManager()


# Convenience functions
def get_rate_limiter(source: str) -> RateLimiter:
    """Get rate limiter for a specific source"""
    return rate_limiter_manager.get_limiter(source)


def acquire_request_permission(source: str, timeout: Optional[float] = None) -> bool:
    """Acquire permission to make a request to a source"""
    limiter = get_rate_limiter(source)
    return limiter.acquire(timeout)


async def acquire_request_permission_async(source: str, timeout: Optional[float] = None) -> bool:
    """Async version of acquire_request_permission"""
    limiter = get_rate_limiter(source)
    return await limiter.acquire_async(timeout)


def record_request_error(source: str, error_type: str = "generic"):
    """Record an error for backoff calculation"""
    limiter = get_rate_limiter(source)
    limiter.record_error(error_type)


def record_request_success(source: str):
    """Record a successful request"""
    limiter = get_rate_limiter(source)
    limiter.record_success()


def get_rate_limiting_stats() -> Dict[str, Dict]:
    """Get statistics for all rate limiters"""
    return rate_limiter_manager.get_all_stats()


# Context managers for easier usage
class RateLimitedRequest:
    """Context manager for rate-limited requests"""
    
    def __init__(self, source: str, timeout: Optional[float] = None):
        self.source = source
        self.timeout = timeout
        self.limiter = get_rate_limiter(source)
        self.acquired = False
    
    def __enter__(self):
        self.acquired = self.limiter.acquire(self.timeout)
        if not self.acquired:
            raise TimeoutError(f"Could not acquire rate limit permission for {self.source}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            if exc_type is None:
                self.limiter.record_success()
            else:
                self.limiter.record_error(str(exc_type.__name__) if exc_type else "unknown")


class AsyncRateLimitedRequest:
    """Async context manager for rate-limited requests"""
    
    def __init__(self, source: str, timeout: Optional[float] = None):
        self.source = source
        self.timeout = timeout
        self.limiter = get_rate_limiter(source)
        self.acquired = False
    
    async def __aenter__(self):
        self.acquired = await self.limiter.acquire_async(self.timeout)
        if not self.acquired:
            raise TimeoutError(f"Could not acquire rate limit permission for {self.source}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.acquired:
            if exc_type is None:
                self.limiter.record_success()
            else:
                self.limiter.record_error(str(exc_type.__name__) if exc_type else "unknown")