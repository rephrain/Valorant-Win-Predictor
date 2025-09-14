"""
Base scraper class providing common functionality for all Valorant data scrapers.
Implements rate limiting, error handling, caching, and standardized data extraction patterns.
"""

import time
import logging
import random
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
import json
import pickle

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import cloudscraper
from fake_useragent import UserAgent

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

from config.settings import settings
from config.rate_limits import get_rate_limiter, record_request_error, record_request_success
from utils.storage.cache_manager import CacheManager
from utils.validators.data_quality import DataQualityValidator

# Set up logging
logger = logging.getLogger(__name__)


@dataclass
class ScrapingResult:
    """Standardized result container for scraping operations"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None
    response_time: Optional[float] = None
    cached: bool = False
    url: Optional[str] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class ScrapingConfig:
    """Configuration for scraping behavior"""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    timeout: int = 30
    use_cache: bool = True
    cache_ttl: int = 300  # 5 minutes default
    respect_robots: bool = True
    use_cloudscraper: bool = False
    custom_headers: Optional[Dict[str, str]] = None


class BaseScraper(ABC):
    """
    Abstract base class for all Valorant data scrapers.
    Provides common functionality including rate limiting, caching, and error handling.
    """
    
    def __init__(self, source_name: str, config: Optional[ScrapingConfig] = None):
        self.source_name = source_name.lower()
        self.config = config or ScrapingConfig()
        
        # Initialize components
        self.rate_limiter = get_rate_limiter(self.source_name)
        self.cache_manager = CacheManager()
        self.data_validator = DataQualityValidator()
        
        # Session setup
        self.session = self._create_session()
        
        # User agent rotation
        self.ua = UserAgent()
        self.user_agents = settings.USER_AGENTS.copy()
        
        # Statistics tracking
        self.stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'requests_cached': 0,
            'total_response_time': 0.0,
            'errors': [],
            'last_request_time': None
        }
        
        logger.info(f"Initialized {self.__class__.__name__} for source: {source_name}")
    
    def _create_session(self) -> requests.Session:
        """Create and configure a requests session with retries and adapters"""
        if self.config.use_cloudscraper:
            session = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'windows',
                    'desktop': True
                }
            )
        else:
            session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        if self.config.custom_headers:
            session.headers.update(self.config.custom_headers)
        
        return session
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent string"""
        try:
            if random.random() < 0.7:  # 70% chance to use predefined agents
                return random.choice(self.user_agents)
            else:  # 30% chance to use fake-useragent
                return self.ua.random
        except Exception:
            # Fallback to predefined agents if fake-useragent fails
            return random.choice(self.user_agents)
    
    def _generate_cache_key(self, url: str, params: Optional[Dict] = None) -> str:
        """Generate a cache key for the request"""
        cache_data = {
            'url': url,
            'params': params or {},
            'source': self.source_name
        }
        
        cache_string = json.dumps(cache_data, sort_keys=True)
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    def _should_cache_request(self, url: str, method: str = 'GET') -> bool:
        """Determine if a request should be cached"""
        if not self.config.use_cache or method.upper() != 'GET':
            return False
        
        # Don't cache certain URLs (live data, user-specific content)
        nocache_patterns = [
            '/live', '/api/user', '/login', '/logout', 
            'timestamp', 'cache-bust', 'random'
        ]
        
        url_lower = url.lower()
        return not any(pattern in url_lower for pattern in nocache_patterns)
    
    def _make_request(self, 
                     url: str, 
                     method: str = 'GET',
                     params: Optional[Dict] = None,
                     data: Optional[Dict] = None,
                     headers: Optional[Dict] = None,
                     **kwargs) -> ScrapingResult:
        """
        Make an HTTP request with rate limiting, caching, and error handling
        """
        start_time = time.time()
        
        # Check cache first
        cache_key = None
        if self._should_cache_request(url, method):
            cache_key = self._generate_cache_key(url, params)
            cached_result = self.cache_manager.get(cache_key)
            
            if cached_result:
                self.stats['requests_cached'] += 1
                logger.debug(f"Cache hit for {url}")
                
                return ScrapingResult(
                    success=True,
                    data=cached_result,
                    cached=True,
                    url=url,
                    response_time=0.0
                )
        
        # Apply rate limiting
        if not self.rate_limiter.acquire(timeout=30):
            error_msg = f"Rate limit timeout for {self.source_name}"
            logger.warning(error_msg)
            self.stats['requests_failed'] += 1
            
            return ScrapingResult(
                success=False,
                error=error_msg,
                url=url
            )
        
        # Prepare request
        request_headers = headers or {}
        request_headers['User-Agent'] = self._get_random_user_agent()
        
        # Add random delay to seem more human
        if random.random() < 0.3:  # 30% chance of random delay
            time.sleep(random.uniform(0.5, 2.0))
        
        try:
            self.stats['requests_made'] += 1
            self.stats['last_request_time'] = datetime.now()
            
            # Make the request
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                headers=request_headers,
                timeout=self.config.timeout,
                **kwargs
            )
            
            response_time = time.time() - start_time
            self.stats['total_response_time'] += response_time
            
            # Handle response
            if response.status_code == 200:
                # Success
                self.stats['requests_successful'] += 1
                record_request_success(self.source_name)
                
                # Parse response data
                try:
                    if 'application/json' in response.headers.get('content-type', ''):
                        data = response.json()
                    else:
                        data = {
                            'content': response.text,
                            'headers': dict(response.headers),
                            'url': response.url
                        }
                    
                    # Cache successful response
                    if cache_key:
                        self.cache_manager.set(cache_key, data, ttl=self.config.cache_ttl)
                    
                    return ScrapingResult(
                        success=True,
                        data=data,
                        status_code=response.status_code,
                        response_time=response_time,
                        url=url
                    )
                    
                except Exception as e:
                    error_msg = f"Failed to parse response from {url}: {str(e)}"
                    logger.error(error_msg)
                    self.stats['requests_failed'] += 1
                    
                    return ScrapingResult(
                        success=False,
                        error=error_msg,
                        status_code=response.status_code,
                        response_time=response_time,
                        url=url
                    )
            
            elif response.status_code == 429:
                # Rate limited
                error_msg = f"Rate limited by {self.source_name} (429)"
                logger.warning(error_msg)
                record_request_error(self.source_name, "rate_limit")
                
                # Increase delay for next request
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        delay = int(retry_after)
                        logger.info(f"Retry-After header suggests {delay}s delay")
                        time.sleep(min(delay, 300))  # Max 5 minute delay
                    except ValueError:
                        time.sleep(60)  # Default 1 minute delay
                
                self.stats['requests_failed'] += 1
                return ScrapingResult(
                    success=False,
                    error=error_msg,
                    status_code=response.status_code,
                    response_time=response_time,
                    url=url
                )
            
            elif response.status_code in [403, 404]:
                # Client errors
                error_msg = f"Client error {response.status_code} for {url}"
                logger.warning(error_msg)
                record_request_error(self.source_name, f"client_error_{response.status_code}")
                
                self.stats['requests_failed'] += 1
                return ScrapingResult(
                    success=False,
                    error=error_msg,
                    status_code=response.status_code,
                    response_time=response_time,
                    url=url
                )
            
            else:
                # Other HTTP errors
                error_msg = f"HTTP {response.status_code} error for {url}"
                logger.error(error_msg)
                record_request_error(self.source_name, f"http_error_{response.status_code}")
                
                self.stats['requests_failed'] += 1
                return ScrapingResult(
                    success=False,
                    error=error_msg,
                    status_code=response.status_code,
                    response_time=response_time,
                    url=url
                )
        
        except requests.exceptions.Timeout:
            error_msg = f"Request timeout for {url}"
            logger.error(error_msg)
            record_request_error(self.source_name, "timeout")
            
            self.stats['requests_failed'] += 1
            return ScrapingResult(
                success=False,
                error=error_msg,
                url=url,
                response_time=time.time() - start_time
            )
        
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Connection error for {url}: {str(e)}"
            logger.error(error_msg)
            record_request_error(self.source_name, "connection_error")
            
            self.stats['requests_failed'] += 1
            return ScrapingResult(
                success=False,
                error=error_msg,
                url=url,
                response_time=time.time() - start_time
            )
        
        except Exception as e:
            error_msg = f"Unexpected error for {url}: {str(e)}"
            logger.error(error_msg)
            record_request_error(self.source_name, "unexpected_error")
            
            self.stats['requests_failed'] += 1
            self.stats['errors'].append({
                'url': url,
                'error': str(e),
                'timestamp': datetime.now()
            })
            
            return ScrapingResult(
                success=False,
                error=error_msg,
                url=url,
                response_time=time.time() - start_time
            )
    
    def _extract_with_soup(self, html_content: str, selectors: Dict[str, str], 
                          transforms: Optional[Dict[str, Callable]] = None) -> Dict[str, Any]:
        """
        Extract data from HTML using BeautifulSoup and CSS selectors
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        extracted_data = {}
        transforms = transforms or {}
        
        for field_name, selector in selectors.items():
            try:
                elements = soup.select(selector)
                
                if not elements:
                    extracted_data[field_name] = None
                    continue
                
                # Extract text or attribute
                if len(elements) == 1:
                    element = elements[0]
                    # Check if selector specifies an attribute
                    if '::attr(' in selector:
                        attr_name = selector.split('::attr(')[1].rstrip(')')
                        value = element.get(attr_name)
                    else:
                        value = element.get_text(strip=True)
                else:
                    # Multiple elements
                    values = []
                    for element in elements:
                        if '::attr(' in selector:
                            attr_name = selector.split('::attr(')[1].rstrip(')')
                            values.append(element.get(attr_name))
                        else:
                            values.append(element.get_text(strip=True))
                    value = values
                
                # Apply transformation if specified
                if field_name in transforms:
                    try:
                        value = transforms[field_name](value)
                    except Exception as e:
                        logger.warning(f"Transform failed for {field_name}: {e}")
                        value = None
                
                extracted_data[field_name] = value
                
            except Exception as e:
                logger.warning(f"Failed to extract {field_name} with selector {selector}: {e}")
                extracted_data[field_name] = None
        
        return extracted_data
    
    def _validate_extracted_data(self, data: Dict[str, Any], 
                                required_fields: Optional[List[str]] = None) -> bool:
        """
        Validate extracted data meets minimum quality requirements
        """
        if not data:
            return False
        
        required_fields = required_fields or []
        
        # Check required fields
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.warning(f"Missing required field: {field}")
                return False
        
        # Check data quality using validator
        quality_result = self.data_validator.validate_scraped_data(data, self.source_name)
        
        if not quality_result['is_valid']:
            logger.warning(f"Data quality validation failed: {quality_result['issues']}")
            return False
        
        return True
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get scraping statistics and performance metrics"""
        avg_response_time = (
            self.stats['total_response_time'] / max(self.stats['requests_successful'], 1)
        )
        
        success_rate = (
            self.stats['requests_successful'] / max(self.stats['requests_made'], 1)
        )
        
        return {
            'source': self.source_name,
            'requests_made': self.stats['requests_made'],
            'requests_successful': self.stats['requests_successful'],
            'requests_failed': self.stats['requests_failed'],
            'requests_cached': self.stats['requests_cached'],
            'success_rate': success_rate,
            'average_response_time': avg_response_time,
            'last_request_time': self.stats['last_request_time'],
            'recent_errors': self.stats['errors'][-5:],  # Last 5 errors
        }
    
    def reset_statistics(self):
        """Reset all statistics counters"""
        self.stats = {
            'requests_made': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'requests_cached': 0,
            'total_response_time': 0.0,
            'errors': [],
            'last_request_time': None
        }
        logger.info(f"Reset statistics for {self.source_name} scraper")
    
    def close(self):
        """Clean up resources"""
        if hasattr(self, 'session'):
            self.session.close()
        
        logger.info(f"Closed {self.source_name} scraper")
    
    # Abstract methods that must be implemented by subclasses
    @abstractmethod
    def scrape_match_data(self, match_url: str, match_id: str) -> Optional[Dict[str, Any]]:
        """Scrape detailed match data from a match page/endpoint"""
        pass
    
    @abstractmethod
    def scrape_team_data(self, team_url: str, team_id: str) -> Optional[Dict[str, Any]]:
        """Scrape team information and statistics"""
        pass
    
    @abstractmethod
    def scrape_player_data(self, player_url: str, player_id: str) -> Optional[Dict[str, Any]]:
        """Scrape player profile and statistics"""
        pass
    
    @abstractmethod
    def search_matches(self, **filters) -> List[Dict[str, Any]]:
        """Search for matches based on filters"""
        pass
    
    # Optional methods with default implementations
    def scrape_event_data(self, event_url: str, event_id: str) -> Optional[Dict[str, Any]]:
        """Scrape event/tournament information"""
        logger.warning(f"Event scraping not implemented for {self.source_name}")
        return None
    
    def scrape_rankings(self) -> Optional[List[Dict[str, Any]]]:
        """Scrape team/player rankings"""
        logger.warning(f"Rankings scraping not implemented for {self.source_name}")
        return None
    
    def test_connectivity(self) -> bool:
        """Test if the scraper can connect to the source"""
        try:
            # Override in subclasses with source-specific test
            test_url = "https://httpbin.org/get"  # Generic test
            result = self._make_request(test_url)
            return result.success
        except Exception as e:
            logger.error(f"Connectivity test failed for {self.source_name}: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        
        if exc_type:
            logger.error(f"Exception in {self.source_name} scraper: {exc_val}")
        
        return False  # Don't suppress exceptions


# Utility functions for common scraping tasks
def clean_text(text: str) -> str:
    """Clean and normalize scraped text"""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    # Remove common artifacts
    text = text.replace('\u00a0', ' ')  # Non-breaking space
    text = text.replace('\u200b', '')   # Zero-width space
    text = text.strip()
    
    return text


def parse_number(text: str) -> Optional[float]:
    """Parse a number from text, handling common formats"""
    if not text:
        return None
    
    # Clean the text
    text = text.strip().replace(',', '').replace('%', '')
    
    try:
        # Try integer first
        if '.' not in text:
            return int(text)
        else:
            return float(text)
    except ValueError:
        return None


def extract_id_from_url(url: str) -> Optional[str]:
    """Extract ID from URL patterns like /team/123/team-name"""
    import re
    
    patterns = [
        r'/(\d+)/',           # /123/
        r'/(\d+)$',           # /123
        r'id=(\d+)',          # id=123
        r'[?&]id=(\d+)',      # ?id=123 or &id=123
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None