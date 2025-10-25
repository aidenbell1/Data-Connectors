"""
Base Connector Class

This is the abstract base class that all connectors inherit from.
It provides common functionality:
- Rate limiting
- Retry logic
- Pagination handling
- Error handling
- Logging
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Generator
import requests
import time
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from pydantic import BaseModel, ValidationError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConnectorConfig(BaseModel):
    """Configuration for a connector"""
    base_url: str
    api_key: Optional[str] = None
    rate_limit_calls: int = 60  # calls per minute
    rate_limit_period: int = 60  # seconds
    max_retries: int = 3
    timeout: int = 30


class BaseConnector(ABC):
    """
    Abstract base class for all data connectors.
    
    Subclasses must implement:
    - extract(): Main data extraction logic
    - validate_response(): Validate API response
    """
    
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self.session = requests.Session()
        self._rate_limit_calls = []  # Track API calls for rate limiting
        
        # Set up session headers
        if config.api_key:
            self.session.headers.update(self._get_auth_headers())
    
    @abstractmethod
    def _get_auth_headers(self) -> Dict[str, str]:
        """Return authentication headers. Override in subclass."""
        pass
    
    @abstractmethod
    def extract(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Main extraction method. Override in subclass.
        
        Args:
            **kwargs: Connector-specific parameters
            
        Returns:
            List of records extracted from the API
        """
        pass
    
    @abstractmethod
    def validate_response(self, data: Any) -> bool:
        """
        Validate API response structure.
        
        Args:
            data: Response data to validate
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def _rate_limit_check(self):
        """
        Check if we're within rate limits.
        If not, sleep until we are.
        """
        now = time.time()
        # Remove old calls outside the rate limit period
        self._rate_limit_calls = [
            call_time for call_time in self._rate_limit_calls
            if now - call_time < self.config.rate_limit_period
        ]
        
        # If we've hit the limit, wait
        if len(self._rate_limit_calls) >= self.config.rate_limit_calls:
            oldest_call = min(self._rate_limit_calls)
            sleep_time = self.config.rate_limit_period - (now - oldest_call)
            if sleep_time > 0:
                logger.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        # Record this call
        self._rate_limit_calls.append(time.time())
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True
    )
    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Make HTTP request with rate limiting and retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc)
            url: Full URL to request
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
            
        Raises:
            requests.RequestException: If request fails after retries
        """
        self._rate_limit_check()
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                timeout=self.config.timeout,
                **kwargs
            )
            response.raise_for_status()
            return response
            
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def get(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Convenience method for GET requests"""
        url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
        response = self._make_request("GET", url, **kwargs)
        return response.json()
    
    def post(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Convenience method for POST requests"""
        url = f"{self.config.base_url}/{endpoint.lstrip('/')}"
        response = self._make_request("POST", url, **kwargs)
        return response.json()
    
    def paginate_offset(
        self, 
        endpoint: str, 
        limit: int = 100,
        max_pages: Optional[int] = None,
        **kwargs
    ) -> Generator[List[Dict], None, None]:
        """
        Paginate using offset-based pagination.
        
        Args:
            endpoint: API endpoint
            limit: Records per page
            max_pages: Maximum pages to fetch (None = all)
            **kwargs: Additional query parameters
            
        Yields:
            List of records per page
        """
        offset = 0
        page = 0
        
        while True:
            if max_pages and page >= max_pages:
                break
                
            params = kwargs.get('params', {})
            params.update({'limit': limit, 'offset': offset})
            kwargs['params'] = params
            
            try:
                data = self.get(endpoint, **kwargs)
                records = self._extract_records_from_response(data)
                
                if not records:
                    break
                    
                yield records
                
                if len(records) < limit:
                    break
                    
                offset += limit
                page += 1
                
            except Exception as e:
                logger.error(f"Pagination failed at offset {offset}: {e}")
                break
    
    def paginate_cursor(
        self,
        endpoint: str,
        cursor_param: str = 'cursor',
        cursor_path: str = 'next_cursor',
        max_pages: Optional[int] = None,
        **kwargs
    ) -> Generator[List[Dict], None, None]:
        """
        Paginate using cursor-based pagination.
        
        Args:
            endpoint: API endpoint
            cursor_param: Query parameter name for cursor
            cursor_path: Path to next cursor in response (dot notation)
            max_pages: Maximum pages to fetch
            **kwargs: Additional parameters
            
        Yields:
            List of records per page
        """
        cursor = None
        page = 0
        
        while True:
            if max_pages and page >= max_pages:
                break
                
            params = kwargs.get('params', {})
            if cursor:
                params[cursor_param] = cursor
            kwargs['params'] = params
            
            try:
                data = self.get(endpoint, **kwargs)
                records = self._extract_records_from_response(data)
                
                if not records:
                    break
                    
                yield records
                
                # Get next cursor
                cursor = self._get_nested_value(data, cursor_path)
                if not cursor:
                    break
                    
                page += 1
                
            except Exception as e:
                logger.error(f"Pagination failed at page {page}: {e}")
                break
    
    def _extract_records_from_response(self, data: Any) -> List[Dict]:
        """
        Extract records from API response.
        Override in subclass if response has nested structure.
        
        Args:
            data: API response data
            
        Returns:
            List of records
        """
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'data' in data:
            return data['data']
        elif isinstance(data, dict) and 'results' in data:
            return data['results']
        elif isinstance(data, dict) and 'items' in data:
            return data['items']
        else:
            return [data]
    
    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """
        Get nested value from dict using dot notation.
        
        Example: _get_nested_value({'a': {'b': 'c'}}, 'a.b') -> 'c'
        """
        keys = path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    def close(self):
        """Clean up resources"""
        self.session.close()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()