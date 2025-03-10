import logging
import requests
from typing import Dict, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("hoppe_etl_pipeline")

class APIClient:
    """Handles API communication with retry logic"""
    
    def __init__(self, base_url: str, api_key: str, timeout: int = 30):
        self.base_url = base_url
        self.api_key = api_key
        self.session = self._create_session(timeout)

    def _create_session(self, timeout: int) -> requests.Session:
        """Creates requests session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "Authorization": f"ApiKey {self.api_key}",
            "Accept": "application/json"
        })
        session.timeout = timeout
        return session
    
    def get_data(self, relative_url: str, params: Optional[Dict] = None) -> Tuple[requests.Response, Optional[dict]]:
        """
        Fetches data from API with error handling
        
        Args:
            relative_url: API endpoint path
            params: Optional query parameters
            
        Returns:
            Tuple of (Response, JSON data)
        """
        try:
            request_url = f"{self.base_url}{relative_url}"
            response = self.session.get(request_url, params=params)
            response.raise_for_status()
            return response, response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            if hasattr(e, 'response'):
                return e.response, None
            return None, None
