import logging
import requests
import time

class API_Client:

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.logger = logging.getLogger('API Client')

    def get_data(self, relative_url, max_retries=3, backoff_factor=2):
        for attempt in range(max_retries):
            try:
                request_url = f"{self.base_url}{relative_url}"
                response = requests.request("GET", request_url, headers={"Authorization": f"ApiKey {self.api_key}"})
                self.logger.info(f"Request for {relative_url} successful")
                return response, response.json()
            except requests.exceptions.SSLError as e:
                self.logger.error(f"SSL-Zertifikatsfehler: {str(e)}")
                return None, None
            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    wait_time = backoff_factor ** attempt
                    self.logger.warning(f"Timeout bei API-Anfrage: {str(e)}. Retry {attempt+1}/{max_retries} nach {wait_time}s")
                    time.sleep(wait_time)
                    continue
                self.logger.error(f"Timeout bei API-Anfrage nach {max_retries} Versuchen: {str(e)}")
                return None, None
            except requests.exceptions.ConnectionError as e:
                if attempt < max_retries - 1:
                    wait_time = backoff_factor ** attempt
                    self.logger.warning(f"Verbindungsfehler: {str(e)}. Retry {attempt+1}/{max_retries} nach {wait_time}s")
                    time.sleep(wait_time)
                    continue
                self.logger.error(f"Verbindungsfehler nach {max_retries} Versuchen: {str(e)}")
                return None, None
            except requests.exceptions.RequestException as e:
                self.logger.error(f"API request failed: {str(e)}")
                if hasattr(e, 'response'):
                    return e.response, None
                return None, None