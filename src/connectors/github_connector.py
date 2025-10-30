"""
GitHub API Connector

This connector fetches repository data from GitHub's public API.
"""

from typing import Dict, List, Any
from src.base_connector import BaseConnector, ConnectorConfig
import logging

logger = logging.getLogger(__name__)


class GitHubConfig(ConnectorConfig):
    """
    Configuration for GitHub connector
    
    YOUR TASK: What fields does this need?
    Hint: Inherits from ConnectorConfig, so it already has base_url, api_key, etc.
    Do we need any GitHub-specific config? Think about it.
    """
    pass  # Add any GitHub-specific config here if needed


class GitHubConnector(BaseConnector):
    """
    Connector for GitHub API
    
    Fetches repository information for users.
    """
    
    def __init__(self, config: GitHubConfig):
        """
        Initialize the GitHub connector
        
        YOUR TASK: Call the parent class __init__
        """
        # YOUR CODE HERE
        pass
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for GitHub API
        
        YOUR TASK: 
        GitHub uses 'Authorization: token YOUR_TOKEN' format
        If we have an api_key, return the proper header
        If not, return empty dict
        
        Hint: Check if self.config.api_key exists
        """
        # YOUR CODE HERE
        pass
    
    def extract(self, username: str, max_repos: int = 100) -> List[Dict[str, Any]]:
        """
        Extract repository data for a GitHub user
        
        Args:
            username: GitHub username
            max_repos: Maximum number of repos to fetch
        
        Returns:
            List of repository dictionaries
            
        YOUR TASK:
        1. Build the endpoint: f"users/{username}/repos"
        2. Use self.get() to fetch data (from base class)
        3. GitHub returns a list directly, no pagination needed for simple case
        4. Validate each repo using validate_response()
        5. Return the list
        
        ERROR HANDLING:
        - What if username doesn't exist?
        - What if API is down?
        - Wrap in try/except and log errors
        """
        # YOUR CODE HERE
        pass
    
    def validate_response(self, data: Any) -> bool:
        """
        Validate that a repository object has required fields
        
        YOUR TASK:
        GitHub repos should have these fields:
        - id (int)
        - name (str)
        - full_name (str)
        - html_url (str)
        
        Check if data is a dict and has these fields.
        Return True if valid, False otherwise.
        """
        # YOUR CODE HERE
        pass
    
    def extract_with_pagination(
        self, 
        username: str, 
        per_page: int = 30,
        max_pages: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Extract repos with pagination support
        
        YOUR TASK (BONUS - do this after basic extract() works):
        GitHub uses offset pagination with 'per_page' and 'page' params
        
        1. Use self.paginate_offset() from base class
        2. The endpoint is f"users/{username}/repos"
        3. Pass per_page as limit
        4. Collect all pages into a single list
        5. Return the combined list
        
        Hint: paginate_offset returns a generator, you need to loop over it
        """
        # YOUR CODE HERE (BONUS)
        pass