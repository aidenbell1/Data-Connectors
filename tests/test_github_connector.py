"""
Tests for GitHub connector
"""

import pytest
from src.connectors.github_connector import GitHubConnector, GitHubConfig


def test_github_config_creation():
    """
    Test that we can create a GitHubConfig object
    
    YOUR TASK: 
    1. Create a GitHubConfig with base_url and api_key
    2. Assert that the config was created correctly
    3. Assert base_url is what you set
    """
    # YOUR CODE HERE
    pass


def test_github_connector_initialization():
    """
    Test that we can initialize the connector
    
    YOUR TASK:
    1. Create a GitHubConfig
    2. Create a GitHubConnector with that config
    3. Assert the connector was created
    4. Assert the connector has a session
    """
    # YOUR CODE HERE
    pass


def test_fetch_repos_returns_list():
    """
    Test that fetching repos returns a list
    
    YOUR TASK:
    1. Create connector
    2. Call extract() with a username (use "torvalds" - he has public repos)
    3. Assert result is a list
    4. Assert list is not empty
    5. Assert first item has 'name' field
    """
    # YOUR CODE HERE
    pass


# We'll add more tests as we go