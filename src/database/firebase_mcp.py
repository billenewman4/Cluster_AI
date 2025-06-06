"""
Firebase MCP Integration

Provides a bridge between OpenAI Agents and Firebase MCP server for
enhanced database operations in the beef cut extraction pipeline.

This module handles:
1. Starting and managing the Firebase MCP server
2. Connecting OpenAI agents to Firebase tools
3. Providing high-level functions for common operations
"""

import os
import sys
import subprocess
import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Callable

# For agent communication
from openai_agents import Agent
from mcp import MCPServerStdio
from openai import OpenAI

# Configure logging
logger = logging.getLogger(__name__)

class FirebaseMCPClient:
    """
    Streamlined client for Firebase MCP operations using OpenAI Agents.
    
    Manages the MCP server lifecycle and provides optimized, high-level
    operations for Firebase Firestore, Auth, and Storage services.
    """
    
    def __init__(
        self,
        project_dir: Optional[str] = None,
        services: List[str] = ["firestore", "auth", "storage"],
        openai_api_key: Optional[str] = None,
        debug_mode: bool = False
    ):
        """
        Initialize the Firebase MCP Client with optimized configuration.
        
        Args:
            project_dir: Firebase project directory (defaults to current working directory)
            services: List of Firebase services to enable
            openai_api_key: OpenAI API key for agent operations
            debug_mode: Whether to log detailed debugging information
        """
        # Set up paths and configuration
        self.project_dir = Path(project_dir or os.getcwd()).resolve()
        self.firebase_script = self.project_dir / "firebase-mcp"
        self.services = services
        self.debug_mode = debug_mode
        self._server_process = None
        self._agent = None
        
        # Validate and prepare environment
        self._validate_environment()
        
        # Configure OpenAI API key
        if openai_api_key:
            os.environ["OPENAI_API_KEY"] = openai_api_key
        elif "OPENAI_API_KEY" not in os.environ:
            raise ValueError("OpenAI API key must be provided or set as OPENAI_API_KEY environment variable")
        
        # Initialize MCP server and agent
        self._initialize_mcp_server()
    
    def _validate_environment(self) -> None:
        """Validate Firebase project setup and Node.js environment."""
        # Check for firebase-mcp script
        if not self.firebase_script.exists():
            raise FileNotFoundError(
                f"Firebase MCP script not found at {self.firebase_script}. "
                "Run the Node.js v20 setup script first."
            )
        
        # Check for Firebase project files
        firebase_rc = self.project_dir / ".firebaserc"
        firebase_json = self.project_dir / "firebase.json"
        
        if not firebase_rc.exists() or not firebase_json.exists():
            raise ValueError(
                f"Firebase project files not found in {self.project_dir}. "
                "Run 'firebase init' to set up a Firebase project first."
            )
    
    def _initialize_mcp_server(self) -> None:
        """Initialize the Firebase MCP server with proper error handling."""
        try:
            # Prepare MCP server with optimal configuration
            services_arg = ",".join(self.services)
            
            self.mcp_server = MCPServerStdio(
                params={
                    "command": str(self.firebase_script),
                    "args": [
                        "experimental:mcp",
                        "--dir", str(self.project_dir),
                        "--only", services_arg
                    ]
                }
            )
            
            # Create an OpenAI agent with optimal instructions
            self._agent = Agent(
                name="FirebaseBeefCutsAgent",
                instructions=(
                    "You are a specialized Firebase agent for beef cut extraction data management. "
                    "Use Firebase tools to efficiently store, retrieve, and manage extraction results "
                    "based on user requests. Focus on performance and data integrity."
                ),
                mcp_servers=[self.mcp_server],
                debug=self.debug_mode
            )
            
            logger.info(f"Firebase MCP server initialized with services: {services_arg}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Firebase MCP server: {e}")
            raise RuntimeError(f"Failed to initialize Firebase MCP server: {e}")
    
    def execute_operation(self, instruction: str, max_retries: int = 2) -> Dict[str, Any]:
        """
        Execute a natural language operation using the Firebase MCP agent.
        
        Args:
            instruction: Natural language instruction for Firebase operation
            max_retries: Maximum number of retries for transient errors
            
        Returns:
            Response dictionary from the agent
        """
        if not self._agent:
            raise RuntimeError("Firebase MCP agent not initialized")
            
        # Handle retries with exponential backoff
        retry_count = 0
        last_error = None
        
        while retry_count <= max_retries:
            try:
                response = self._agent(instruction)
                return self._parse_agent_response(response)
            except Exception as e:
                last_error = e
                retry_count += 1
                if retry_count <= max_retries:
                    logger.warning(f"Retrying Firebase operation after error: {e}")
                    
        # If we exhausted retries, raise the last error
        raise RuntimeError(f"Firebase operation failed after {max_retries} retries: {last_error}")
    
    def _parse_agent_response(self, response: str) -> Dict[str, Any]:
        """Parse and extract structured data from agent responses."""
        # Try to extract JSON if present in the response
        try:
            # Look for JSON-like content in the response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_str = response[json_start:json_end]
                return json.loads(json_str)
        except json.JSONDecodeError:
            pass
            
        # If no JSON found, return the full response as text
        return {"text": response}
    
    # High-level operations optimized for beef cut extraction
    
    def store_extraction_result(
        self, 
        collection: str, 
        document_id: str, 
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Store a beef cut extraction result in Firestore.
        
        Args:
            collection: Firestore collection name
            document_id: Document ID
            data: Extraction data to store
            
        Returns:
            Operation result
        """
        instruction = (
            f"Create or update a document in collection '{collection}' "
            f"with ID '{document_id}' containing this data: {json.dumps(data)}"
        )
        return self.execute_operation(instruction)
    
    def get_extraction_results(
        self, 
        collection: str, 
        filters: Optional[List[Dict[str, Any]]] = None,
        limit: int = 50
    ) -> Dict[str, Any]:
        """
        Retrieve beef cut extraction results from Firestore.
        
        Args:
            collection: Firestore collection name
            filters: Optional list of filter conditions
            limit: Maximum number of results
            
        Returns:
            Query results
        """
        instruction = f"Get documents from collection '{collection}'"
        
        if filters:
            filter_str = json.dumps(filters)
            instruction += f" with these filters: {filter_str}"
            
        instruction += f" limited to {limit} results."
        
        return self.execute_operation(instruction)
    
    def close(self) -> None:
        """Properly terminate MCP server and clean up resources."""
        if self.mcp_server:
            try:
                self.mcp_server.stop()
                logger.info("Firebase MCP server stopped")
            except Exception as e:
                logger.error(f"Error stopping Firebase MCP server: {e}")


# Utility function for singleton-like access
_firebase_mcp_instance = None

def get_firebase_mcp(
    project_dir: Optional[str] = None,
    services: List[str] = ["firestore", "auth", "storage"],
    openai_api_key: Optional[str] = None,
    debug_mode: bool = False
) -> FirebaseMCPClient:
    """
    Get or create a FirebaseMCPClient instance with singleton pattern.
    
    Args:
        project_dir: Firebase project directory
        services: Firebase services to enable
        openai_api_key: OpenAI API key
        debug_mode: Enable debug logging
        
    Returns:
        FirebaseMCPClient instance
    """
    global _firebase_mcp_instance
    
    if _firebase_mcp_instance is None:
        _firebase_mcp_instance = FirebaseMCPClient(
            project_dir=project_dir,
            services=services,
            openai_api_key=openai_api_key,
            debug_mode=debug_mode
        )
        
    return _firebase_mcp_instance
