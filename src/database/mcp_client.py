"""
Firebase MCP Client

A minimal client for connecting to Firebase MCP server.
This is optimized for use with AI assistants like Cascade.
"""

import os
import sys
from pathlib import Path
import logging
import json

# For MCP client
from openai_agents import Agent
from mcp import MCPServerStdio

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FirebaseMCPAIClient:
    """Lightweight client for Firebase MCP interactions through AI assistants."""
    
    def __init__(self, firebase_script_path=None):
        """
        Initialize the Firebase MCP client for AI assistant integration.
        
        Args:
            firebase_script_path: Path to firebase-mcp script
        """
        # Set up paths
        project_root = Path(__file__).resolve().parent.parent.parent
        self.firebase_script = firebase_script_path or project_root / "firebase-mcp"
        
        # Initialize MCP components
        self.mcp_server = None
        self.agent = None
        
        # Validate environment
        if not os.path.exists(self.firebase_script):
            logger.error(f"Firebase MCP script not found at: {self.firebase_script}")
            raise FileNotFoundError(f"Firebase MCP script not found at: {self.firebase_script}")
            
        if "OPENAI_API_KEY" not in os.environ:
            logger.error("OPENAI_API_KEY environment variable not set")
            raise EnvironmentError("OPENAI_API_KEY environment variable not set")
    
    def start(self):
        """Start the Firebase MCP server and connect an agent to it."""
        try:
            logger.info("Starting Firebase MCP server")
            
            # Initialize MCP server
            self.mcp_server = MCPServerStdio(
                params={
                    "command": str(self.firebase_script),
                    "args": [
                        "experimental:mcp",
                        "--only", "firestore"  # Add more services as needed
                    ]
                }
            )
            
            # Create agent
            self.agent = Agent(
                name="FirebaseAssistant",
                instructions="Use Firebase tools to handle database operations for beef cut extraction data.",
                mcp_servers=[self.mcp_server],
                debug=True
            )
            
            logger.info("Firebase MCP client started successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start Firebase MCP client: {e}")
            return False
    
    def stop(self):
        """Stop the MCP server and clean up resources."""
        if self.mcp_server:
            try:
                self.mcp_server.stop()
                logger.info("Firebase MCP server stopped")
            except Exception as e:
                logger.error(f"Error stopping Firebase MCP server: {e}")


if __name__ == "__main__":
    # Simple CLI for starting the client
    client = FirebaseMCPAIClient()
    
    if client.start():
        print("\n" + "=" * 60)
        print("  Firebase MCP Client Running - Press Ctrl+C to stop")
        print("=" * 60)
        
        try:
            # Keep the script running until interrupted
            while True:
                pass
        except KeyboardInterrupt:
            print("\nStopping MCP client...")
        finally:
            client.stop()
            print("Client stopped. Goodbye!")
    else:
        sys.exit(1)
