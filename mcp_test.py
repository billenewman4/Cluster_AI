#!/usr/bin/env python3
"""
Test script for MCP connection to Databricks Unity Catalog functions.
Based on the Databricks documentation for MCP setup.
"""

import asyncio
import os
from mcp.client.streamable_http import streamablehttp_client
from mcp.client.session import ClientSession
from databricks_mcp import DatabricksOAuthClientProvider
from databricks.sdk import WorkspaceClient

# Configuration - UPDATE THESE VALUES
WORKSPACE_HOSTNAME = "adb-3654027577608521.1.azuredatabricks.net"  # e.g., "dbc-12345678-1234.cloud.databricks.com"
DATABRICKS_CLI_PROFILE = "adb-3654027577608521"  # Your Databricks CLI profile name

# MCP server URLs for Unity Catalog functions
# Format: https://<workspace-hostname>/api/2.0/mcp/functions/{catalog_name}/{schema_name}
MCP_SERVER_URLS = [
    f"https://{WORKSPACE_HOSTNAME}/api/2.0/mcp/functions/system/ai",  # Built-in AI tools
    # Add your custom Unity Catalog function schemas here, e.g.:
    # f"https://{WORKSPACE_HOSTNAME}/api/2.0/mcp/functions/main/default",
]

async def test_mcp_connection():
    """Test connection to Databricks MCP server and list available tools."""
    
    print("🔗 Testing MCP connection to Databricks Unity Catalog functions...")
    
    try:
        # Initialize Databricks workspace client
        workspace_client = WorkspaceClient(profile=DATABRICKS_CLI_PROFILE)
        print(f"✅ Connected to workspace: {workspace_client.config.host}")
        
        # Test each MCP server URL
        for server_url in MCP_SERVER_URLS:
            print(f"\n📡 Testing MCP server: {server_url}")
            
            async with streamablehttp_client(
                server_url, 
                auth=DatabricksOAuthClientProvider(workspace_client)
            ) as (read_stream, write_stream, _), ClientSession(
                read_stream, write_stream
            ) as session:
                
                # Initialize session
                await session.initialize()
                print("✅ MCP session initialized")
                
                # List available tools
                tools = await session.list_tools()
                print(f"🛠️  Found {len(tools.tools)} tools:")
                
                for tool in tools.tools:
                    print(f"   • {tool.name}: {tool.description}")
                
                # Test calling a simple tool (if available)
                if tools.tools:
                    first_tool = tools.tools[0]
                    print(f"\n🧪 Testing tool: {first_tool.name}")
                    
                    try:
                        # Try calling the first tool with minimal parameters
                        if first_tool.name == "system__ai__python_exec":
                            result = await session.call_tool(
                                "system__ai__python_exec", 
                                {"code": "print('Hello from MCP!')"}
                            )
                            print(f"✅ Tool result: {result.content}")
                        else:
                            print(f"ℹ️  Skipping test call for {first_tool.name} (requires specific parameters)")
                    
                    except Exception as e:
                        print(f"⚠️  Tool test failed: {e}")
                
    except Exception as e:
        print(f"❌ MCP connection failed: {e}")
        print("\n🔧 Troubleshooting tips:")
        print("1. Make sure you've authenticated with: databricks auth login --host https://your-workspace-hostname")
        print("2. Verify your workspace hostname is correct")
        print("3. Check that serverless compute is enabled in your workspace")
        print("4. Ensure you have access to Unity Catalog functions")

async def test_basic_databricks_connection():
    """Test basic Databricks SDK connection."""
    
    print("🔗 Testing basic Databricks connection...")
    
    try:
        workspace_client = WorkspaceClient(profile=DATABRICKS_CLI_PROFILE)
        
        # Get current user info
        current_user = workspace_client.current_user.me()
        print(f"✅ Connected as: {current_user.user_name}")
        print(f"✅ Workspace: {workspace_client.config.host}")
        
        return True
        
    except Exception as e:
        print(f"❌ Databricks connection failed: {e}")
        print("\n🔧 Setup instructions:")
        print("1. Install Databricks CLI: pip install databricks-cli")
        print("2. Authenticate: databricks auth login --host https://your-workspace-hostname")
        print("3. Update WORKSPACE_HOSTNAME and DATABRICKS_CLI_PROFILE in this script")
        
        return False

if __name__ == "__main__":
    print("🚀 Databricks MCP Connection Test")
    print("=" * 50)
    
    # Update configuration check
    if WORKSPACE_HOSTNAME == "YOUR_WORKSPACE_HOSTNAME":
        print("❌ Please update WORKSPACE_HOSTNAME in the script")
        print("   Set it to your Databricks workspace hostname (without https://)")
        exit(1)
    
    # Test basic connection first
    print("\n1️⃣ Testing basic Databricks connection...")
    basic_connection_ok = asyncio.run(test_basic_databricks_connection())
    
    if basic_connection_ok:
        print("\n2️⃣ Testing MCP connection...")
        asyncio.run(test_mcp_connection())
    else:
        print("\n⏭️  Skipping MCP test due to basic connection failure")
    
    print("\n�� Test complete!") 