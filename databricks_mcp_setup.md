# Databricks MCP (Model Context Protocol) Setup Guide

This guide walks you through setting up MCP to connect to Databricks Unity Catalog functions, enabling AI agents to access your enterprise data and tools.

## Overview

Databricks provides managed MCP servers that allow agents to:
- **Query Unity Catalog functions** in specified schemas
- **Search vector indexes** for unstructured data
- **Query Genie spaces** for structured data insights
- **Access built-in AI tools** like Python code execution

## Prerequisites

1. **Databricks workspace** on Premium plan or above
2. **Unity Catalog enabled** in your workspace
3. **Serverless compute enabled** (required for built-in AI tools)
4. **Python 3.12+** for local development

## Step 1: Install Required Packages

```bash
pip install -U databricks-mcp "mcp>=1.9" "databricks-sdk[openai]" "mlflow>=3.1.0" "databricks-agents>=1.0.0"
```

## Step 2: Install and Configure Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Authenticate to your workspace
databricks auth login --host https://your-workspace-hostname

# Verify connection
databricks current-user me
```

## Step 3: Understand MCP Server URLs

Databricks provides these managed MCP servers:

### Unity Catalog Functions
```
https://<workspace-hostname>/api/2.0/mcp/functions/{catalog_name}/{schema_name}
```
- Exposes Unity Catalog functions (UDFs) in the specified schema
- Functions must be properly registered in Unity Catalog
- Permissions are enforced based on Unity Catalog access controls

### Vector Search
```
https://<workspace-hostname>/api/2.0/mcp/vector-search/{catalog_name}/{schema_name}
```
- Provides access to Vector Search indexes in the specified schema
- Useful for RAG (Retrieval Augmented Generation) applications

### Genie Spaces
```
https://<workspace-hostname>/api/2.0/mcp/genie/{genie_space_id}
```
- Connects to Genie spaces for natural language queries on structured data
- Enables agents to ask questions about tables and get insights

### Built-in AI Tools
```
https://<workspace-hostname>/api/2.0/mcp/functions/system/ai
```
- Provides built-in tools like `system.ai.python_exec` for code execution
- Requires serverless compute to be enabled

## Step 4: Test Your Connection

1. **Update the test script** (`mcp_test.py`):
   ```python
   WORKSPACE_HOSTNAME = "your-actual-workspace-hostname.cloud.databricks.com"
   DATABRICKS_CLI_PROFILE = "DEFAULT"  # or your profile name
   ```

2. **Run the test**:
   ```bash
   python mcp_test.py
   ```

3. **Expected output**:
   ```
   🚀 Databricks MCP Connection Test
   ✅ Connected as: your.email@company.com
   ✅ Workspace: https://your-workspace-hostname.cloud.databricks.com
   📡 Testing MCP server: https://your-workspace-hostname.cloud.databricks.com/api/2.0/mcp/functions/system/ai
   ✅ MCP session initialized
   🛠️  Found 1 tools:
      • system__ai__python_exec: Execute Python code in a secure environment
   ```

## Step 5: Create Unity Catalog Functions (Optional)

To expose your own functions via MCP, create them in Unity Catalog:

```sql
-- Example: Create a simple data retrieval function
CREATE FUNCTION main.default.get_customer_info(customer_id INT)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    # Your function logic here
    return f"Customer info for ID: {customer_id}"
$$;

-- Grant permissions
GRANT EXECUTE ON FUNCTION main.default.get_customer_info TO `data_team`;
```

Then access via MCP:
```
https://your-workspace-hostname/api/2.0/mcp/functions/main/default
```

## Step 6: Build a Simple Agent

Here's a minimal agent that uses MCP:

```python
import asyncio
from databricks_mcp import DatabricksOAuthClientProvider
from databricks.sdk import WorkspaceClient
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

async def simple_agent_query(question: str):
    """Simple agent that can use Databricks tools via MCP."""
    
    workspace_client = WorkspaceClient(profile="DEFAULT")
    mcp_server_url = f"{workspace_client.config.host}/api/2.0/mcp/functions/system/ai"
    
    async with streamablehttp_client(
        mcp_server_url, 
        auth=DatabricksOAuthClientProvider(workspace_client)
    ) as (read_stream, write_stream, _), ClientSession(
        read_stream, write_stream
    ) as session:
        
        await session.initialize()
        
        # List available tools
        tools = await session.list_tools()
        print(f"Available tools: {[t.name for t in tools.tools]}")
        
        # Example: Execute Python code
        if question.startswith("calculate:"):
            code = question.replace("calculate:", "").strip()
            result = await session.call_tool(
                "system__ai__python_exec", 
                {"code": f"result = {code}\nprint(f'Result: {{result}}')"}
            )
            return result.content
        
        return "I can help with calculations using Python. Try: calculate: 2 + 2"

# Usage
if __name__ == "__main__":
    result = asyncio.run(simple_agent_query("calculate: 15 * 7"))
    print(result)
```

## Step 7: Deploy Production Agents

For production deployment, use the standard Databricks agent deployment process:

```python
import mlflow
from databricks import agents
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint

# Specify all resources your agent needs
resources = [
    DatabricksServingEndpoint(endpoint_name="your-llm-endpoint"),
    DatabricksFunction("system.ai.python_exec"),
    # Add your custom functions:
    # DatabricksFunction("main.default.your_custom_function"),
]

# Log and deploy
with mlflow.start_run():
    logged_model = mlflow.pyfunc.log_model(
        artifact_path="mcp_agent",
        python_model="your_agent.py",
        resources=resources,
    )

# Register and deploy
registered_model = mlflow.register_model(logged_model.model_uri, "main.default.your_mcp_agent")
agents.deploy(model_name="main.default.your_mcp_agent", model_version=registered_model.version)
```

## Common Use Cases

### 1. Data Analysis Agent
```python
# Agent that can query tables and perform analysis
MCP_SERVERS = [
    f"{host}/api/2.0/mcp/functions/system/ai",  # Python execution
    f"{host}/api/2.0/mcp/genie/{genie_space_id}",  # Data queries
]
```

### 2. RAG Application
```python
# Agent with vector search capabilities
MCP_SERVERS = [
    f"{host}/api/2.0/mcp/vector-search/prod/documents",  # Document search
    f"{host}/api/2.0/mcp/functions/prod/processing",  # Custom processing functions
]
```

### 3. Business Intelligence Agent
```python
# Agent for business insights
MCP_SERVERS = [
    f"{host}/api/2.0/mcp/genie/{sales_genie_space}",  # Sales data
    f"{host}/api/2.0/mcp/genie/{finance_genie_space}",  # Finance data
    f"{host}/api/2.0/mcp/functions/analytics/reports",  # Custom report functions
]
```

## Security and Permissions

- **Unity Catalog permissions are enforced** - agents can only access functions/data they have permission to
- **OAuth authentication** ensures secure access to your workspace
- **Audit logging** tracks all function calls and data access
- **Row-level security** and **column masking** are respected

## Troubleshooting

### Common Issues

1. **"MCP connection failed"**
   - Verify workspace hostname is correct
   - Check authentication: `databricks current-user me`
   - Ensure serverless compute is enabled

2. **"No tools found"**
   - Check if the schema exists and has functions
   - Verify you have EXECUTE permissions on functions
   - For system.ai, ensure serverless compute is enabled

3. **"Tool call failed"**
   - Check function parameters match expected schema
   - Verify data permissions for functions that access tables
   - Review Unity Catalog function logs

### Debug Commands

```bash
# Check authentication
databricks current-user me

# List available functions in a schema
databricks sql exec "SHOW FUNCTIONS IN main.default"

# Test function directly
databricks sql exec "SELECT main.default.your_function('test')"
```

## Next Steps

1. **Create custom Unity Catalog functions** for your specific use cases
2. **Set up vector search indexes** for unstructured data
3. **Configure Genie spaces** for natural language data queries
4. **Deploy production agents** using the MLflow workflow
5. **Monitor usage** through Databricks audit logs

## Resources

- [Databricks MCP Documentation](https://docs.databricks.com/en/ai-and-machine-learning/build-gen-ai-apps/mcp.html)
- [Unity Catalog Functions Guide](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions.html)
- [Vector Search Setup](https://docs.databricks.com/en/generative-ai/vector-search.html)
- [Genie Spaces Documentation](https://docs.databricks.com/en/genie/index.html)

---

**Questions or issues?** Check the troubleshooting section above or consult the Databricks documentation. 