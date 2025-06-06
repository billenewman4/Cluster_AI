#!/bin/bash
# setup-node20.sh - Configure system to use Node.js v20 from Homebrew
# Created: 2025-06-05

# ANSI color codes for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}Setting up Node.js v20 environment...${NC}"

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo -e "${RED}Homebrew not found! Please install Homebrew first.${NC}"
    exit 1
fi

# Check if Node.js v20 is installed via Homebrew
if [ ! -d "/opt/homebrew/opt/node@20" ] && [ ! -d "/usr/local/opt/node@20" ]; then
    echo -e "${YELLOW}Node.js v20 not found in Homebrew installation.${NC}"
    echo "Installing Node.js v20 with Homebrew..."
    brew install node@20
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to install Node.js v20. Aborting.${NC}"
        exit 1
    fi
fi

# Detect the correct Homebrew path (supports both Intel and Apple Silicon Macs)
if [ -d "/opt/homebrew/opt/node@20" ]; then
    NODE_PATH="/opt/homebrew/opt/node@20"
else
    NODE_PATH="/usr/local/opt/node@20"
fi

# Create a local bin directory in the project for Node.js v20 binaries
NODE_BIN_DIR="$(pwd)/.node_bin"
mkdir -p "$NODE_BIN_DIR"

# Remove any existing symlinks to avoid errors
rm -f "$NODE_BIN_DIR/node" "$NODE_BIN_DIR/npm" "$NODE_BIN_DIR/npx"

# Create new symlinks
ln -sf "$NODE_PATH/bin/node" "$NODE_BIN_DIR/node"
ln -sf "$NODE_PATH/bin/npm" "$NODE_BIN_DIR/npm"
ln -sf "$NODE_PATH/bin/npx" "$NODE_BIN_DIR/npx"

# Add the project's .node_bin to PATH for this session
export PATH="$NODE_BIN_DIR:$PATH"

# Verify Node.js version
NODE_VERSION=$(node -v)
if [[ ! "$NODE_VERSION" =~ ^v20 ]]; then
    echo -e "${RED}Error: Node.js v20 is not active. Found $NODE_VERSION${NC}"
    echo "Please check your PATH configuration."
    exit 1
fi

echo -e "${GREEN}Using Node.js version: ${NODE_VERSION}${NC}"

# Install Firebase Tools globally with this Node.js version
echo "Installing Firebase Tools..."
"$NODE_BIN_DIR/npm" install -g firebase-tools

# Verify Firebase Tools version
FIREBASE_VERSION=$("$NODE_BIN_DIR/node" "$(npm root -g)/firebase-tools/lib/bin/firebase.js" --version)
echo -e "${GREEN}Firebase Tools version: ${FIREBASE_VERSION}${NC}"

# Create an optimized helper script to run Firebase with Node.js v20
cat > "$(pwd)/firebase-mcp" << EOF
#!/bin/bash
# Helper script to run Firebase with Node.js v20
# Created $(date +"%Y-%m-%d")

# Source path with reliable directory detection for symlink resilience  
SCRIPT_DIR="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
export PATH="\$SCRIPT_DIR/.node_bin:\$PATH"

# Execute firebase with all arguments passed through
"\$SCRIPT_DIR/.node_bin/node" "\$(npm root -g)/firebase-tools/lib/bin/firebase.js" "\$@"
EOF

# Make it executable
chmod +x "$(pwd)/firebase-mcp"

echo -e "${GREEN}Setup complete!${NC}"
echo -e "${BLUE}To use Firebase MCP, run: ./firebase-mcp experimental:mcp${NC}"

# Log the task completion
mkdir -p .cline
cat > .cline/task-log_05-06-25-17-55.log << EOF
GOAL: Configure isolated Node.js v20 environment for Firebase MCP
IMPLEMENTATION: Created localized Node.js v20 binaries and helper scripts with proper path resolution, error handling, and cross-platform compatibility. The solution isolates the Node.js v20 environment to avoid conflicts with system Node.js installations.
COMPLETED: $(date +"%d-%m-%y-%H-%M")
EOF

# Add a reminder about project-specific configuration
echo -e "${YELLOW}IMPORTANT:${NC} This script has created a project-specific Node.js v20 environment."
echo "Future Firebase CLI commands should be run using the ./firebase-mcp script."
