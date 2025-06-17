#!/usr/bin/env python3
"""
Wrapper script to run batch processing while preventing computer sleep.
Uses macOS 'caffeinate' command to keep system awake.
"""

import os
import sys
import subprocess
import signal
import argparse
from pathlib import Path

def run_with_caffeinate(command_args):
    """Run a command while preventing system sleep."""
    
    # Build the caffeinate command
    # -d: prevent display sleep
    # -i: prevent idle sleep
    # -m: prevent disk idle sleep
    # -s: prevent system sleep
    caffeinate_cmd = ['caffeinate', '-dims']
    
    # Add the Python command
    python_cmd = ['python3', 'run_fast_batch.py'] + command_args
    full_cmd = caffeinate_cmd + python_cmd
    
    print("🚀 Starting batch processing with sleep prevention...")
    print(f"Command: {' '.join(full_cmd)}")
    print("⚠️  Your computer will NOT sleep during this process")
    print("💡 Press Ctrl+C to stop and save progress to checkpoint")
    print()
    
    try:
        # Run the command
        result = subprocess.run(full_cmd, check=False)
        return result.returncode
        
    except KeyboardInterrupt:
        print("\n⏸️  Process interrupted by user")
        print("💾 Progress should be saved in checkpoint")
        return 130  # Standard exit code for SIGINT

def main():
    """Main function to handle arguments and run caffeinate wrapper."""
    
    # Check if we're on macOS
    if sys.platform != 'darwin':
        print("❌ This script is designed for macOS only")
        print("💡 On other systems, manually prevent sleep or use tools like:")
        print("   - Windows: powercfg -change -standby-timeout-ac 0")
        print("   - Linux: systemctl mask sleep.target suspend.target")
        sys.exit(1)
    
    # Check if caffeinate is available
    try:
        subprocess.run(['which', 'caffeinate'], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("❌ 'caffeinate' command not found")
        print("💡 This should be available on macOS by default")
        sys.exit(1)
    
    # Parse arguments (pass through to the actual script)
    parser = argparse.ArgumentParser(
        description='Run batch processing with sleep prevention',
        add_help=False  # Don't show help for this wrapper
    )
    
    # Capture all arguments to pass through
    args, unknown = parser.parse_known_args()
    command_args = sys.argv[1:]  # Get all arguments after script name
    
    # Show usage examples
    if not command_args or '--help' in command_args or '-h' in command_args:
        print("🔋 Batch Processing with Sleep Prevention")
        print()
        print("Usage examples:")
        print("  python3 run_with_caffeinate.py --test-run")
        print("  python3 run_with_caffeinate.py --upload-firebase")
        print("  python3 run_with_caffeinate.py --test-run --upload-firebase")
        print("  python3 run_with_caffeinate.py --resume")
        print()
        print("This wrapper prevents your computer from sleeping during processing.")
        print("Progress is automatically saved and can be resumed if interrupted.")
        return 0
    
    # Run the batch processing with sleep prevention
    return run_with_caffeinate(command_args)

if __name__ == "__main__":
    sys.exit(main()) 