"""
File Utilities Module
Provides efficient file operations and metadata extraction.
"""

import os
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Union, List, Optional


def ensure_directory(directory_path: Union[str, Path]) -> Path:
    """Ensure directory exists, creating it if necessary.
    
    Args:
        directory_path: Path to the directory
        
    Returns:
        Path: Path object for the directory
    """
    directory_path = Path(directory_path) if isinstance(directory_path, str) else directory_path
    
    # Create directory if it doesn't exist
    directory_path.mkdir(parents=True, exist_ok=True)
    
    return directory_path

def find_newest_file(directory: Union[str, Path], pattern: str = "*") -> Optional[Path]:
    """Find the newest file in a directory matching a pattern.
    
    Args:
        directory: Directory to search
        pattern: Glob pattern to match filenames
        
    Returns:
        Optional[Path]: Path to newest file or None if no files found
    """
    directory = Path(directory) if isinstance(directory, str) else directory
    
    if not directory.exists() or not directory.is_dir():
        return None
    
    # Use a single glob operation and sort by modification time
    matching_files = list(directory.glob(pattern))
    
    if not matching_files:
        return None
    
    # Sort by modification time (most recent first)
    return max(matching_files, key=lambda p: p.stat().st_mtime)

def batch_file_operations(file_paths: List[Union[str, Path]], 
                           operation_fn: callable, 
                           batch_size: int = 50) -> List:
    """Process files in batches to optimize memory usage.
    
    Args:
        file_paths: List of paths to process
        operation_fn: Function to apply to each file
        batch_size: Number of files to process in each batch
        
    Returns:
        List: Results from operation_fn for each file
    """
    results = []
    
    # Convert all paths to Path objects once
    paths = [Path(p) if isinstance(p, str) else p for p in file_paths]
    
    # Process in batches
    for i in range(0, len(paths), batch_size):
        batch = paths[i:i+batch_size]
        batch_results = [operation_fn(file) for file in batch]
        results.extend(batch_results)
    
    return results
