"""
Data Validation Module
Provides functions for validating data schema and detecting anomalies.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Union, Set
import logging

logger = logging.getLogger(__name__)

def validate_dataframe_schema(
    df: pd.DataFrame,
    required_columns: List[str],
    column_types: Optional[Dict[str, type]] = None
) -> Tuple[bool, List[str]]:
    """Validate DataFrame schema against requirements efficiently.
    
    Args:
        df: DataFrame to validate
        required_columns: List of columns that must be present
        column_types: Dict mapping column names to expected types
        
    Returns:
        Tuple[bool, List[str]]: (is_valid, list of validation errors)
    """
    # List to collect validation errors
    errors = []
    
    # Check for missing columns - O(n) operation
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")
    
    # Check column types if specified
    if column_types:
        for col, expected_type in column_types.items():
            if col in df.columns:
                # Use pandas built-in type checking for efficiency
                if expected_type == str:
                    if not pd.api.types.is_string_dtype(df[col]):
                        errors.append(f"Column '{col}' should be string type")
                elif expected_type == int:
                    if not pd.api.types.is_integer_dtype(df[col]):
                        errors.append(f"Column '{col}' should be integer type")
                elif expected_type == float:
                    if not pd.api.types.is_float_dtype(df[col]):
                        errors.append(f"Column '{col}' should be float type")
                elif expected_type == bool:
                    if not pd.api.types.is_bool_dtype(df[col]):
                        errors.append(f"Column '{col}' should be boolean type")
    
    return len(errors) == 0, errors

def detect_anomalies(
    df: pd.DataFrame,
    numeric_columns: Optional[List[str]] = None,
    categorical_columns: Optional[List[str]] = None,
    threshold: float = 3.0
) -> Dict[str, List]:
    """Detect anomalies in DataFrame columns using statistical methods.
    
    Args:
        df: DataFrame to analyze
        numeric_columns: List of numeric columns to check
        categorical_columns: List of categorical columns to check  
        threshold: Z-score threshold for numerical outliers
        
    Returns:
        Dict[str, List]: Dictionary of anomalies by column
    """
    anomalies = {}
    
    # Auto-detect column types if not provided
    if numeric_columns is None:
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    
    if categorical_columns is None:
        categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
    
    # Check numeric columns for outliers using z-score
    for col in numeric_columns:
        if col in df.columns:
            # Skip if all values are the same
            if df[col].nunique() <= 1:
                continue
                
            # Use vectorized operations - much faster than iterating
            mean = df[col].mean()
            std = df[col].std()
            
            if std == 0:  # Avoid division by zero
                continue
                
            # Calculate z-scores using vectorized operations
            z_scores = np.abs((df[col] - mean) / std)
            
            # Find outliers
            outlier_indices = np.where(z_scores > threshold)[0]
            if len(outlier_indices) > 0:
                anomalies[col] = outlier_indices.tolist()
    
    # Check categorical columns for rare values
    for col in categorical_columns:
        if col in df.columns:
            # Calculate value counts and frequencies
            value_counts = df[col].value_counts(normalize=True)
            
            # Identify rare categories (less than 1%)
            rare_values = value_counts[value_counts < 0.01].index.tolist()
            
            if rare_values:
                # Find indices with rare values using efficient boolean indexing
                rare_indices = df.index[df[col].isin(rare_values)].tolist()
                if rare_indices:
                    anomalies[col] = rare_indices
    
    return anomalies

def validate_consistency(df: pd.DataFrame, rules: Dict[str, callable]) -> Dict[str, List[int]]:
    """Validate data consistency using custom rules.
    
    Args:
        df: DataFrame to validate
        rules: Dict mapping rule names to validation functions
        
    Returns:
        Dict[str, List[int]]: Failed row indices by rule name
    """
    violations = {}
    
    for rule_name, validation_fn in rules.items():
        try:
            # Apply validation function and get boolean mask of violations
            mask = ~validation_fn(df)
            
            # Get indices of rows that violated the rule
            if mask.any():
                violations[rule_name] = df.index[mask].tolist()
        except Exception as e:
            logger.error(f"Error applying rule '{rule_name}': {str(e)}")
            violations[f"{rule_name}_error"] = str(e)
    
    return violations
