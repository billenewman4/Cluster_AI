"""
Reference Data Loader Module

Loads and parses reference data from Excel files for use in the extraction pipeline.
Handles loading primal cut data, synonyms, and grade information.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional, Set

import pandas as pd

logger = logging.getLogger(__name__)

class ReferenceDataLoader:
    """
    Loads and manages reference data for beef extraction from Excel spreadsheets.
    
    Provides access to primal cuts, their subprimals, synonyms, and grade mappings.
    """
    
    def __init__(self, data_path: str = "data/incoming/beef_cuts.xlsx"):
        """
        Initialize the reference data loader.
        
        Args:
            data_path: Path to the beef cuts reference Excel file
        """
        self.data_path = Path(data_path)
        self.primal_data: Dict[str, Dict[str, List[str]]] = {}
        self.grade_mappings: Dict[str, List[str]] = {}
        self._load_data()
        
    def _load_data(self) -> None:
        """
        Load data from the reference Excel file.
        
        Populates primal_data and grade_mappings dictionaries.
        """
        if not self.data_path.exists():
            logger.error(f"Reference data file not found: {self.data_path}")
            raise FileNotFoundError(f"Reference data file not found: {self.data_path}")
            
        try:
            # Load the Excel file
            excel_file = pd.ExcelFile(self.data_path)
            
            # Extract sheet names, ignoring the Grades sheet
            primal_sheets = [sheet for sheet in excel_file.sheet_names if sheet != 'Grades']
            
            # Load each primal cut sheet
            for sheet_name in primal_sheets:
                # Skip any non-beef sheets or special sheets
                if not sheet_name.startswith('Beef'):
                    continue
                    
                # Extract the primal name from the sheet name
                primal_name = sheet_name.replace('Beef ', '')
                
                # Load the sheet data
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Convert to dictionary of subprimal -> synonyms
                subprimal_dict = {}
                for _, row in df.iterrows():
                    subprimal = row['Sub-primal']
                    synonyms = []
                    
                    # Process synonyms if they exist
                    if pd.notna(row.get('Known Synonyms')):
                        # Split by comma and strip whitespace
                        synonyms = [s.strip() for s in str(row['Known Synonyms']).split(',')]
                    
                    subprimal_dict[subprimal] = synonyms
                
                # Add to primal data dictionary
                self.primal_data[primal_name] = subprimal_dict
            
            # Load grade mappings
            grades_df = pd.read_excel(excel_file, sheet_name='Grades')
            for _, row in grades_df.iterrows():
                official_grade = row['Official / Commercial Grade Name']
                if pd.notna(row.get('Common Synonyms & Acronyms')):
                    # Split by comma and strip whitespace
                    synonyms = [s.strip() for s in str(row['Common Synonyms & Acronyms']).split(',')]
                    self.grade_mappings[official_grade] = synonyms
                else:
                    self.grade_mappings[official_grade] = []
                    
            logger.info(f"Loaded reference data for {len(self.primal_data)} primal cuts")
            
        except Exception as e:
            logger.error(f"Error loading reference data: {str(e)}")
            raise
    
    def get_primals(self) -> List[str]:
        """
        Get list of all primal cuts.
        
        Returns:
            List of primal cut names
        """
        return list(self.primal_data.keys())
    
    def get_subprimals(self, primal: str) -> List[str]:
        """
        Get list of subprimal cuts for a given primal.
        
        Args:
            primal: The primal cut name
            
        Returns:
            List of subprimal cut names
        """
        if primal not in self.primal_data:
            logger.warning(f"Primal cut not found: {primal}")
            return []
        
        return list(self.primal_data[primal].keys())
    
    def get_subprimal_synonyms(self, primal: str, subprimal: str) -> List[str]:
        """
        Get synonyms for a specific subprimal cut.
        
        Args:
            primal: The primal cut name
            subprimal: The subprimal cut name
            
        Returns:
            List of synonyms for the subprimal
        """
        if primal not in self.primal_data or subprimal not in self.primal_data[primal]:
            logger.warning(f"Subprimal cut not found: {primal} - {subprimal}")
            return []
        
        return self.primal_data[primal][subprimal]
    
    def get_all_subprimal_terms(self, primal: str) -> Set[str]:
        """
        Get all possible terms (names and synonyms) for subprimals of a primal.
        
        Args:
            primal: The primal cut name
            
        Returns:
            Set of all terms for the subprimals
        """
        result = set()
        if primal not in self.primal_data:
            return result
            
        # Add all subprimal names
        for subprimal, synonyms in self.primal_data[primal].items():
            result.add(subprimal)
            # Add all synonyms
            result.update(synonyms)
            
        return result
    
    def get_grades(self) -> List[str]:
        """
        Get list of all official grade names.
        
        Returns:
            List of official grade names
        """
        return list(self.grade_mappings.keys())
    
    def get_grade_synonyms(self, grade: str) -> List[str]:
        """
        Get synonyms for a specific grade.
        
        Args:
            grade: The official grade name
            
        Returns:
            List of synonyms for the grade
        """
        if grade not in self.grade_mappings:
            logger.warning(f"Grade not found: {grade}")
            return []
        
        return self.grade_mappings[grade]
    
    def get_all_grade_terms(self) -> Set[str]:
        """
        Get all possible grade terms (official names and synonyms).
        
        Returns:
            Set of all grade terms
        """
        result = set()
        
        # Add all official grade names
        result.update(self.grade_mappings.keys())
        
        # Add all grade synonyms
        for synonyms in self.grade_mappings.values():
            result.update(synonyms)
            
        return result
