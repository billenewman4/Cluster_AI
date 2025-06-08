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

from ..core.reader import FileReader

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
        self.file_reader = FileReader()
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
            # Use our centralized FileReader to read all sheets
            # First, define a filter function for beef-related sheets
            def sheet_filter(sheet_name: str) -> bool:
                return sheet_name.startswith('Beef') or sheet_name == 'Grades'
            
            # Load all relevant sheets using the FileReader
            sheet_data = self.file_reader.read_excel_sheets(self.data_path, sheet_filter=sheet_filter)
            logger.info(f"Loaded {len(sheet_data)} reference sheets from {self.data_path.name}")
            
            # Process each sheet (except Grades)
            for sheet_name, df in sheet_data.items():
                # Skip the Grades sheet for now (processed separately below)
                if sheet_name == 'Grades':
                    continue
                
                # Extract the primal name from the sheet name
                primal_name = sheet_name.replace('Beef ', '')
                
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
                logger.debug(f"Processed {primal_name} with {len(subprimal_dict)} subprimals")
            
            # Process grade mappings from the Grades sheet
            if 'Grades' in sheet_data:
                grades_df = sheet_data['Grades']
                for _, row in grades_df.iterrows():
                    official_grade = row['Official / Commercial Grade Name']
                    if pd.notna(row.get('Common Synonyms & Acronyms')):
                        # Split by comma and strip whitespace
                        synonyms = [s.strip() for s in str(row['Common Synonyms & Acronyms']).split(',')]
                        self.grade_mappings[official_grade] = synonyms
                        
                logger.debug(f"Processed {len(self.grade_mappings)} grade mappings")
            else:
                logger.warning("Grades sheet not found in the reference data file")
                    
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
        
    def get_synonyms(self, term_type: str, term_name: str, primal: Optional[str] = None) -> List[str]:
        """
        General method to get synonyms for different types of terms.
        Used for backward compatibility with code expecting this method.
        
        Args:
            term_type: Type of term to get synonyms for ('subprimal' or 'grade')
            term_name: Name of the term to get synonyms for
            primal: Primal cut name (required for subprimal synonyms)
            
        Returns:
            List of synonyms for the specified term with duplicates removed
        """
        # Defensive check to ensure term_type is not None
        if term_type is None:
            logger.warning("term_type is None in get_synonyms call")
            return []
            
        # Get synonyms based on term type
        if term_type.lower() == 'subprimal':
            if not primal:
                logger.warning("Primal cut name is required for subprimal synonyms")
                return []
            synonyms = self.get_subprimal_synonyms(primal, term_name)
        
        elif term_type.lower() == 'grade':
            synonyms = self.get_grade_synonyms(term_name)
        
        else:
            logger.warning(f"Unknown term type for get_synonyms: {term_type}")
            return []
        
        # Remove duplicates by converting to set and back to list
        return list(set(synonyms))
        
    def get_subprimal_terms(self, primal: str, subprimal: str) -> List[str]:
        """
        Get all terms (name and synonyms) for a specific subprimal cut.
        
        Args:
            primal: The primal cut name
            subprimal: The subprimal cut name
            
        Returns:
            List of all terms for the subprimal (including the subprimal name itself)
        """
        result = [subprimal]  # Always include the subprimal name itself
        
        # Add synonyms if they exist
        synonyms = self.get_subprimal_synonyms(primal, subprimal)
        result.extend(synonyms)
        
        return result

