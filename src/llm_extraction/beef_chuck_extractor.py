"""
Beef Chuck Extractor
Specialized extractor for beef chuck subprimal cuts, inheriting from BaseLLMExtractor.
"""

from typing import Dict, List
from .base_extractor import BaseLLMExtractor

class BeefChuckExtractor(BaseLLMExtractor):
    """Specialized extractor for beef chuck cuts."""
    
    # Beef-specific grades with variations
    BEEF_CHUCK_GRADES = {
        'no grade': ['no grade', 'ungraded', 'commercial', 'natural/off', 'natural', 'n/off'],
        'prime': ['prime', 'pr'],
        'angus': ['angus', 'certified angus beef', 'cab', 'black angus', 'ang'],
        'creekstone angus': ['creekstone angus', 'creekstone', 'creek stone angus'],
        'choice': ['choice', 'ch', 'aaa', 'aaa grade', 'aaa exc'],  # Choice (AAA)
        'select': ['select', 'se', 'aa', 'aa grade'],              # Select (AA)
        'no roll': ['no roll', 'nr', 'n/r'],                       # No Roll (NR)
        'utility': ['utility', 'ute', 'a', 'a grade'],             # Utility (A)
        'wagyu': ['wagyu', 'kobe', 'kobe beef', 'japanese wagyu', 'american wagyu']  # Wagyu/Kobe
    }
    
    def get_category_name(self) -> str:
        """Return the category name."""
        return "Beef Chuck"
    
    def get_beef_grades(self) -> Dict[str, List[str]]:
        """Return beef-specific grades with variations."""
        return self.BEEF_CHUCK_GRADES
    
    def get_subprimal_mapping(self) -> Dict[str, List[str]]:
        """Return mapping of chuck subprimal names to their variations.
        
        Based on the requirements, detecting these chuck cuts:
        - Chuck flap, chuck roll, clod/clod shoulder (same thing), clod hearts, 
          flat iron, teres major, chuck short rib, bone in chuck, bone in arm, 
          top blade, blade meat, scotty/scotch tender/mock tender (same thing)
        """
        return {
            'chuck flap': ['chuck flap', 'flap'],
            'chuck roll': ['chuck roll', 'roll', 'chuck eye roll'],
            'clod': ['clod', 'clod shoulder', 'shoulder clod', 'shl clod'],  # clod/clod shoulder are same
            'clod hearts': ['clod hearts', 'clod heart', 'heart of clod'],
            'flat iron': ['flat iron', 'flatiron', 'flat-iron'],
            'teres major': ['teres major', 'teres', 'shoulder tender'],
            'chuck short rib': ['chuck short rib', 'chuck short ribs', 'chuck ribs'],
            'bone in chuck': ['bone in chuck', 'bone-in chuck', 'chuck bone-in', 'bi chuck'],
            'bone in arm': ['bone in arm', 'bone-in arm', 'arm roast', 'arm pot roast', 'bi arm'],
            'top blade': ['top blade', 'blade steak', 'top blade steak'],
            'blade meat': ['blade meat', 'blade', 'chuck blade', 'blad'],
            'mock tender': ['scotty', 'scotch tender', 'mock tender', 'chuck tender']  # scotty/scotch tender/mock tender are same
        } 