"""
Product Data Model
Provides a unified data structure for product information throughout the pipeline.
Includes both required fields from data ingestion and optional fields from LLM extraction.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, ClassVar


@dataclass
class ProductData:
    """
    Data class representing a product with all required and optional attributes.
    
    Required fields are populated during data ingestion, while optional fields
    are filled in later by the LLM extraction process.
    """
    # Required fields (populated during data ingestion)
    productcode: str
    productdescription: str
    category_description: str  # Keep this as-is since cleaner already normalizes it to this form
    
    # Optional fields (populated during LLM extraction)
    subprimal: Optional[str] = None
    grade: Optional[str] = None
    size: Optional[float] = None
    size_uom: Optional[str] = None  # Unit of measurement (oz, lb, #, g, kg)
    brand: Optional[str] = None
    bone_in: bool = False
    
    # Database-specific fields
    family: Optional[str] = None  # Constructed from species, primal, subprimal, and grade
    approved: str = ''  # Approval status field
    comments: str = ''  # User comments field
    species: Optional[str] = None  # Species field (beef, pork, etc.)
    
    # Metadata fields
    confidence: float = 0.0
    needs_review: bool = False
    
    # Additional data that might be preserved from original sources
    additional_data: Dict[str, Any] = field(default_factory=dict)
    
    # Class variable for required fields
    REQUIRED_FIELDS: ClassVar[List[str]] = [
        'productcode',
        'productdescription',
        'category_description'
    ]
    
    # Class variable for all standard fields used in the data pipeline
    STANDARD_FIELDS: ClassVar[List[str]] = [
        'product_code',
        'product_description',
        'category_description',
        'subprimal',
        'grade',
        'size',
        'size_uom',
        'brand',
        'bone_in',
        'family',
        'approved',
        'comments',
        'species',
        'confidence', 
        'needs_review'
    ]
    
    def __post_init__(self):
        """Post-initialization processing."""
        # Generate family field if species, primal, subprimal, or grade are present
        if self.family is None and any([self.species, self.grade, self.subprimal, self.category_description]):
            components = []
            if self.species:
                components.append(self.species)
            if self.category_description:
                components.append(self.category_description)
            if self.subprimal:
                components.append(self.subprimal)
            if self.grade:
                components.append(self.grade)
            
            if components:
                self.family = ' '.join(components)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the product data to a dictionary."""
        return {
            # Required fields
            'product_code': self.product_code,
            'product_description': self.product_description,
            'category_description': self.category_description,
            
            # Optional fields - only include non-None values
            **({'subprimal': self.subprimal} if self.subprimal is not None else {}),
            **({'grade': self.grade} if self.grade is not None else {}),
            **({'size': self.size} if self.size is not None else {}),
            **({'size_uom': self.size_uom} if self.size_uom is not None else {}),
            **({'brand': self.brand} if self.brand is not None else {}),
            'bone_in': self.bone_in,
            
            # Database-specific fields
            **({'family': self.family} if self.family is not None else {}),
            'approved': self.approved,
            'comments': self.comments,
            **({'species': self.species} if self.species is not None else {}),
            
            # Metadata
            'confidence': self.confidence,
            'needs_review': self.needs_review,
            
            # Additional data
            **self.additional_data
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProductData":
        """
        Create a ProductData instance from a dictionary.
        
        Args:
            data: Dictionary containing product data
            
        Returns:
            ProductData: New instance with data from dictionary
        """
        # Extract required and known optional fields
        required_fields = {
            'productcode': data.get('productcode', ''),
            'productdescription': data.get('productdescription', ''),
            'category_description': data.get('category_description', '')
        }
        
        optional_fields = {
            'subprimal': data.get('subprimal'),
            'grade': data.get('grade'),
            'size': data.get('size'),
            'size_uom': data.get('size_uom'),
            'brand': data.get('brand'),
            'bone_in': data.get('bone_in', False),
            'family': data.get('family'),
            'approved': data.get('approved', ''),
            'comments': data.get('comments', ''),
            'species': data.get('species'),
            'confidence': data.get('confidence', 0.0),
            'needs_review': data.get('needs_review', False)
        }
        
        # Create additional_data with any remaining fields
        known_fields = set(required_fields.keys()) | set(optional_fields.keys())
        additional_data = {k: v for k, v in data.items() if k not in known_fields}
        
        # Create and return instance
        return cls(
            **required_fields,
            **optional_fields,
            additional_data=additional_data
        )
        
    @classmethod
    def get_required_field_names(cls) -> List[str]:
        """
        Return a list of required field names for data transformation.
        
        Used to standardize incoming data column names before processing.
        
        Returns:
            List[str]: List of required field names
        """
        return cls.REQUIRED_FIELDS.copy()
    
    @classmethod
    def get_standard_field_names(cls) -> List[str]:
        """
        Return a list of all standard field names used in the pipeline.
        
        Includes both required fields and optional fields with standard naming.
        
        Returns:
            List[str]: List of standard field names
        """
        return cls.STANDARD_FIELDS.copy()
    
    @classmethod
    def create_column_mapping(cls, source_columns: List[str]) -> Dict[str, str]:
        """
        Create a mapping from source column names to standard field names.
        
        Attempts to match source columns to standard fields using case-insensitive
        comparison and common naming patterns.
        
        Args:
            source_columns: List of source column names from input data
            
        Returns:
            Dict[str, str]: Mapping from source columns to standard field names
        """
        mapping = {}
        standard_fields_lower = {field.lower(): field for field in cls.STANDARD_FIELDS}
        
        # Comprehensive mapping of column name variations from all pipeline components
        # Combined from ProductTransformer, DataCleaner, and domain knowledge
        variations = {
            'product_code': [
                'productcode'
            ],
            'product_description1': [
                'productdescription', 'product description', 'description', 
                'item_description', 'product description 1', 'product_name', 
                'item_name', 'title', 'item_title', 'productdescription',
                'productdescription1', 'productdescription'
            ],
            'product_description2': [
                'productdescription2', 'product_descriptoin2'
            ],
            'category_description': [
                'productcategory'
            ],
            'subprimal': [
                'sub_primal', 'sub primal', 'subprimalcut', 'sub-primal',
                'cut_type', 'cut', 'primal_cut', 'subprimal_cut'
            ],
            'brand': [
                'branddescription', 'brand_description', 'brandname', 'brand_name',
                'manufacturer', 'vendor', 'supplier_name', 'producer'
            ],
            'grade': [
                'product_grade', 'meat_grade', 'quality_grade', 'usda_grade', 
                'grading', 'quality_level'
            ],
            'size': [
                'product_size', 'item_size', 'weight', 'net_weight', 
                'package_weight', 'portion_size'
            ],
            'size_uom': [
                'uom', 'unit_of_measure', 'measure_unit', 'weight_unit',
                'size_unit', 'unit', 'measurement'
            ],
            'species': [
                'animal', 'meat_type', 'origin', 'product_type', 'protein_type'
            ],
            'family': [
                'product_family', 'item_family', 'product_group', 'meat_family'
            ],
            'bone_in': [
                'has_bone', 'bone', 'bone_type', 'boneless'
            ]
        }
        
        # Try to match each source column to a standard field
        for source_col in source_columns:
            # Handle None columns gracefully
            if source_col is None:
                continue
                
            source_col_lower = str(source_col).lower().strip()
            
            # Direct match
            if source_col_lower in standard_fields_lower:
                mapping[source_col] = standard_fields_lower[source_col_lower]
                continue
                
            # Check known variations
            for standard_field, aliases in variations.items():
                if source_col_lower in aliases:
                    mapping[source_col] = standard_field
                    break
            
            # Handle special cases with more complex transformations
            col_no_spaces = source_col_lower.replace(' ', '')
            if col_no_spaces in standard_fields_lower:
                mapping[source_col] = standard_fields_lower[col_no_spaces]
                continue
                
            for standard_field, aliases in variations.items():
                if col_no_spaces in aliases:
                    mapping[source_col] = standard_field
                    break
                    
            # Check for partial matches (contains)
            if not source_col in mapping:
                if 'product' in source_col_lower and 'code' in source_col_lower:
                    mapping[source_col] = 'product_code'
                elif 'product' in source_col_lower and 'descr' in source_col_lower:
                    mapping[source_col] = 'product_description'
                elif 'category' in source_col_lower or 'categ' in source_col_lower:
                    mapping[source_col] = 'category_description'
                
        return mapping