"""
Firebase Database Client Module

Provides a centralized interface for Firebase Firestore database operations with optimized
connection handling, efficient querying, and proper error management.
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.cloud.firestore_v1.base_query import FieldFilter

# Configure logging
logger = logging.getLogger(__name__)

class FirebaseClient:
    """Firebase client for database operations with connection pooling and error handling."""
    
    _instance = None  # Singleton instance
    
    def __new__(cls, *args, **kwargs):
        """Implement singleton pattern for connection reuse."""
        if cls._instance is None:
            cls._instance = super(FirebaseClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, 
                 credentials_path: Optional[str] = None,
                 collection_name: str = "beef_cuts",
                 project_id: Optional[str] = None):
        """Initialize Firebase client with credentials and default collection.
        
        Args:
            credentials_path: Path to Firebase service account JSON file (optional)
            collection_name: Default Firestore collection name to use
            project_id: Optional project ID to override default
        """
        if self._initialized:
            return
            
        self._initialized = True
        self._app = None
        self._db = None
        self._storage = None
        self.collection_name = collection_name
        
        # Check for existing Firebase apps
        try:
            existing_app = firebase_admin.get_app()
            self._app = existing_app
            logger.info(f"Using existing Firebase app: {self._app.name}")
            self._db = firestore.client()
            try:
                # Try to get the default storage bucket
                self._storage = storage.bucket(app=self._app)
            except Exception as storage_error:
                logger.warning(f"Could not initialize storage bucket: {storage_error}")
                self._storage = None
            return
        except ValueError:
            # No existing app, proceed with initialization
            pass
        
        try:
            # Try to load credentials from various locations
            creds = self._load_credentials(credentials_path)
            
            # Set options with explicit project ID if provided
            options = {}
            if project_id:
                options['projectId'] = project_id
                options['storageBucket'] = f"{project_id}.appspot.com"
            
            # Initialize Firebase with the obtained credentials
            if creds:
                if hasattr(creds, 'project_id') and creds.project_id:
                    # Use project ID from credentials if available
                    logger.info(f"Using project ID from credentials: {creds.project_id}")
                    options['projectId'] = creds.project_id
                    options['storageBucket'] = f"{creds.project_id}.appspot.com"
                
                self._app = firebase_admin.initialize_app(creds, options if options else None)
                self._db = firestore.client()
                
                # Handle storage bucket initialization properly
                try:
                    if project_id:
                        bucket_name = f"{project_id}.appspot.com"
                        self._storage = storage.bucket(bucket_name, app=self._app)
                        logger.info(f"Storage bucket initialized with name: {bucket_name}")
                    else:
                        # Try to get bucket from app config
                        self._storage = storage.bucket(app=self._app)
                except Exception as e:
                    logger.error(f"Could not initialize storage bucket: {str(e)}")
                    raise ConnectionError(f"Could not connect to Firebase Storage: {str(e)}")
            else:
                # Default initialization with application default credentials
                self._app = firebase_admin.initialize_app(options=options if options else None)
                self._db = firestore.client()
                
                # Handle storage bucket initialization properly
                try:
                    if project_id:
                        bucket_name = f"{project_id}.appspot.com"
                        self._storage = storage.bucket(bucket_name, app=self._app)
                        logger.info(f"Storage bucket initialized with name: {bucket_name}")
                    else:
                        # Try to get bucket from app config
                        self._storage = storage.bucket(app=self._app)
                except Exception as e:
                    logger.error(f"Could not initialize storage bucket: {str(e)}")
                    raise ConnectionError(f"Could not connect to Firebase Storage: {str(e)}")
                
        except Exception as e:
            logger.error(f"Firebase initialization failed: {str(e)}")
            raise ConnectionError(f"Could not connect to Firebase: {str(e)}")
            
        # Log successful connection
        logger.info(f"Successfully connected to Firestore database")
        if self._storage:
            logger.info(f"Successfully connected to Firebase Storage")
    
    def _load_credentials(self, credentials_path=None):
        """
        Load Firebase credentials from multiple potential sources with robust validation.
        
        Args:
            credentials_path: Optional explicit path to credentials file
            
        Returns:
            Firebase credentials object or None for default credentials
        """
        # Try explicit path first
        if credentials_path and os.path.isfile(credentials_path):
            try:
                logger.info(f"Loading Firebase credentials from {credentials_path}")
                creds = credentials.Certificate(credentials_path)
                # Validate the credentials - check for project_id which should always be present
                if hasattr(creds, 'project_id') and creds.project_id:
                    service_email = getattr(creds, 'client_email', getattr(creds, '_service_account_email', 'unknown'))
                    logger.info(f"Valid credentials loaded for project: {creds.project_id} (service account: {service_email})")
                    return creds
                else:
                    logger.error(f"Credentials at {credentials_path} are invalid (missing project_id)")
                    raise ValueError(f"Invalid credentials: missing project_id")
            except Exception as e:
                logger.error(f"Invalid credentials at {credentials_path}: {str(e)}")
                raise ValueError(f"Invalid credentials at {credentials_path}: {str(e)}")
        
        # Try environment variable paths (support both common env var names)
        for env_var in ['FIREBASE_CREDENTIALS', 'GOOGLE_APPLICATION_CREDENTIALS']:
            env_path = os.environ.get(env_var)
            if env_path and os.path.isfile(env_path):
                try:
                    logger.info(f"Loading Firebase credentials from {env_var} env: {env_path}")
                    creds = credentials.Certificate(env_path)
                    if hasattr(creds, 'project_id') and creds.project_id:
                        service_email = getattr(creds, 'client_email', getattr(creds, '_service_account_email', 'unknown'))
                        logger.info(f"Valid credentials loaded for project: {creds.project_id} (service account: {service_email})")
                        return creds
                    else:
                        logger.error(f"Credentials from {env_var} are invalid (missing project_id)")
                        raise ValueError(f"Invalid credentials from {env_var}: missing project_id")
                except Exception as e:
                    logger.error(f"Invalid credentials from {env_var} env: {str(e)}")
                    raise ValueError(f"Invalid credentials from {env_var} env: {str(e)}")
        
        # Try default path in project
        project_root = Path(__file__).resolve().parent.parent.parent
        default_path = project_root / "config" / "firebase" / "service-account.json"
        
        if default_path.exists():
            try:
                logger.info(f"Loading Firebase credentials from default path: {default_path}")
                creds = credentials.Certificate(str(default_path))
                if hasattr(creds, 'project_id') and creds.project_id:
                    service_email = getattr(creds, 'client_email', getattr(creds, '_service_account_email', 'unknown'))
                    logger.info(f"Valid credentials loaded for project: {creds.project_id} (service account: {service_email})")
                    return creds
                else:
                    logger.error(f"Credentials at default path are invalid (missing project_id)")
                    raise ValueError(f"Invalid credentials at default path: missing project_id")
            except Exception as e:
                logger.error(f"Invalid credentials at {default_path}: {str(e)}")
                raise ValueError(f"Invalid credentials at {default_path}: {str(e)}")
        
        # Check additional common paths
        common_paths = [
            project_root / "config" / "firebase" / "credentials.json",
            project_root / "config" / "firebase" / "firebase-adminsdk.json",
            Path.home() / ".config" / "firebase" / "service-account.json"
        ]
        
        for path in common_paths:
            if path.exists():
                try:
                    creds = credentials.Certificate(str(path))
                    if hasattr(creds, '_service_account_email') and creds._service_account_email:
                        return creds
                    else:
                        logger.error(f"Credentials at {path} are invalid")
                        continue
                except ValueError as e:
                    logger.error(f"Invalid credentials at {path}: {e}")
        
        # If FIREBASE_CONFIG or GOOGLE_APPLICATION_CREDENTIALS environment variables are set
        # or running in a GCP environment, return None to use Application Default Credentials
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("FIREBASE_CONFIG"):
            logger.info("Using application default credentials from environment variables")
        
        # Return None to use application default credentials
        return None
    
    def get_document(self, doc_id: str, collection: Optional[str] = None) -> Optional[Dict]:
        """Get a document by ID with optimized network usage.
        
        Args:
            doc_id: Document ID to retrieve
            collection: Collection name or use default if None
            
        Returns:
            Document data as dict or None if not found
        """
        coll = collection or self.collection_name
        try:
            doc_ref = self._db.collection(coll).document(doc_id)
            doc = doc_ref.get()
            if doc.exists:
                return doc.to_dict()
            return None
        except Exception as e:
            logger.error(f"Error retrieving document {doc_id} from {coll}: {str(e)}")
            return None
    
    def get_documents(self, collection: Optional[str] = None, 
                      filters: Optional[List[Dict]] = None, 
                      limit: Optional[int] = None,
                      order_by: Optional[str] = None,
                      direction: str = "ASCENDING") -> List[Dict]:
        """Get multiple documents with filters and pagination.
        
        Args:
            collection: Collection name or use default if None
            filters: List of filter dictionaries with field, op, value
            limit: Maximum number of documents to return
            order_by: Field to order results by
            direction: "ASCENDING" or "DESCENDING"
            
        Returns:
            List of document dictionaries
        """
        coll = collection or self.collection_name
        try:
            query = self._db.collection(coll)
            
            # Apply filters if provided
            if filters:
                for filter_dict in filters:
                    field = filter_dict.get("field")
                    op = filter_dict.get("op", "==")
                    value = filter_dict.get("value")
                    query = query.where(filter=FieldFilter(field, op, value))
            
            # Apply order if provided
            if order_by:
                direction_obj = firestore.Query.ASCENDING if direction == "ASCENDING" else firestore.Query.DESCENDING
                query = query.order_by(order_by, direction=direction_obj)
            
            # Apply limit if provided
            if limit:
                query = query.limit(limit)
                
            # Execute query
            docs = query.stream()
            return [doc.to_dict() | {"id": doc.id} for doc in docs]
            
        except Exception as e:
            logger.error(f"Error querying collection {coll}: {str(e)}")
            return []
    
    def add_document(self, data: Dict, collection: Optional[str] = None, 
                     doc_id: Optional[str] = None) -> Optional[str]:
        """Add a document to Firestore with optimized write.
        
        Args:
            data: Document data to store
            collection: Collection name or use default if None
            doc_id: Optional document ID (auto-generated if None)
            
        Returns:
            Document ID of added document or None if failed
        """
        coll = collection or self.collection_name
        try:
            coll_ref = self._db.collection(coll)
            if doc_id:
                doc_ref = coll_ref.document(doc_id)
                doc_ref.set(data)
                return doc_id
            else:
                doc_ref = coll_ref.add(data)[1]
                return doc_ref.id
        except Exception as e:
            logger.error(f"Error adding document to {coll}: {str(e)}")
            return None
    
    def update_document(self, doc_id: str, data: Dict, 
                        collection: Optional[str] = None,
                        merge: bool = True) -> bool:
        """Update a document with merge support and optimized writes.
        
        Args:
            doc_id: Document ID to update
            data: Fields to update
            collection: Collection name or use default if None
            merge: Whether to merge with existing data or overwrite
            
        Returns:
            True if successful, False otherwise
        """
        coll = collection or self.collection_name
        try:
            doc_ref = self._db.collection(coll).document(doc_id)
            doc_ref.set(data, merge=merge)
            return True
        except Exception as e:
            logger.error(f"Error updating document {doc_id} in {coll}: {str(e)}")
            return False
    
    def delete_document(self, doc_id: str, collection: Optional[str] = None) -> bool:
        """Delete a document by ID.
        
        Args:
            doc_id: Document ID to delete
            collection: Collection name or use default if None
            
        Returns:
            True if successful, False otherwise
        """
        coll = collection or self.collection_name
        try:
            doc_ref = self._db.collection(coll).document(doc_id)
            doc_ref.delete()
            return True
        except Exception as e:
            logger.error(f"Error deleting document {doc_id} from {coll}: {str(e)}")
            return False
    
    def batch_write(self, operations: List[Dict], 
                    collection: Optional[str] = None) -> int:
        """Perform batch operations for efficiency with transaction support.
        
        Args:
            operations: List of operation dictionaries {op, doc_id, data}
                where op is one of 'set', 'update', 'delete'
            collection: Collection name or use default if None
            
        Returns:
            Number of successful operations
        """
        if not operations:
            return 0
            
        coll = collection or self.collection_name
        batch = self._db.batch()
        count = 0
        
        try:
            for op in operations:
                doc_id = op.get("doc_id")
                data = op.get("data")
                operation = op.get("op", "set")
                
                doc_ref = self._db.collection(coll).document(doc_id)
                
                if operation == "set":
                    merge = op.get("merge", True)
                    batch.set(doc_ref, data, merge=merge)
                    count += 1
                elif operation == "update":
                    batch.update(doc_ref, data)
                    count += 1
                elif operation == "delete":
                    batch.delete(doc_ref)
                    count += 1
                else:
                    logger.warning(f"Unknown batch operation: {operation}")
            
            batch.commit()
            return count
        except Exception as e:
            logger.error(f"Error in batch operation: {str(e)}")
            return 0
    
    @property
    def db(self) -> firestore.Client:
        """Access to raw Firestore client for advanced operations."""
        return self._db
    
    @property
    def storage(self):
        """Access to raw Storage bucket for advanced operations."""
        return self._storage
