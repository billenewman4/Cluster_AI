"""
API Utilities Module
Handles OpenAI API interactions, rate limiting, and retry mechanisms.
"""

import time
import random
import logging
import threading
from typing import Optional, Dict, Any

import openai
from openai import OpenAI

# Configure logging
logger = logging.getLogger(__name__)

class APIManager:
    """Manages API interactions with rate limiting and retries."""
    
    def __init__(self, api_key: str, model: str = "gpt-4", max_rpm: int = 100):
        """Initialize API manager.
        
        Args:
            api_key: OpenAI API key
            model: Model to use for completions
            max_rpm: Maximum requests per minute
        """
        if not api_key or not isinstance(api_key, str):
            raise ValueError("API key must be a non-empty string")
        if not api_key.startswith(('sk-', 'sk-proj-')):
            logger.warning("API key format may be invalid")
        
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.max_rpm = max_rpm
        self.request_times = []
        # Add thread lock for synchronization
        self._lock = threading.RLock()
    
    def enforce_rate_limit(self) -> None:
        """Enforce API rate limits to prevent 429 errors.
        
        Thread-safe implementation using lock synchronization.
        """
        current_time = time.time()
        
        with self._lock:
            # Remove requests older than 1 minute - O(n) time complexity
            self.request_times = [t for t in self.request_times if current_time - t < 60]
            
            # If we're at the limit, wait
            if len(self.request_times) >= self.max_rpm:
                sleep_time = max(0, 60 - (current_time - self.request_times[0])) + random.uniform(0.5, 1.5)
                logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                # Release lock during sleep to prevent blocking other threads
                self._lock.release()
                try:
                    time.sleep(sleep_time)
                finally:
                    # Re-acquire lock after sleep
                    self._lock.acquire()
            
            # Record this request
            self.request_times.append(current_time)
    
    def call_with_retry(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        max_retries: int = 3,
        temperature: float = 0.1,
        max_tokens: int = 500
    ) -> Optional[str]:
        """Call OpenAI API with retry and backoff logic.
        
        Args:
            system_prompt: System message for the chat completion
            user_prompt: User message for the chat completion
            max_retries: Maximum number of retry attempts
            temperature: Sampling temperature (0.0 to 1.0)
            max_tokens: Maximum tokens in the response
            
        Returns:
            The LLM response text, or None if all attempts fail
        """
        # Input validation
        if not system_prompt or not isinstance(system_prompt, str):
            raise ValueError("system_prompt must be a non-empty string")
        if not user_prompt or not isinstance(user_prompt, str):
            raise ValueError("user_prompt must be a non-empty string")
        if max_retries < 0 or temperature < 0 or max_tokens <= 0:
            raise ValueError("Invalid parameter values")
        
        for attempt in range(max_retries):
            try:
                # Enforce rate limiting
                self.enforce_rate_limit()
                
                # Make API call
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                
                return response.choices[0].message.content.strip()
                
            except openai.RateLimitError as e:
                logger.warning(f"Rate limit hit on attempt {attempt + 1}: {str(e)}")
            except openai.APIError as e:
                logger.warning(f"API error on attempt {attempt + 1}: {str(e)}")
            except Exception as e:
                logger.warning(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            
            if attempt < max_retries - 1:
                # Exponential backoff with jitter
                sleep_time = (2 ** attempt) + random.uniform(0, 1)
                logger.info(f"Retrying in {sleep_time:.2f} seconds (attempt {attempt+1}/{max_retries})")
                time.sleep(sleep_time)
            else:
                logger.error(f"All API attempts failed after {max_retries} retries")
        
        return None