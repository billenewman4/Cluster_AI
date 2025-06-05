"""
API Utilities Module
Handles OpenAI API interactions, rate limiting, and retry mechanisms.
"""

import time
import random
import logging
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
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.max_rpm = max_rpm
        self.request_times = []
    
    def enforce_rate_limit(self) -> None:
        """Enforce API rate limits to prevent 429 errors."""
        current_time = time.time()
        
        # Remove requests older than 1 minute
        self.request_times = [t for t in self.request_times if current_time - t < 60]
        
        # If we're at the limit, wait
        if len(self.request_times) >= self.max_rpm:
            sleep_time = 60 - (current_time - self.request_times[0]) + random.uniform(0.5, 1.5)
            logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
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
            system_prompt: System prompt for the model
            user_prompt: User prompt with instructions
            max_retries: Maximum number of retry attempts
            temperature: Sampling temperature
            max_tokens: Maximum tokens in the response
            
        Returns:
            Optional[str]: Model response or None if all retries failed
        """
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
                
            except Exception as e:
                logger.warning(f"API call attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All API attempts failed after {max_retries} retries")
                    return None
        
        return None
