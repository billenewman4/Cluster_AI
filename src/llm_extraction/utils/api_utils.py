"""
API Utilities Module
Handles LLM interactions using LangChain for easy provider switching.
"""

import time
import random
import logging
import threading
from typing import Optional, Dict, Any, Union

# Core LangChain imports (required)
try:
    from langchain_core.messages import SystemMessage, HumanMessage
    from langchain_core.language_models.chat_models import BaseChatModel
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

# Provider-specific imports (optional)
AVAILABLE_PROVIDERS = {}

# OpenAI (always try to import since it's most common)
try:
    from langchain_openai import ChatOpenAI
    AVAILABLE_PROVIDERS["openai"] = {
        "class": ChatOpenAI,
        "default_model": "gpt-4o-mini",
        "api_key_env": "OPENAI_API_KEY"
    }
except ImportError:
    pass

# Anthropic (optional)
try:
    from langchain_anthropic import ChatAnthropic
    AVAILABLE_PROVIDERS["anthropic"] = {
        "class": ChatAnthropic,
        "default_model": "claude-3-sonnet-20240229",
        "api_key_env": "ANTHROPIC_API_KEY"
    }
except ImportError:
    pass

# Google (optional)
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    AVAILABLE_PROVIDERS["google"] = {
        "class": ChatGoogleGenerativeAI,
        "default_model": "gemini-pro",
        "api_key_env": "GOOGLE_API_KEY"
    }
except ImportError:
    pass

# Fallback to OpenAI directly if LangChain not available
if not LANGCHAIN_AVAILABLE:
    try:
        import openai
        from openai import OpenAI
        OPENAI_DIRECT_AVAILABLE = True
    except ImportError:
        OPENAI_DIRECT_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

class APIManager:
    """Manages LLM interactions with rate limiting and retries using LangChain."""
    
    def __init__(
        self, 
        provider: str = "openai",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        max_rpm: int = 100,
        temperature: float = 0.6,
        max_tokens: int = 1000,
        **kwargs
    ):
        """Initialize API manager with LangChain or fallback to OpenAI direct.
        
        Args:
            provider: LLM provider ("openai", "anthropic", "google")
            model: Model name (uses provider default if None)
            api_key: API key (will look in environment if None)
            max_rpm: Maximum requests per minute
            temperature: Default sampling temperature
            max_tokens: Default max tokens
            **kwargs: Additional provider-specific parameters
        """
        # Validate API key
        if not api_key or not isinstance(api_key, str):
            raise ValueError("API key must be a non-empty string")
        
        self.provider = provider
        self.max_rpm = max_rpm
        self.default_temperature = temperature
        self.default_max_tokens = max_tokens
        
        # Rate limiting
        self.request_times = []
        self._lock = threading.RLock()
        
        # Initialize based on available libraries
        if LANGCHAIN_AVAILABLE and provider in AVAILABLE_PROVIDERS:
            self._use_langchain = True
            self.provider_config = AVAILABLE_PROVIDERS[provider]
            self.model_name = model or self.provider_config["default_model"]
            self.chat_model = self._initialize_langchain_model(api_key, **kwargs)
        elif provider == "openai" and OPENAI_DIRECT_AVAILABLE:
            # Fallback to direct OpenAI
            self._use_langchain = False
            self.model_name = model or "gpt-4o-mini"
            if not api_key.startswith(('sk-', 'sk-proj-')):
                logger.warning("API key format may be invalid")
            self.client = OpenAI(api_key=api_key)
            logger.info("Using direct OpenAI client (LangChain not available)")
        else:
            available = list(AVAILABLE_PROVIDERS.keys()) if LANGCHAIN_AVAILABLE else ["openai (direct)"]
            raise ValueError(f"Provider '{provider}' not available. Available providers: {available}")
    
    def _initialize_langchain_model(self, api_key: str, **kwargs) -> BaseChatModel:
        """Initialize the appropriate LangChain chat model."""
        provider_class = self.provider_config["class"]
        
        # Base parameters for all providers
        init_params = {
            "model": self.model_name,
            "temperature": self.default_temperature,
            **kwargs
        }
        
        # Add API key with correct parameter name
        if self.provider == "openai":
            init_params["openai_api_key"] = api_key
            init_params["max_tokens"] = self.default_max_tokens
        elif self.provider == "anthropic":
            init_params["anthropic_api_key"] = api_key
            init_params["max_tokens"] = self.default_max_tokens
        elif self.provider == "google":
            init_params["google_api_key"] = api_key
        
        try:
            return provider_class(**init_params)
        except Exception as e:
            logger.error(f"Failed to initialize {self.provider} chat model: {e}")
            raise ValueError(f"Could not initialize {self.provider} with model {self.model_name}") from e
    
    def enforce_rate_limit(self) -> None:
        """Enforce API rate limits to prevent 429 errors."""
        current_time = time.time()
        
        with self._lock:
            # Remove requests older than 1 minute
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
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None
    ) -> Optional[str]:
        """Call LLM API with retry and backoff logic.
        
        Args:
            system_prompt: System message for the chat completion
            user_prompt: User message for the chat completion
            max_retries: Maximum number of retry attempts
            temperature: Sampling temperature (overrides default if provided)
            max_tokens: Maximum tokens in response (overrides default if provided)
            
        Returns:
            The LLM response text, or None if all attempts fail
        """
        # Input validation
        if not system_prompt or not isinstance(system_prompt, str):
            raise ValueError("system_prompt must be a non-empty string")
        if not user_prompt or not isinstance(user_prompt, str):
            raise ValueError("user_prompt must be a non-empty string")
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        
        if self._use_langchain:
            return self._call_with_langchain(system_prompt, user_prompt, max_retries, temperature, max_tokens)
        else:
            return self._call_with_openai_direct(system_prompt, user_prompt, max_retries, temperature, max_tokens)
    
    def _call_with_langchain(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        max_retries: int,
        temperature: Optional[float],
        max_tokens: Optional[int]
    ) -> Optional[str]:
        """Call using LangChain."""
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        for attempt in range(max_retries):
            try:
                # Enforce rate limiting
                self.enforce_rate_limit()
                
                # Use custom parameters if provided
                chat_model = self.chat_model
                if temperature is not None or max_tokens is not None:
                    custom_params = {"model": self.model_name}
                    if temperature is not None:
                        custom_params["temperature"] = temperature
                    if max_tokens is not None and self.provider in ["openai", "anthropic"]:
                        custom_params["max_tokens"] = max_tokens
                    
                    # Create temporary model with custom parameters
                    provider_class = self.provider_config["class"]
                    if self.provider == "openai":
                        custom_params["openai_api_key"] = self.chat_model.openai_api_key
                        chat_model = provider_class(**custom_params)
                    elif self.provider == "anthropic":
                        custom_params["anthropic_api_key"] = self.chat_model.anthropic_api_key
                        chat_model = provider_class(**custom_params)
                
                # Make API call
                response = chat_model.invoke(messages)
                
                if hasattr(response, 'content'):
                    return response.content.strip()
                else:
                    return str(response).strip()
                
            except Exception as e:
                error_type = type(e).__name__
                logger.warning(f"{error_type} on attempt {attempt + 1}: {str(e)}")
                
                if attempt < max_retries - 1:
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All LangChain attempts failed after {max_retries} retries")
        
        return None
    
    def _call_with_openai_direct(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        max_retries: int,
        temperature: Optional[float],
        max_tokens: Optional[int]
    ) -> Optional[str]:
        """Call using direct OpenAI client (fallback)."""
        for attempt in range(max_retries):
            try:
                # Enforce rate limiting
                self.enforce_rate_limit()
                
                # Make API call
                response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=temperature or self.default_temperature,
                    max_tokens=max_tokens or self.default_max_tokens
                )
                
                return response.choices[0].message.content.strip()
                
            except Exception as e:
                error_type = type(e).__name__
                logger.warning(f"{error_type} on attempt {attempt + 1}: {str(e)}")
                
                if attempt < max_retries - 1:
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.info(f"Retrying in {sleep_time:.2f} seconds") 
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All OpenAI attempts failed after {max_retries} retries")
        
        return None
    
    def get_provider_info(self) -> Dict[str, Any]:
        """Get information about the current provider configuration."""
        return {
            "provider": self.provider,
            "model": self.model_name,
            "max_rpm": self.max_rpm,
            "default_temperature": self.default_temperature,
            "default_max_tokens": self.default_max_tokens,
            "using_langchain": self._use_langchain,
            "available_providers": list(AVAILABLE_PROVIDERS.keys()) if LANGCHAIN_AVAILABLE else ["openai (direct)"]
        }
    
    @classmethod
    def create_openai(cls, api_key: str, model: str = "gpt-4o-mini", **kwargs) -> 'APIManager':
        """Convenience method to create OpenAI API manager."""
        return cls(provider="openai", model=model, api_key=api_key, **kwargs)
    
    @classmethod  
    def create_anthropic(cls, api_key: str, model: str = "claude-3-sonnet-20240229", **kwargs) -> 'APIManager':
        """Convenience method to create Anthropic API manager."""
        return cls(provider="anthropic", model=model, api_key=api_key, **kwargs)
    
    @classmethod
    def create_google(cls, api_key: str, model: str = "gemini-pro", **kwargs) -> 'APIManager':
        """Convenience method to create Google API manager."""
        return cls(provider="google", model=model, api_key=api_key, **kwargs)