"""
Multi-LLM Provider System for IoT Database Query Interface
Supports multiple AI providers with fallback mechanisms and knowledge building
"""

import os
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import anthropic

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LLMProvider(ABC):
    """Abstract base class for LLM providers"""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.is_available = True
        self.last_error = None
    
    @abstractmethod
    def generate_sql(self, query: str, context: str, examples: List[Dict] = None) -> Tuple[str, Dict]:
        """
        Generate SQL from natural language query
        
        Args:
            query: Natural language query
            context: Database schema and metadata
            examples: List of successful query examples
            
        Returns:
            Tuple of (generated_sql, metadata)
        """
        pass
    
    @abstractmethod
    def is_healthy(self) -> bool:
        """Check if provider is healthy and available"""
        pass
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the model being used"""
        return {
            'provider': self.name,
            'available': self.is_available,
            'last_error': self.last_error,
            'config': self.config
        }


class ClaudeProvider(LLMProvider):
    """Anthropic Claude provider"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("claude", config)
        self.api_key = self.config.get('api_key') or os.getenv('ANTHROPIC_API_KEY')
        self.model = self.config.get('model', 'claude-3-haiku-20240307')
        self.max_tokens = self.config.get('max_tokens', 1000)
        
        if not self.api_key:
            self.is_available = False
            self.last_error = "No API key provided"
            logger.warning("Claude provider: No API key found")
        else:
            try:
                self.client = anthropic.Anthropic(api_key=self.api_key)
                logger.info("✓ Claude provider initialized successfully")
            except Exception as e:
                self.is_available = False
                self.last_error = str(e)
                logger.error(f"Claude provider initialization failed: {e}")
    
    def generate_sql(self, query: str, context: str, examples: List[Dict] = None) -> Tuple[str, Dict]:
        """Generate SQL using Claude API"""
        if not self.is_available:
            raise Exception(f"Claude provider not available: {self.last_error}")
        
        try:
            # Build prompt with context and examples
            prompt = self._build_prompt(query, context, examples)
            
            # Make API call
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            sql = response.content[0].text.strip()
            
            # Extract SQL if wrapped in markdown
            if "```sql" in sql:
                sql = sql.split("```sql")[1].split("```")[0].strip()
            elif "```" in sql:
                sql = sql.split("```")[1].split("```")[0].strip()
            
            metadata = {
                'provider': self.name,
                'model': self.model,
                'tokens_used': response.usage.input_tokens + response.usage.output_tokens,
                'confidence': 'high',  # Claude typically produces high quality results
                'timestamp': datetime.now().isoformat()
            }
            
            return sql, metadata
            
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"Claude API error: {e}")
            raise e
    
    def is_healthy(self) -> bool:
        """Check Claude API health"""
        if not self.is_available:
            return False
        
        try:
            # Simple test call
            response = self.client.messages.create(
                model=self.model,
                max_tokens=10,
                messages=[{"role": "user", "content": "Hello"}]
            )
            return True
        except Exception as e:
            self.last_error = str(e)
            return False
    
    def _build_prompt(self, query: str, context: str, examples: List[Dict] = None) -> str:
        """Build prompt for Claude with context and examples"""
        prompt_parts = [
            "You are an expert SQL developer for an IoT database system.",
            "Convert natural language queries to SQL using the provided schema.",
            "",
            "Database Schema:",
            context,
            ""
        ]
        
        # Add examples if provided
        if examples:
            prompt_parts.extend([
                "Here are some successful query examples:",
                ""
            ])
            for example in examples[-3:]:  # Use last 3 examples
                prompt_parts.extend([
                    f"Natural Language: {example['query']}",
                    f"SQL: {example['sql']}",
                    ""
                ])
        
        prompt_parts.extend([
            "Rules:",
            "- Generate only valid SQL queries",
            "- Use table aliases for readability",
            "- Include appropriate LIMIT clauses",
            "- Handle time/date queries carefully",
            "- Return only the SQL query, no explanations",
            "",
            f"Query: {query}",
            "",
            "SQL:"
        ])
        
        return "\n".join(prompt_parts)


class OpenAIProvider(LLMProvider):
    """OpenAI GPT provider"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("openai", config)
        self.api_key = self.config.get('api_key') or os.getenv('OPENAI_API_KEY')
        self.model = self.config.get('model', 'gpt-4')
        self.max_tokens = self.config.get('max_tokens', 1000)
        
        if not self.api_key:
            self.is_available = False
            self.last_error = "No API key provided"
            logger.warning("OpenAI provider: No API key found")
        else:
            try:
                import openai
                self.client = openai.OpenAI(api_key=self.api_key)
                logger.info("✓ OpenAI provider initialized successfully")
            except ImportError:
                self.is_available = False
                self.last_error = "openai package not installed"
                logger.error("OpenAI provider: openai package not installed")
            except Exception as e:
                self.is_available = False
                self.last_error = str(e)
                logger.error(f"OpenAI provider initialization failed: {e}")
    
    def generate_sql(self, query: str, context: str, examples: List[Dict] = None) -> Tuple[str, Dict]:
        """Generate SQL using OpenAI API"""
        if not self.is_available:
            raise Exception(f"OpenAI provider not available: {self.last_error}")
        
        try:
            # Build prompt
            prompt = self._build_prompt(query, context, examples)
            
            # Make API call
            response = self.client.chat.completions.create(
                model=self.model,
                max_tokens=self.max_tokens,
                messages=[
                    {"role": "system", "content": "You are an expert SQL developer for IoT database systems."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            sql = response.choices[0].message.content.strip()
            
            # Extract SQL if wrapped in markdown
            if "```sql" in sql:
                sql = sql.split("```sql")[1].split("```")[0].strip()
            elif "```" in sql:
                sql = sql.split("```")[1].split("```")[0].strip()
            
            metadata = {
                'provider': self.name,
                'model': self.model,
                'tokens_used': response.usage.total_tokens,
                'confidence': 'high',
                'timestamp': datetime.now().isoformat()
            }
            
            return sql, metadata
            
        except Exception as e:
            self.last_error = str(e)
            logger.error(f"OpenAI API error: {e}")
            raise e
    
    def is_healthy(self) -> bool:
        """Check OpenAI API health"""
        if not self.is_available:
            return False
        
        try:
            # Simple test call
            response = self.client.chat.completions.create(
                model=self.model,
                max_tokens=5,
                messages=[{"role": "user", "content": "Hello"}]
            )
            return True
        except Exception as e:
            self.last_error = str(e)
            return False
    
    def _build_prompt(self, query: str, context: str, examples: List[Dict] = None) -> str:
        """Build prompt for OpenAI with context and examples"""
        prompt_parts = [
            "Convert natural language queries to SQL using the provided IoT database schema.",
            "",
            "Database Schema:",
            context,
            ""
        ]
        
        # Add examples if provided
        if examples:
            prompt_parts.extend([
                "Successful query examples:",
                ""
            ])
            for example in examples[-3:]:  # Use last 3 examples
                prompt_parts.extend([
                    f"Q: {example['query']}",
                    f"A: {example['sql']}",
                    ""
                ])
        
        prompt_parts.extend([
            "Generate only valid SQL. Use table aliases and appropriate LIMITs.",
            "",
            f"Query: {query}",
            ""
        ])
        
        return "\n".join(prompt_parts)


class LLMProviderManager:
    """Manages multiple LLM providers with fallback logic"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.providers: Dict[str, LLMProvider] = {}
        self.provider_order = self.config.get('fallback_order', ['claude', 'openai'])
        self.current_provider = None
        
        # Initialize providers
        self._initialize_providers()
    
    def _initialize_providers(self):
        """Initialize all available providers"""
        
        # Claude provider
        claude_config = self.config.get('claude', {})
        claude = ClaudeProvider(claude_config)
        if claude.is_available:
            self.providers['claude'] = claude
        
        # OpenAI provider
        openai_config = self.config.get('openai', {})
        openai_provider = OpenAIProvider(openai_config)
        if openai_provider.is_available:
            self.providers['openai'] = openai_provider
        
        # Set current provider to first available
        for provider_name in self.provider_order:
            if provider_name in self.providers:
                self.current_provider = provider_name
                break
        
        logger.info(f"Available providers: {list(self.providers.keys())}")
        logger.info(f"Current provider: {self.current_provider}")
    
    def generate_sql(self, query: str, context: str, examples: List[Dict] = None) -> Tuple[str, Dict]:
        """Generate SQL using the best available provider"""
        
        # Try current provider first
        if self.current_provider and self.current_provider in self.providers:
            try:
                return self.providers[self.current_provider].generate_sql(query, context, examples)
            except Exception as e:
                logger.warning(f"Primary provider {self.current_provider} failed: {e}")
        
        # Try fallback providers
        for provider_name in self.provider_order:
            if provider_name in self.providers and provider_name != self.current_provider:
                try:
                    logger.info(f"Trying fallback provider: {provider_name}")
                    result = self.providers[provider_name].generate_sql(query, context, examples)
                    
                    # Update current provider if successful
                    self.current_provider = provider_name
                    return result
                    
                except Exception as e:
                    logger.warning(f"Fallback provider {provider_name} failed: {e}")
                    continue
        
        # All providers failed
        raise Exception("All LLM providers failed to generate SQL")
    
    def switch_provider(self, provider_name: str) -> bool:
        """Manually switch to a specific provider"""
        if provider_name in self.providers:
            if self.providers[provider_name].is_healthy():
                self.current_provider = provider_name
                logger.info(f"Switched to provider: {provider_name}")
                return True
            else:
                logger.warning(f"Provider {provider_name} is not healthy")
                return False
        else:
            logger.error(f"Provider {provider_name} not available")
            return False
    
    def get_provider_status(self) -> Dict[str, Any]:
        """Get status of all providers"""
        status = {
            'current_provider': self.current_provider,
            'available_providers': list(self.providers.keys()),
            'providers': {}
        }
        
        for name, provider in self.providers.items():
            status['providers'][name] = provider.get_model_info()
        
        return status
    
    def health_check(self) -> Dict[str, bool]:
        """Check health of all providers"""
        health = {}
        for name, provider in self.providers.items():
            health[name] = provider.is_healthy()
        return health