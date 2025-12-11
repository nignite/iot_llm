#!/usr/bin/env python3
"""
Enhanced LLM Integration for Natural Language Queries to IoT Database
Supports multiple LLM providers with knowledge building and fallback mechanisms
"""

import os
import json
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from .domain_mapping import DomainMapper
from .database import get_database_manager
from .config import get_config
from .llm_providers import LLMProviderManager
from .knowledge_system import QueryKnowledgeSystem, EnhancedSchemaAnalyzer
from .isa95_domain import ISA95DomainKnowledge, ISA95QueryEnhancer
import anthropic

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, use system env vars

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedQueryInterface:
    def __init__(self, database_manager=None, llm_config=None):
        """
        Initialize Enhanced Query Interface with multi-LLM support and knowledge building
        
        Args:
            database_manager: Optional DatabaseManager instance. If None, will create based on config.
            llm_config: Configuration for LLM providers
        """
        self.config = get_config()
        self.mapper = DomainMapper()
        
        # Use provided database manager or create one
        if database_manager:
            self.db = database_manager
            self._owns_db = False
        else:
            self.db = get_database_manager()
            self._owns_db = True
            
        # Connect to database
        if not self.db.connect():
            raise ConnectionError("Failed to connect to database")
        
        print(f"✓ Connected to {self.db.get_database_type().upper()} database")
        
        # Initialize LLM provider manager
        default_llm_config = {
            'claude': {
                'api_key': os.getenv('ANTHROPIC_API_KEY'),
                'model': 'claude-3-haiku-20240307'
            },
            'openai': {
                'api_key': os.getenv('OPENAI_API_KEY'),
                'model': 'gpt-4'
            },
            'fallback_order': ['claude', 'openai']
        }
        
        if llm_config:
            default_llm_config.update(llm_config)
        
        self.llm_manager = LLMProviderManager(default_llm_config)
        
        # Initialize knowledge system
        self.knowledge = QueryKnowledgeSystem()
        
        # Initialize schema analyzer
        self.schema_analyzer = EnhancedSchemaAnalyzer(self.db, self.knowledge)
        
        # Initialize ISA-95 domain knowledge
        self.isa95_domain = ISA95DomainKnowledge()
        self.isa95_enhancer = ISA95QueryEnhancer(self.isa95_domain)
        
        # Cache for schema context
        self._schema_context = None
        self._schema_analysis = None
        self._isa95_context = None
        
        print(f"✓ LLM providers initialized: {list(self.llm_manager.providers.keys())}")
        print(f"✓ Current provider: {self.llm_manager.current_provider}")
        print(f"✓ ISA-95 Manufacturing domain knowledge loaded")
        
        # Legacy Claude client for fallback
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key:
            print("WARNING: ANTHROPIC_API_KEY not found. Set environment variable to use Claude API.")
            print("Falling back to rule-based processing.")
            self.use_claude = False
            self.client = None
        else:
            try:
                self.client = anthropic.Anthropic(api_key=api_key)
                self.use_claude = True
                print("✓ Claude API initialized successfully")
            except Exception as e:
                print(f"ERROR: Failed to initialize Claude API: {e}")
                print("Falling back to rule-based processing.")
                self.use_claude = False
                self.client = None
        
    def get_database_schema(self) -> Dict:
        """Get complete database schema for context"""
        schema = {}
        
        # Get all tables using database abstraction
        tables = self.db.get_all_tables()
        
        for table in tables:
            columns = self.db.get_table_schema(table)
            
            # Normalize column info for different database types
            normalized_columns = []
            for col in columns:
                if self.db.get_database_type() == 'sqlite':
                    # SQLite PRAGMA table_info format
                    normalized_columns.append({
                        'name': col.get('name', ''),
                        'type': col.get('type', ''),
                        'notnull': col.get('notnull', 0),
                        'pk': col.get('pk', 0)
                    })
                else:
                    # Oracle all_tab_columns format
                    normalized_columns.append({
                        'name': col.get('column_name', ''),
                        'type': col.get('data_type', ''),
                        'notnull': 1 if col.get('nullable') == 'N' else 0,
                        'pk': 0  # Would need additional query for PK info
                    })
            
            schema[table] = {
                'columns': normalized_columns,
                'domain_names': self.mapper.reverse_lookup_table(table)
            }
        
        return schema
    
    def get_sample_data(self, table: str, limit: int = 3) -> List[Dict]:
        """Get sample data from a table for context"""
        try:
            # Use database abstraction layer
            sql = f"SELECT * FROM {table}"
            if self.db.get_database_type() == 'oracle':
                sql += f" WHERE ROWNUM <= {limit}"
            else:
                sql += f" LIMIT {limit}"
            
            return self.db.execute_query(sql)
        except Exception as e:
            print(f"Warning: Could not get sample data from {table}: {e}")
            return []
    
    def create_claude_prompt(self, query: str) -> str:
        """Create a detailed prompt for Claude to generate SQL"""
        schema = self.get_database_schema()
        
        # Get sample data for context
        sample_data = {}
        for table in schema.keys():
            sample_data[table] = self.get_sample_data(table, 2)
        
        prompt = f"""You are an expert SQL generator for an IoT database. Convert the natural language query into a valid SQLite query.

DATABASE SCHEMA:
"""
        
        for table, info in schema.items():
            columns_info = []
            for col in info['columns']:
                pk_marker = " (PRIMARY KEY)" if col['pk'] else ""
                columns_info.append(f"  - {col['name']} ({col['type']}){pk_marker}")
            
            domain_names = ", ".join(info['domain_names']) if info['domain_names'] else "None"
            
            prompt += f"""
Table: {table}
Domain Names: {domain_names}
Columns:
""" + "\n".join(columns_info)
            
            if sample_data.get(table):
                prompt += f"\nSample Data:\n"
                for i, row in enumerate(sample_data[table], 1):
                    prompt += f"  Row {i}: {row}\n"
            prompt += "\n"
        
        prompt += f"""
DOMAIN MAPPING RULES:
- "signals", "sensor data", "readings" → RepData table
- "alerts", "alarms", "notifications" → AlertLog table  
- "devices", "equipment", "sensors" → DevMap table
- "thresholds", "limits", "boundaries" → ThreshSet table
- "locations", "sites", "facilities" → LocRef table
- "logs", "calculated data", "analytics" → RepItem table

TIME EXPRESSIONS:
- "yesterday" = past 24 hours from yesterday
- "today" = current day
- "last week" = past 7 days
- "last month" = past 30 days
- Convert relative time to DATETIME comparisons

QUERY RULES:
1. Always use proper table aliases for readability
2. Include appropriate WHERE clauses for time filtering
3. Use JOINs when cross-referencing data (e.g., threshold violations)
4. Limit results to 100 for SELECT queries unless specified
5. Use ORDER BY timestamp DESC for time-series data
6. For threshold violations, join RepData with ThreshSet and compare values

NATURAL LANGUAGE QUERY: "{query}"

Generate ONLY the SQL query without explanation. The query should be executable SQLite syntax.

SQL Query:"""
        
        return prompt
    
    def generate_sql_with_claude(self, query: str) -> Tuple[str, bool]:
        """Use Claude API to generate SQL from natural language"""
        if not self.use_claude or not self.client:
            return None, False
        
        try:
            prompt = self.create_claude_prompt(query)
            
            response = self.client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1000,
                temperature=0.1,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            sql = response.content[0].text.strip()
            
            # Clean up the SQL (remove markdown formatting if present)
            sql = re.sub(r'^```sql\s*', '', sql, flags=re.MULTILINE)
            sql = re.sub(r'^```\s*$', '', sql, flags=re.MULTILINE)
            sql = sql.strip()
            
            print(f"🤖 Claude generated SQL: {sql}")
            return sql, True
            
        except Exception as e:
            print(f"❌ Claude API error: {e}")
            return None, False
    
    def parse_time_expressions(self, query: str) -> Tuple[str, Optional[datetime], Optional[datetime]]:
        """Parse time expressions from natural language"""
        query_lower = query.lower()
        now = datetime.now()
        start_date = None
        end_date = None
        modified_query = query
        
        # Time patterns
        time_patterns = {
            r'\blast\s+week\b': (now - timedelta(days=7), now),
            r'\bpast\s+week\b': (now - timedelta(days=7), now),
            r'\bprevious\s+week\b': (now - timedelta(days=7), now),
            r'\byesterday\b': (now - timedelta(days=1), now - timedelta(days=1) + timedelta(hours=23, minutes=59)),
            r'\btoday\b': (now.replace(hour=0, minute=0, second=0), now),
            r'\blast\s+(\d+)\s+days?\b': lambda m: (now - timedelta(days=int(m.group(1))), now),
            r'\blast\s+(\d+)\s+hours?\b': lambda m: (now - timedelta(hours=int(m.group(1))), now),
            r'\blast\s+month\b': (now - timedelta(days=30), now),
            r'\bthis\s+week\b': (now - timedelta(days=now.weekday()), now)
        }
        
        for pattern, time_range in time_patterns.items():
            match = re.search(pattern, query_lower)
            if match:
                if callable(time_range):
                    start_date, end_date = time_range(match)
                else:
                    start_date, end_date = time_range
                modified_query = re.sub(pattern, '', query, flags=re.IGNORECASE).strip()
                break
        
        return modified_query, start_date, end_date
    
    def extract_query_intent(self, query: str) -> Dict:
        """Extract intent and entities from natural language query (fallback method)"""
        query_lower = query.lower()
        intent = {
            'action': None,
            'entity': None,
            'condition': None,
            'aggregation': None,
            'comparison': None,
            'value': None
        }
        
        # Action patterns
        if any(word in query_lower for word in ['which', 'what', 'show', 'find', 'get', 'list']):
            intent['action'] = 'SELECT'
        elif any(word in query_lower for word in ['count', 'how many']):
            intent['action'] = 'COUNT'
        elif any(word in query_lower for word in ['average', 'avg', 'mean']):
            intent['action'] = 'AVG'
        elif any(word in query_lower for word in ['maximum', 'max', 'highest']):
            intent['action'] = 'MAX'
        elif any(word in query_lower for word in ['minimum', 'min', 'lowest']):
            intent['action'] = 'MIN'
        
        # Entity extraction
        for domain_term, table in self.mapper.table_mappings.items():
            if domain_term in query_lower:
                intent['entity'] = table
                break
        
        # Condition patterns
        if any(word in query_lower for word in ['crossed', 'exceeded', 'violated', 'breached']):
            intent['condition'] = 'threshold_exceeded'
        elif any(word in query_lower for word in ['offline', 'disconnected', 'down']):
            intent['condition'] = 'status_offline'
        elif any(word in query_lower for word in ['online', 'connected', 'active']):
            intent['condition'] = 'status_online'
        
        # Comparison and values
        comparison_patterns = [
            (r'>\s*(\d+(?:\.\d+)?)', 'GT'),
            (r'<\s*(\d+(?:\.\d+)?)', 'LT'),
            (r'=\s*(\d+(?:\.\d+)?)', 'EQ'),
            (r'above\s+(\d+(?:\.\d+)?)', 'GT'),
            (r'below\s+(\d+(?:\.\d+)?)', 'LT'),
            (r'over\s+(\d+(?:\.\d+)?)', 'GT'),
            (r'under\s+(\d+(?:\.\d+)?)', 'LT')
        ]
        
        for pattern, comp_type in comparison_patterns:
            match = re.search(pattern, query_lower)
            if match:
                intent['comparison'] = comp_type
                intent['value'] = float(match.group(1))
                break
        
        return intent
    
    def build_sql_query_fallback(self, query: str, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> Tuple[str, List]:
        """Build SQL query using rule-based approach (fallback)"""
        intent = self.extract_query_intent(query)
        params = []
        
        # Default to signal data if no entity detected
        if not intent['entity']:
            intent['entity'] = 'RepData'
        
        # Build SELECT clause
        if intent['action'] == 'COUNT':
            select_clause = "SELECT COUNT(*)"
        elif intent['action'] in ['AVG', 'MAX', 'MIN']:
            select_clause = f"SELECT {intent['action']}(value)"
        else:
            if intent['entity'] == 'RepData':
                select_clause = "SELECT device_id, sensor_type, value, unit, timestamp"
            elif intent['entity'] == 'AlertLog':
                select_clause = "SELECT device_id, sensor_type, alert_type, actual_value, threshold_value, timestamp"
            elif intent['entity'] == 'DevMap':
                select_clause = "SELECT device_id, device_name, status, location"
            else:
                select_clause = "SELECT *"
        
        # Build FROM clause
        from_clause = f"FROM {intent['entity']}"
        
        # Build WHERE clause
        where_conditions = []
        
        # Time conditions
        if start_date and end_date:
            if intent['condition'] == 'threshold_exceeded' and intent['entity'] == 'RepData':
                where_conditions.append("r.timestamp BETWEEN ? AND ?")
            else:
                where_conditions.append("timestamp BETWEEN ? AND ?")
            params.extend([start_date.isoformat(), end_date.isoformat()])
        
        # Specific conditions based on intent
        if intent['condition'] == 'threshold_exceeded':
            if intent['entity'] == 'RepData':
                # Join with thresholds to find exceeded values
                from_clause = f"FROM {intent['entity']} r JOIN ThreshSet t ON r.sensor_type = t.sensor_type"
                select_clause = "SELECT r.device_id, r.sensor_type, r.value, r.unit, r.timestamp"
                where_conditions.append("(r.value > t.max_value OR r.value < t.min_value)")
            elif intent['entity'] == 'AlertLog':
                where_conditions.append("alert_type = 'threshold_exceeded'")
        elif intent['condition'] == 'status_offline':
            where_conditions.append("status = 'offline'")
        elif intent['condition'] == 'status_online':
            where_conditions.append("status = 'online'")
        
        # Value comparisons
        if intent['comparison'] and intent['value'] is not None:
            if intent['comparison'] == 'GT':
                where_conditions.append("value > ?")
            elif intent['comparison'] == 'LT':
                where_conditions.append("value < ?")
            elif intent['comparison'] == 'EQ':
                where_conditions.append("value = ?")
            params.append(intent['value'])
        
        # Construct final query
        sql = select_clause + " " + from_clause
        if where_conditions:
            sql += " WHERE " + " AND ".join(where_conditions)
        
        # Add ORDER BY for better results
        if intent['entity'] == 'RepData':
            sql += " ORDER BY timestamp DESC"
        elif intent['entity'] == 'AlertLog':
            sql += " ORDER BY timestamp DESC"
        
        # Add LIMIT for large result sets
        if intent['action'] == 'SELECT':
            sql += " LIMIT 100"
        
        return sql, params
    
    def execute_natural_language_query(self, query: str) -> Dict:
        """Execute a natural language query against the IoT database with enhanced LLM support"""
        start_time = datetime.now()
        
        try:
            print(f"🔍 Processing query: {query}")
            
            # Get similar examples from knowledge system
            examples = self.knowledge.get_similar_examples(query, limit=3)
            
            # Build enhanced schema context
            context = self._get_enhanced_schema_context()
            
            sql = None
            params = []
            provider_info = None
            
            # Try LLM providers first with ISA-95 enhancement
            try:
                # Apply ISA-95 manufacturing term mapping
                enhanced_query = self.isa95_domain.map_manufacturing_terms(query)
                
                sql, provider_info = self.llm_manager.generate_sql(enhanced_query, context, examples)
                print(f"🤖 {provider_info['provider'].title()} generated SQL: {sql}")
                
                # Check if SQL contains parameters or is just explanatory text
                if not self._is_valid_sql(sql):
                    raise Exception("Generated response is not valid SQL")
                    
            except Exception as e:
                print(f"❌ LLM providers failed: {e}")
                print("📝 Using fallback rule-based processing...")
                # Parse time expressions for fallback
                cleaned_query, start_date, end_date = self.parse_time_expressions(query)
                # Build SQL query using fallback method
                sql, params = self.build_sql_query_fallback(cleaned_query, start_date, end_date)
                provider_info = {'provider': 'fallback', 'model': 'rule-based'}
            
            print(f"🗃️ Executing SQL: {sql}")
            if params:
                print(f"📊 Parameters: {params}")
            
            # Execute query using database abstraction
            if params:
                formatted_results = self.db.execute_query(sql, params)
            else:
                formatted_results = self.db.execute_query(sql)
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            print(f"✅ Found {len(formatted_results)} results")
            
            # Record successful query in knowledge system
            self.knowledge.record_query(
                natural_query=query,
                generated_sql=sql,
                execution_success=True,
                execution_time_ms=execution_time,
                result_count=len(formatted_results),
                provider_info=provider_info
            )
            
            # Generate ISA-95 manufacturing insights
            query_result = {
                'success': True,
                'query': query,
                'sql': sql,
                'params': params if params else [],
                'results': formatted_results,
                'count': len(formatted_results),
                'provider_used': provider_info.get('provider', 'unknown') if provider_info else 'unknown',
                'execution_time_ms': execution_time,
                'time_range': None  # Could be enhanced to extract from response
            }
            
            # Add manufacturing insights
            manufacturing_insights = self.isa95_enhancer.suggest_manufacturing_insights(query_result)
            if manufacturing_insights:
                query_result['manufacturing_insights'] = manufacturing_insights
                print(f"🏭 Manufacturing Insights: {'; '.join(manufacturing_insights[:2])}")  # Show first 2 insights
            
            return query_result
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            error_msg = str(e)
            print(f"❌ Error: {error_msg}")
            
            # Record failed query for learning
            if 'sql' in locals() and sql:
                self.knowledge.record_query(
                    natural_query=query,
                    generated_sql=sql,
                    execution_success=False,
                    execution_time_ms=execution_time,
                    provider_info=provider_info,
                    error_message=error_msg
                )
            
            return {
                'success': False,
                'query': query,
                'error': error_msg,
                'sql': sql if 'sql' in locals() else None,
                'provider_used': provider_info.get('provider', 'unknown') if provider_info else 'unknown'
            }
    
    def get_sample_queries(self) -> List[str]:
        """Return sample natural language queries for testing with ISA-95 manufacturing focus"""
        basic_queries = [
            "Which signals crossed the value limits last week?",
            "Show me all alerts from yesterday", 
            "What devices are currently offline?",
            "Find temperature readings above 30 degrees",
            "Show me all critical alerts",
            "What's the current status of all temperature sensors?"
        ]
        
        # Add ISA-95 manufacturing queries
        try:
            schema = self.get_database_schema()
            isa95_queries = self.isa95_domain.suggest_isa95_queries(schema)
            return basic_queries + isa95_queries[:5]  # Combine basic and ISA-95 specific queries
        except:
            return basic_queries
    
    def get_isa95_context(self) -> Dict[str, Any]:
        """Get ISA-95 manufacturing domain context"""
        return self.isa95_domain.get_manufacturing_context()
    
    def get_manufacturing_metrics(self) -> Dict[str, Any]:
        """Get available manufacturing metrics and KPIs"""
        return self.isa95_domain.common_metrics
    
    def _get_enhanced_schema_context(self) -> str:
        """Build enhanced schema context with knowledge insights and ISA-95 domain knowledge"""
        if self._schema_context is None:
            # Get basic schema
            schema = self.get_database_schema()
            
            # Get domain vocabulary
            vocabulary = self.knowledge.get_domain_vocabulary()
            
            # Get schema insights
            insights = self.knowledge.get_schema_insights()
            
            context_parts = [
                "ISA-95 Manufacturing IoT Database Schema with AI-Enhanced Insights:",
                "",
                "=== DATABASE SCHEMA ==="
            ]
            
            for table, info in schema.items():
                context_parts.append(f"\nTable: {table}")
                context_parts.append(f"Domain Names: {', '.join(info['domain_names'])}")
                
                for col in info['columns']:
                    col_type = f"{col['type']}"
                    if col['pk']:
                        col_type += " (Primary Key)"
                    context_parts.append(f"  - {col['name']}: {col_type}")
                
                # Add insights if available
                if table in insights.get('common_filters', {}):
                    filters = insights['common_filters'][table]
                    if filters:
                        context_parts.append(f"  Common filters: {', '.join([f['description'] for f in filters[:3]])}")
            
            # Add learned domain vocabulary
            if vocabulary:
                context_parts.extend([
                    "",
                    "=== LEARNED VOCABULARY ===",
                    ""
                ])
                for term, mapping in list(vocabulary.items())[:10]:  # Top 10 terms
                    context_parts.append(f"  '{term}' → {mapping}")
            
            # Add ISA-95 manufacturing context
            basic_context = "\n".join(context_parts)
            enhanced_context = self.isa95_domain.enhance_query_context("", basic_context)
            
            # Add ISA-95 suggested queries
            isa95_suggestions = self.isa95_domain.suggest_isa95_queries(schema)
            if isa95_suggestions:
                context_parts.extend([
                    "",
                    "=== ISA-95 MANUFACTURING QUERY EXAMPLES ===",
                    ""
                ])
                for suggestion in isa95_suggestions[:5]:  # Top 5 suggestions
                    context_parts.append(f"  • {suggestion}")
            
            self._schema_context = enhanced_context
        
        return self._schema_context
    
    def _is_valid_sql(self, text: str) -> bool:
        """Check if the generated text is valid SQL"""
        if not text or not isinstance(text, str):
            return False
        
        # Basic SQL validation
        text_upper = text.upper().strip()
        
        # Must start with a SQL command
        sql_commands = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH']
        if not any(text_upper.startswith(cmd) for cmd in sql_commands):
            return False
        
        # Should not contain explanation phrases
        explanation_phrases = [
            'THE PROVIDED', 'I CANNOT', 'PLEASE NOTE', 'HOWEVER',
            'UNFORTUNATELY', 'SORRY', 'WITHOUT A CLEAR'
        ]
        if any(phrase in text_upper for phrase in explanation_phrases):
            return False
        
        return True
    
    def switch_llm_provider(self, provider_name: str) -> bool:
        """Switch to a specific LLM provider"""
        return self.llm_manager.switch_provider(provider_name)
    
    def get_provider_status(self) -> Dict[str, Any]:
        """Get status of all LLM providers"""
        return self.llm_manager.get_provider_status()
    
    def get_knowledge_stats(self, days_back: int = 30) -> Dict[str, Any]:
        """Get knowledge system statistics"""
        return self.knowledge.get_success_stats(days_back)
    
    def get_schema_analysis(self) -> Dict[str, Any]:
        """Get comprehensive schema analysis"""
        if self._schema_analysis is None:
            self._schema_analysis = self.schema_analyzer.analyze_schema()
        return self._schema_analysis
    
    def close(self):
        """Close database and knowledge system connections"""
        if self._owns_db and self.db:
            self.db.disconnect()
        
        if self.knowledge:
            self.knowledge.close()


# Maintain backward compatibility
class ClaudeQueryInterface(EnhancedQueryInterface):
    """Legacy class name for backward compatibility"""
    def __init__(self, database_manager=None):
        print("ℹ️  Using legacy ClaudeQueryInterface. Consider upgrading to EnhancedQueryInterface.")
        super().__init__(database_manager)

def main():
    """Demo the Claude Query Interface"""
    interface = ClaudeQueryInterface()
    
    print("Claude IoT Database Query Interface")
    print("=" * 50)
    
    # Show sample queries
    print("\nSample Queries:")
    for i, sample in enumerate(interface.get_sample_queries(), 1):
        print(f"{i:2}. {sample}")
    
    print("\nDatabase Schema Summary:")
    schema = interface.get_database_schema()
    for table_name, table_info in schema.items():
        domain_names = ', '.join(table_info['domain_names'])
        print(f"{table_name:12} -> Domain names: {domain_names}")
    
    # Test a sample query
    print("\n" + "=" * 50)
    print("Testing Sample Query:")
    test_query = "Which signals crossed the value limits last week?"
    print(f"Query: {test_query}")
    
    result = interface.execute_natural_language_query(test_query)
    
    if result['success']:
        print(f"\nGenerated SQL: {result['sql']}")
        if result['params']:
            print(f"Parameters: {result['params']}")
        print(f"Used Claude API: {result['used_claude']}")
        print(f"Results found: {result['count']}")
        
        if result['results'][:3]:  # Show first 3 results
            print("\nSample Results:")
            for i, row in enumerate(result['results'][:3], 1):
                print(f"{i}. {row}")
    else:
        print(f"Error: {result['error']}")
    
    interface.close()

if __name__ == "__main__":
    main()