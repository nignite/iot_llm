"""
Database Knowledge System for IoT Query Interface
Builds up knowledge about successful queries and database patterns
"""

import sqlite3
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import re
from collections import Counter

logger = logging.getLogger(__name__)

class QueryKnowledgeSystem:
    """
    Manages query history and builds database knowledge for better SQL generation
    """
    
    def __init__(self, knowledge_db_path: str = "query_knowledge.db"):
        self.db_path = knowledge_db_path
        self.conn = None
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the knowledge database with required tables"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            
            # Create tables
            self.conn.executescript("""
                -- Query history table
                CREATE TABLE IF NOT EXISTS query_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    natural_query TEXT NOT NULL,
                    generated_sql TEXT NOT NULL,
                    execution_success BOOLEAN NOT NULL,
                    execution_time_ms REAL,
                    result_count INTEGER,
                    provider_used TEXT,
                    model_used TEXT,
                    confidence_score REAL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    feedback_score INTEGER,  -- User feedback (-1, 0, 1)
                    error_message TEXT,
                    query_hash TEXT,  -- Hash for deduplication
                    UNIQUE(query_hash, generated_sql)
                );
                
                -- Schema knowledge table
                CREATE TABLE IF NOT EXISTS schema_knowledge (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    column_name TEXT,
                    knowledge_type TEXT NOT NULL, -- 'description', 'usage_pattern', 'common_filter'
                    knowledge_data TEXT NOT NULL, -- JSON data
                    confidence_score REAL DEFAULT 0.5,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Domain vocabulary table
                CREATE TABLE IF NOT EXISTS domain_vocabulary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    term TEXT NOT NULL UNIQUE,
                    sql_mapping TEXT NOT NULL,
                    usage_count INTEGER DEFAULT 1,
                    confidence REAL DEFAULT 0.5,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Query patterns table
                CREATE TABLE IF NOT EXISTS query_patterns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pattern_type TEXT NOT NULL, -- 'time_filter', 'aggregation', 'join_pattern'
                    pattern_description TEXT,
                    example_nl TEXT,
                    example_sql TEXT,
                    usage_count INTEGER DEFAULT 1,
                    success_rate REAL DEFAULT 1.0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            self.conn.commit()
            logger.info("Knowledge system database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize knowledge database: {e}")
            raise e
    
    def record_query(self, 
                    natural_query: str, 
                    generated_sql: str, 
                    execution_success: bool,
                    execution_time_ms: float = None,
                    result_count: int = None,
                    provider_info: Dict = None,
                    error_message: str = None) -> int:
        """
        Record a query execution for learning
        
        Returns:
            The ID of the recorded query
        """
        try:
            query_hash = self._hash_query(natural_query)
            provider_used = provider_info.get('provider') if provider_info else None
            model_used = provider_info.get('model') if provider_info else None
            confidence = provider_info.get('confidence', 0.5) if provider_info else 0.5
            
            cursor = self.conn.execute("""
                INSERT OR REPLACE INTO query_history 
                (natural_query, generated_sql, execution_success, execution_time_ms, 
                 result_count, provider_used, model_used, confidence_score, 
                 error_message, query_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (natural_query, generated_sql, execution_success, execution_time_ms,
                  result_count, provider_used, model_used, confidence, 
                  error_message, query_hash))
            
            self.conn.commit()
            query_id = cursor.lastrowid
            
            # Learn from successful queries
            if execution_success:
                self._learn_from_successful_query(natural_query, generated_sql)
            
            return query_id
            
        except Exception as e:
            logger.error(f"Failed to record query: {e}")
            return -1
    
    def get_similar_examples(self, natural_query: str, limit: int = 5) -> List[Dict]:
        """
        Get similar successful queries to use as examples
        """
        try:
            # Simple keyword matching for now - could be enhanced with embeddings
            keywords = self._extract_keywords(natural_query)
            
            # Build similarity query
            conditions = []
            params = []
            for keyword in keywords:
                conditions.append("natural_query LIKE ?")
                params.append(f"%{keyword}%")
            
            if not conditions:
                where_clause = "execution_success = 1"
                order_clause = "timestamp DESC"
            else:
                where_clause = f"execution_success = 1 AND ({' OR '.join(conditions)})"
                order_clause = "confidence_score DESC, timestamp DESC"
            
            params.append(limit)
            
            cursor = self.conn.execute(f"""
                SELECT natural_query, generated_sql, confidence_score, result_count
                FROM query_history 
                WHERE {where_clause}
                ORDER BY {order_clause}
                LIMIT ?
            """, params)
            
            examples = []
            for row in cursor.fetchall():
                examples.append({
                    'query': row['natural_query'],
                    'sql': row['generated_sql'],
                    'confidence': row['confidence_score'],
                    'result_count': row['result_count']
                })
            
            return examples
            
        except Exception as e:
            logger.error(f"Failed to get similar examples: {e}")
            return []
    
    def get_schema_insights(self, table_name: str = None) -> Dict[str, Any]:
        """
        Get accumulated knowledge about database schema
        """
        try:
            insights = {
                'common_filters': {},
                'usage_patterns': {},
                'column_descriptions': {}
            }
            
            where_clause = "WHERE table_name = ?" if table_name else ""
            params = [table_name] if table_name else []
            
            cursor = self.conn.execute(f"""
                SELECT table_name, column_name, knowledge_type, knowledge_data, confidence_score
                FROM schema_knowledge 
                {where_clause}
                ORDER BY confidence_score DESC
            """, params)
            
            for row in cursor.fetchall():
                table = row['table_name']
                if table not in insights['common_filters']:
                    insights['common_filters'][table] = []
                    insights['usage_patterns'][table] = []
                    insights['column_descriptions'][table] = {}
                
                knowledge_data = json.loads(row['knowledge_data'])
                
                if row['knowledge_type'] == 'common_filter':
                    insights['common_filters'][table].append(knowledge_data)
                elif row['knowledge_type'] == 'usage_pattern':
                    insights['usage_patterns'][table].append(knowledge_data)
                elif row['knowledge_type'] == 'description':
                    column = row['column_name']
                    insights['column_descriptions'][table][column] = knowledge_data
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to get schema insights: {e}")
            return {}
    
    def get_domain_vocabulary(self) -> Dict[str, str]:
        """
        Get learned domain vocabulary mappings
        """
        try:
            cursor = self.conn.execute("""
                SELECT term, sql_mapping, confidence 
                FROM domain_vocabulary 
                WHERE confidence > 0.3
                ORDER BY usage_count DESC, confidence DESC
            """)
            
            vocabulary = {}
            for row in cursor.fetchall():
                vocabulary[row['term']] = row['sql_mapping']
            
            return vocabulary
            
        except Exception as e:
            logger.error(f"Failed to get domain vocabulary: {e}")
            return {}
    
    def get_success_stats(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Get query success statistics
        """
        try:
            since_date = (datetime.now() - timedelta(days=days_back)).isoformat()
            
            cursor = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_queries,
                    SUM(CASE WHEN execution_success THEN 1 ELSE 0 END) as successful_queries,
                    AVG(execution_time_ms) as avg_execution_time,
                    COUNT(DISTINCT provider_used) as providers_used
                FROM query_history 
                WHERE timestamp > ?
            """, (since_date,))
            
            row = cursor.fetchone()
            
            stats = {
                'total_queries': row['total_queries'],
                'successful_queries': row['successful_queries'],
                'success_rate': row['successful_queries'] / max(row['total_queries'], 1),
                'avg_execution_time_ms': row['avg_execution_time'],
                'providers_used': row['providers_used']
            }
            
            # Provider performance
            cursor = self.conn.execute("""
                SELECT 
                    provider_used,
                    COUNT(*) as queries,
                    AVG(CASE WHEN execution_success THEN 1.0 ELSE 0.0 END) as success_rate
                FROM query_history 
                WHERE timestamp > ? AND provider_used IS NOT NULL
                GROUP BY provider_used
            """, (since_date,))
            
            stats['provider_performance'] = {}
            for row in cursor.fetchall():
                stats['provider_performance'][row['provider_used']] = {
                    'queries': row['queries'],
                    'success_rate': row['success_rate']
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get success stats: {e}")
            return {}
    
    def _learn_from_successful_query(self, natural_query: str, generated_sql: str):
        """
        Extract patterns and knowledge from successful queries
        """
        try:
            # Extract and learn domain vocabulary
            self._learn_vocabulary(natural_query, generated_sql)
            
            # Extract and learn query patterns
            self._learn_query_patterns(natural_query, generated_sql)
            
        except Exception as e:
            logger.error(f"Failed to learn from query: {e}")
    
    def _learn_vocabulary(self, natural_query: str, generated_sql: str):
        """
        Learn domain-specific vocabulary from successful queries
        """
        # Extract terms from natural query
        nl_terms = re.findall(r'\b[a-zA-Z]{3,}\b', natural_query.lower())
        
        # Extract SQL elements
        sql_tables = re.findall(r'\bFROM\s+(\w+)|JOIN\s+(\w+)', generated_sql, re.IGNORECASE)
        sql_columns = re.findall(r'SELECT\s+([^FROM]+)', generated_sql, re.IGNORECASE)
        
        # Simple mapping learning - this could be enhanced
        for term in nl_terms:
            if term in ['show', 'get', 'find', 'list', 'what', 'which', 'where']:
                continue
                
            # Try to map terms to SQL elements
            for table_match in sql_tables:
                table = table_match[0] or table_match[1]
                if table and self._similarity_score(term, table) > 0.7:
                    self._update_vocabulary(term, table, 'table')
    
    def _learn_query_patterns(self, natural_query: str, generated_sql: str):
        """
        Learn common query patterns
        """
        # Detect time-based queries
        if re.search(r'\b(today|yesterday|last\s+week|this\s+month)\b', natural_query, re.IGNORECASE):
            pattern_type = 'time_filter'
            time_pattern = re.search(r'WHERE.*timestamp.*>=.*DATE', generated_sql, re.IGNORECASE)
            if time_pattern:
                self._update_query_pattern(pattern_type, natural_query, generated_sql)
        
        # Detect aggregation queries
        if re.search(r'\b(count|sum|average|max|min|how\s+many)\b', natural_query, re.IGNORECASE):
            pattern_type = 'aggregation'
            if re.search(r'\b(COUNT|SUM|AVG|MAX|MIN)\b', generated_sql, re.IGNORECASE):
                self._update_query_pattern(pattern_type, natural_query, generated_sql)
    
    def _update_vocabulary(self, term: str, sql_mapping: str, mapping_type: str):
        """Update domain vocabulary with new mapping"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO domain_vocabulary (term, sql_mapping, usage_count, confidence)
                VALUES (?, ?, 
                    COALESCE((SELECT usage_count + 1 FROM domain_vocabulary WHERE term = ?), 1),
                    COALESCE((SELECT confidence + 0.1 FROM domain_vocabulary WHERE term = ?), 0.6)
                )
            """, (term, sql_mapping, term, term))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to update vocabulary: {e}")
    
    def _update_query_pattern(self, pattern_type: str, natural_query: str, generated_sql: str):
        """Update query pattern knowledge"""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO query_patterns 
                (pattern_type, example_nl, example_sql, usage_count, success_rate)
                VALUES (?, ?, ?,
                    COALESCE((SELECT usage_count + 1 FROM query_patterns 
                             WHERE pattern_type = ? AND example_nl = ?), 1),
                    1.0
                )
            """, (pattern_type, natural_query, generated_sql, pattern_type, natural_query))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to update query pattern: {e}")
    
    def _hash_query(self, query: str) -> str:
        """Generate hash for query deduplication"""
        import hashlib
        # Normalize query for consistent hashing
        normalized = re.sub(r'\s+', ' ', query.lower().strip())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _extract_keywords(self, query: str) -> List[str]:
        """Extract meaningful keywords from natural language query"""
        # Simple keyword extraction - could be enhanced with NLP
        stop_words = {'show', 'me', 'get', 'find', 'what', 'which', 'where', 'the', 'a', 'an', 'and', 'or'}
        words = re.findall(r'\b[a-zA-Z]{3,}\b', query.lower())
        return [w for w in words if w not in stop_words]
    
    def _similarity_score(self, term1: str, term2: str) -> float:
        """Simple similarity scoring between terms"""
        # Basic edit distance similarity - could use more sophisticated methods
        from difflib import SequenceMatcher
        return SequenceMatcher(None, term1.lower(), term2.lower()).ratio()
    
    def close(self):
        """Close the knowledge database connection"""
        if self.conn:
            self.conn.close()
    
    def __del__(self):
        """Cleanup on destruction"""
        self.close()


class EnhancedSchemaAnalyzer:
    """
    Analyzes database schema to build rich metadata for better SQL generation
    """
    
    def __init__(self, database_manager, knowledge_system: QueryKnowledgeSystem):
        self.db = database_manager
        self.knowledge = knowledge_system
    
    def analyze_schema(self) -> Dict[str, Any]:
        """
        Perform comprehensive schema analysis
        """
        analysis = {
            'tables': {},
            'relationships': [],
            'common_patterns': {},
            'data_insights': {}
        }
        
        try:
            tables = self.db.get_all_tables()
            
            for table in tables:
                analysis['tables'][table] = self._analyze_table(table)
            
            # Detect relationships
            analysis['relationships'] = self._detect_relationships(tables)
            
            # Analyze data patterns
            analysis['data_insights'] = self._analyze_data_patterns(tables)
            
        except Exception as e:
            logger.error(f"Schema analysis failed: {e}")
        
        return analysis
    
    def _analyze_table(self, table_name: str) -> Dict[str, Any]:
        """Analyze individual table structure and content"""
        table_info = {
            'columns': {},
            'sample_data': [],
            'row_count': 0,
            'key_columns': []
        }
        
        try:
            # Get column information
            columns = self.db.get_table_schema(table_name)
            for col in columns:
                col_name = col.get('name') or col.get('column_name', '')
                table_info['columns'][col_name] = {
                    'type': col.get('type') or col.get('data_type', ''),
                    'nullable': col.get('nullable', True),
                    'is_primary_key': col.get('pk', 0) == 1
                }
                
                if table_info['columns'][col_name]['is_primary_key']:
                    table_info['key_columns'].append(col_name)
            
            # Get sample data
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            table_info['sample_data'] = self.db.execute_query(sample_query)
            
            # Get row count
            count_query = f"SELECT COUNT(*) as count FROM {table_name}"
            count_result = self.db.execute_query(count_query)
            table_info['row_count'] = count_result[0]['count'] if count_result else 0
            
        except Exception as e:
            logger.error(f"Failed to analyze table {table_name}: {e}")
        
        return table_info
    
    def _detect_relationships(self, tables: List[str]) -> List[Dict]:
        """Detect potential relationships between tables"""
        relationships = []
        
        # Simple foreign key detection based on naming patterns
        for table in tables:
            try:
                columns = self.db.get_table_schema(table)
                for col in columns:
                    col_name = col.get('name') or col.get('column_name', '')
                    
                    # Look for _id pattern
                    if col_name.endswith('_id') and col_name != 'id':
                        potential_parent = col_name[:-3]  # Remove _id suffix
                        
                        # Check if there's a table that matches
                        for other_table in tables:
                            if other_table.lower() == potential_parent.lower():
                                relationships.append({
                                    'child_table': table,
                                    'child_column': col_name,
                                    'parent_table': other_table,
                                    'parent_column': 'id',
                                    'confidence': 0.8
                                })
            except Exception as e:
                logger.error(f"Error detecting relationships for {table}: {e}")
        
        return relationships
    
    def _analyze_data_patterns(self, tables: List[str]) -> Dict[str, Any]:
        """Analyze data patterns for insights"""
        patterns = {}
        
        for table in tables:
            try:
                # Analyze timestamp patterns
                columns = self.db.get_table_schema(table)
                for col in columns:
                    col_name = col.get('name') or col.get('column_name', '')
                    col_type = col.get('type') or col.get('data_type', '')
                    
                    if 'timestamp' in col_name.lower() or 'datetime' in col_type.lower():
                        # Analyze time distribution
                        time_query = f"""
                            SELECT 
                                DATE({col_name}) as date,
                                COUNT(*) as count
                            FROM {table} 
                            WHERE {col_name} IS NOT NULL
                            GROUP BY DATE({col_name})
                            ORDER BY date DESC
                            LIMIT 10
                        """
                        time_data = self.db.execute_query(time_query)
                        
                        patterns[f"{table}.{col_name}"] = {
                            'type': 'time_distribution',
                            'data': time_data
                        }
                        
            except Exception as e:
                logger.error(f"Error analyzing patterns for {table}: {e}")
        
        return patterns