#!/usr/bin/env python3
"""
Oracle CIMS Database Query Interface for Natural Language Queries
Converts natural language questions into SQL queries using Oracle schema domain mapping
"""

import sqlite3
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from oracle_domain_mapping import OracleDomainMapper

class OracleQueryInterface:
    def __init__(self, db_path: str = "oracle_iot_db.db"):
        self.db_path = db_path
        self.mapper = OracleDomainMapper()
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
    def get_database_schema(self) -> Dict:
        """Get complete database schema for context"""
        schema = {}
        
        # Get all tables
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in self.cursor.fetchall()]
        
        for table in tables:
            self.cursor.execute(f"PRAGMA table_info({table})")
            columns = self.cursor.fetchall()
            schema[table] = {
                'columns': [{'name': col[1], 'type': col[2], 'notnull': col[3], 'pk': col[5]} for col in columns],
                'domain_names': self.mapper.reverse_lookup_table(table)
            }
        
        return schema
    
    def parse_time_expressions(self, query: str) -> Tuple[str, Optional[datetime], Optional[datetime]]:
        """Parse time expressions from natural language"""
        query_lower = query.lower()
        now = datetime.now()
        start_date = None
        end_date = None
        modified_query = query
        
        # Time patterns for Oracle schema
        time_patterns = {
            r'\blast\s+week\b': (now - timedelta(days=7), now),
            r'\bpast\s+week\b': (now - timedelta(days=7), now),
            r'\bthis\s+week\b': (now - timedelta(days=now.weekday()), now),
            r'\blast\s+month\b': (now - timedelta(days=30), now),
            r'\bpast\s+month\b': (now - timedelta(days=30), now),
            r'\bthis\s+month\b': (now.replace(day=1), now),
            r'\blast\s+(\d+)\s+days?\b': lambda m: (now - timedelta(days=int(m.group(1))), now),
            r'\bpast\s+(\d+)\s+days?\b': lambda m: (now - timedelta(days=int(m.group(1))), now),
            r'\blast\s+(\d+)\s+hours?\b': lambda m: (now - timedelta(hours=int(m.group(1))), now),
            r'\bpast\s+(\d+)\s+hours?\b': lambda m: (now - timedelta(hours=int(m.group(1))), now),
            r'\byesterday\b': (now - timedelta(days=1), now - timedelta(days=1) + timedelta(hours=23, minutes=59)),
            r'\btoday\b': (now.replace(hour=0, minute=0, second=0), now),
            r'\blast\s+hour\b': (now - timedelta(hours=1), now),
            r'\bpast\s+hour\b': (now - timedelta(hours=1), now)
        }
        
        for pattern, time_range in time_patterns.items():
            match = re.search(pattern, query_lower)
            if match:
                if callable(time_range):
                    start_date, end_date = time_range(match)
                else:
                    start_date, end_date = time_range
                # Remove the time expression from query
                modified_query = re.sub(pattern, '', modified_query, flags=re.IGNORECASE).strip()
                break
        
        return modified_query, start_date, end_date
    
    def identify_query_intent(self, query: str) -> Dict:
        """Identify the intent and components of the query"""
        query_lower = query.lower().strip()
        
        # Remove time expressions first
        query_cleaned, start_time, end_time = self.parse_time_expressions(query)
        query_cleaned_lower = query_cleaned.lower()
        
        intent = {
            'action': 'SELECT',
            'tables': [],
            'columns': [],
            'filters': [],
            'aggregation': None,
            'time_range': {'start': start_time, 'end': end_time},
            'limit': None,
            'order_by': None
        }
        
        # Identify action
        if any(word in query_lower for word in ['show', 'list', 'display', 'get', 'find', 'what']):
            intent['action'] = 'SELECT'
        elif any(word in query_lower for word in ['count', 'how many']):
            intent['action'] = 'COUNT'
            intent['aggregation'] = 'COUNT'
        elif any(word in query_lower for word in ['average', 'avg', 'mean']):
            intent['aggregation'] = 'AVG'
        elif any(word in query_lower for word in ['maximum', 'max', 'highest']):
            intent['aggregation'] = 'MAX'
        elif any(word in query_lower for word in ['minimum', 'min', 'lowest']):
            intent['aggregation'] = 'MIN'
        elif any(word in query_lower for word in ['sum', 'total']):
            intent['aggregation'] = 'SUM'
        
        # Identify tables based on domain terms
        suggested_tables = self.mapper.suggest_tables(query_cleaned)
        intent['tables'] = suggested_tables
        
        # If no specific tables identified, try to infer from context more intelligently
        if not intent['tables']:
            # Prioritize based on specific keywords
            if any(word in query_cleaned_lower for word in ['current values', 'live values', 'latest values', 'signal values', 'current readings']):
                intent['tables'] = ['SIGNALVALUE']
            elif any(word in query_cleaned_lower for word in ['historical data', 'history', 'past data', 'archived', 'time series']):
                intent['tables'] = ['REPDATA']
            elif any(word in query_cleaned_lower for word in ['channel groups', 'groups', 'equipment groups']):
                intent['tables'] = ['CHANNELGROUP']
            elif any(word in query_cleaned_lower for word in ['channels', 'signal channels', 'communication', 'protocols']):
                intent['tables'] = ['SIGNALCHANNEL']
            elif any(word in query_cleaned_lower for word in ['calculations', 'reports', 'computed', 'aggregated']):
                intent['tables'] = ['REPITEM']
            elif any(word in query_cleaned_lower for word in ['process instances', 'periods', 'time periods', 'batches']):
                intent['tables'] = ['PROCINSTANCE']
            elif any(word in query_cleaned_lower for word in ['addresses', 'external systems', 'endpoints']):
                intent['tables'] = ['ADDRESS']
            elif any(word in query_cleaned_lower for word in ['signal', 'sensor', 'measurement']):
                # Only default to SIGNALITEM if it's clearly about signal definitions
                if any(word in query_cleaned_lower for word in ['definition', 'configure', 'setup', 'type']):
                    intent['tables'] = ['SIGNALITEM']
                else:
                    # For general signal queries, prefer current values
                    intent['tables'] = ['SIGNALVALUE']
            else:
                # Last resort - try to be smart about it
                if 'current' in query_cleaned_lower or 'now' in query_cleaned_lower:
                    intent['tables'] = ['SIGNALVALUE']
                elif 'all' in query_cleaned_lower and any(word in query_cleaned_lower for word in ['signals', 'sensors']):
                    intent['tables'] = ['SIGNALITEM']
                else:
                    intent['tables'] = ['SIGNALITEM']
        
        # Extract filters
        intent['filters'] = self.extract_filters(query_cleaned)
        
        # Extract limit
        limit_match = re.search(r'\btop\s+(\d+)\b|\blimit\s+(\d+)\b|\bfirst\s+(\d+)\b', query_lower)
        if limit_match:
            intent['limit'] = int(next(g for g in limit_match.groups() if g))
        
        # Default limit for safety
        if not intent['limit']:
            intent['limit'] = 100
        
        return intent
    
    def extract_filters(self, query: str) -> List[Dict]:
        """Extract filter conditions from the query"""
        filters = []
        query_lower = query.lower()
        
        # Status filters
        if any(word in query_lower for word in ['online', 'active', 'running']):
            filters.append({'column': 'status', 'operator': '=', 'value': 'online'})
        elif any(word in query_lower for word in ['offline', 'inactive', 'down']):
            filters.append({'column': 'status', 'operator': '=', 'value': 'offline'})
        
        # Quality filters
        if any(word in query_lower for word in ['good quality', 'valid']):
            filters.append({'column': 'SIGSTATUS', 'operator': '=', 'value': 0})
            filters.append({'column': 'PCTQUAL', 'operator': '>', 'value': 0.9})
        elif any(word in query_lower for word in ['bad quality', 'invalid']):
            filters.append({'column': 'SIGSTATUS', 'operator': '!=', 'value': 0})
        
        # Value range filters
        value_patterns = [
            (r'(?:above|over|greater than|>)\s*(\d+(?:\.\d+)?)', '>', 1),
            (r'(?:below|under|less than|<)\s*(\d+(?:\.\d+)?)', '<', 1),
            (r'(?:equals?|=)\s*(\d+(?:\.\d+)?)', '=', 1),
            (r'between\s+(\d+(?:\.\d+)?)\s+and\s+(\d+(?:\.\d+)?)', 'BETWEEN', 2)
        ]
        
        for pattern, operator, num_groups in value_patterns:
            match = re.search(pattern, query_lower)
            if match:
                if num_groups == 1:
                    filters.append({
                        'column': 'SIGNUMVALUE', 
                        'operator': operator, 
                        'value': float(match.group(1))
                    })
                elif num_groups == 2:
                    filters.append({
                        'column': 'SIGNUMVALUE', 
                        'operator': operator, 
                        'value': [float(match.group(1)), float(match.group(2))]
                    })
        
        # Unit filters
        unit_patterns = [
            (r'\b(?:degrees?|°c|celsius)\b', '°C'),
            (r'\b(?:fahrenheit|°f)\b', '°F'),
            (r'\b(?:bar|psi|pascal)\b', 'bar'),
            (r'\b(?:lpm|l/min|liters?)\b', 'L/min'),
            (r'\b(?:watts?|w|kw)\b', 'W'),
            (r'\b(?:percent|%|rh)\b', '%')
        ]
        
        for pattern, unit in unit_patterns:
            if re.search(pattern, query_lower):
                filters.append({'column': 'OBJUNIT', 'operator': 'LIKE', 'value': f'%{unit}%'})
                break
        
        return filters
    
    def build_sql_query(self, intent: Dict) -> Tuple[str, List]:
        """Build SQL query from intent"""
        params = []
        
        # Determine primary table
        primary_table = intent['tables'][0] if intent['tables'] else 'SIGNALITEM'
        
        # Build SELECT clause
        if intent['aggregation']:
            if intent['aggregation'] == 'COUNT':
                select_clause = "SELECT COUNT(*)"
            else:
                # For aggregations, target the numeric value column
                if primary_table == 'SIGNALVALUE':
                    select_clause = f"SELECT {intent['aggregation']}(SIGNUMVALUE)"
                elif primary_table == 'REPDATA':
                    select_clause = f"SELECT {intent['aggregation']}(NUMVALUE)"
                else:
                    select_clause = "SELECT COUNT(*)"
        else:
            # Select appropriate columns based on table
            if primary_table == 'SIGNALITEM':
                select_clause = "SELECT SIGID, SIGNAME, SIGTYPE, OBJDESCR, OBJUNIT, CHANNR"
            elif primary_table == 'SIGNALVALUE':
                select_clause = "SELECT SIGID, UPDATETIME, SIGNUMVALUE, SIGTEXTVALUE, SIGSTATUS"
            elif primary_table == 'SIGNALCHANNEL':
                select_clause = "SELECT CHANNR, CHANNAME, CHANDESCR, GROUPNR, HOSTNAME"
            elif primary_table == 'REPDATA':
                select_clause = "SELECT PINSTID, RICODE, NUMVALUE, TEXTVALUE, PCTQUAL"
            elif primary_table == 'REPITEM':
                select_clause = "SELECT RICODE, RITEXT, RICLASS, RIUNIT, DESCRIPTION"
            elif primary_table == 'CHANNELGROUP':
                select_clause = "SELECT GROUPNR, GROUPNAME, DESCRIPTION, NODENR"
            else:
                select_clause = "SELECT *"
        
        # Build FROM clause
        from_clause = f" FROM {primary_table}"
        
        # Build WHERE clause
        where_conditions = []
        
        # Add filter conditions
        for filter_condition in intent['filters']:
            column = filter_condition['column']
            operator = filter_condition['operator']
            value = filter_condition['value']
            
            if operator == 'BETWEEN' and isinstance(value, list):
                where_conditions.append(f"{column} BETWEEN ? AND ?")
                params.extend(value)
            elif operator == 'LIKE':
                where_conditions.append(f"{column} LIKE ?")
                params.append(value)
            else:
                where_conditions.append(f"{column} {operator} ?")
                params.append(value)
        
        # Add time range filters
        if intent['time_range']['start'] and intent['time_range']['end']:
            if primary_table == 'SIGNALVALUE':
                where_conditions.append("UPDATETIME BETWEEN ? AND ?")
                params.extend([intent['time_range']['start'], intent['time_range']['end']])
            elif primary_table == 'REPDATA':
                # Need to join with PROCINSTANCE for time filtering
                from_clause += " JOIN PROCINSTANCE ON REPDATA.PINSTID = PROCINSTANCE.PINSTID"
                where_conditions.append("PROCINSTANCE.PINSTSTART BETWEEN ? AND ?")
                params.extend([intent['time_range']['start'], intent['time_range']['end']])
        
        # Build complete WHERE clause
        where_clause = ""
        if where_conditions:
            where_clause = " WHERE " + " AND ".join(where_conditions)
        
        # Build ORDER BY clause
        order_clause = ""
        if not intent['aggregation']:
            if primary_table == 'SIGNALVALUE':
                order_clause = " ORDER BY UPDATETIME DESC"
            elif primary_table == 'REPDATA':
                order_clause = " ORDER BY PINSTID DESC"
            elif primary_table == 'SIGNALITEM':
                order_clause = " ORDER BY SIGNAME"
            else:
                order_clause = " ORDER BY 1"
        
        # Build LIMIT clause
        limit_clause = f" LIMIT {intent['limit']}" if intent['limit'] else ""
        
        # Combine all parts
        sql = select_clause + from_clause + where_clause + order_clause + limit_clause
        
        return sql, params
    
    def execute_natural_language_query(self, query: str) -> Dict:
        """Execute a natural language query and return results"""
        try:
            # Parse the query intent
            intent = self.identify_query_intent(query)
            
            # Build SQL
            sql, params = self.build_sql_query(intent)
            
            # Execute query
            self.cursor.execute(sql, params)
            results = self.cursor.fetchall()
            
            # Get column names
            columns = [desc[0] for desc in self.cursor.description]
            
            # Convert to list of dictionaries
            result_dicts = []
            for row in results:
                result_dicts.append(dict(zip(columns, row)))
            
            return {
                'success': True,
                'query': query,
                'sql': sql,
                'params': params,
                'results': result_dicts,
                'count': len(result_dicts),
                'time_range': intent['time_range']
            }
            
        except Exception as e:
            return {
                'success': False,
                'query': query,
                'error': str(e),
                'sql': sql if 'sql' in locals() else None,
                'params': params if 'params' in locals() else None
            }