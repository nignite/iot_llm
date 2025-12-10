#!/usr/bin/env python3
"""
Domain Name to Table Name Mapping System for Oracle-based IoT Database
Maps business/domain terminology to actual Oracle CIMS database table names
"""

import re

class OracleDomainMapper:
    def __init__(self):
        self.table_mappings = {
            # Signal/Sensor Mappings
            "signal": "SIGNALITEM",
            "signals": "SIGNALITEM", 
            "sensor": "SIGNALITEM",
            "sensors": "SIGNALITEM",
            "signal_definitions": "SIGNALITEM",
            "sensor_definitions": "SIGNALITEM",
            "measurement_points": "SIGNALITEM",
            "data_points": "SIGNALITEM",
            "instruments": "SIGNALITEM",
            
            # Current Signal Values
            "current_values": "SIGNALVALUE",
            "live_data": "SIGNALVALUE",
            "real_time_data": "SIGNALVALUE",
            "current_readings": "SIGNALVALUE",
            "latest_values": "SIGNALVALUE",
            "signal_values": "SIGNALVALUE",
            
            # Historical Data
            "historical_data": "REPDATA",
            "history": "REPDATA",
            "historical_values": "REPDATA",
            "time_series": "REPDATA",
            "archived_data": "REPDATA",
            "process_data": "REPDATA",
            "measurements": "REPDATA",
            "readings": "REPDATA",
            
            # Report Items (Calculations/Aggregations)
            "calculations": "REPITEM",
            "calculated_values": "REPITEM",
            "aggregations": "REPITEM",
            "aggregated_data": "REPITEM",
            "computed_data": "REPITEM",
            "analytics": "REPITEM",
            "reports": "REPITEM",
            "report_items": "REPITEM",
            "kpi": "REPITEM",
            "metrics": "REPITEM",
            
            # Communication Channels
            "channels": "SIGNALCHANNEL",
            "signal_channels": "SIGNALCHANNEL",
            "communication_channels": "SIGNALCHANNEL",
            "data_sources": "SIGNALCHANNEL",
            "connections": "SIGNALCHANNEL",
            "interfaces": "SIGNALCHANNEL",
            "protocols": "SIGNALCHANNEL",
            
            # Channel Groups
            "groups": "CHANNELGROUP",
            "channel_groups": "CHANNELGROUP",
            "signal_groups": "CHANNELGROUP",
            "equipment_groups": "CHANNELGROUP",
            "areas": "CHANNELGROUP",
            "zones": "CHANNELGROUP",
            
            # Process Time Periods
            "time_periods": "PROCINSTANCE",
            "process_instances": "PROCINSTANCE",
            "periods": "PROCINSTANCE",
            "intervals": "PROCINSTANCE",
            "batches": "PROCINSTANCE",
            "runs": "PROCINSTANCE",
            "sessions": "PROCINSTANCE",
            
            # External Systems/Addresses
            "addresses": "ADDRESS",
            "external_systems": "ADDRESS",
            "remote_systems": "ADDRESS",
            "customers": "ADDRESS",
            "endpoints": "ADDRESS",
            "destinations": "ADDRESS"
        }
        
        # Business terminology mapping
        self.business_terms = {
            # Process Industry Terms
            "temperature": ["temp", "thermal", "heat", "degrees"],
            "pressure": ["press", "force", "psi", "bar", "pascal"],
            "flow": ["flowrate", "rate", "volume", "throughput"],
            "level": ["height", "depth", "tank_level", "fill"],
            "vibration": ["vibe", "oscillation", "shake", "frequency"],
            "power": ["electrical", "energy", "watts", "consumption"],
            "humidity": ["moisture", "water_content", "rh"],
            
            # Equipment Terms
            "pump": ["motor", "compressor", "fan"],
            "tank": ["vessel", "container", "storage"],
            "valve": ["actuator", "damper", "control"],
            "line": ["pipe", "conduit", "duct"],
            
            # Process Terms
            "production": ["manufacturing", "output", "yield"],
            "quality": ["specification", "grade", "standard"],
            "efficiency": ["performance", "utilization", "productivity"],
            "alarm": ["alert", "warning", "fault", "error"],
            
            # Time-based Terms
            "hourly": ["hour", "hr", "h"],
            "daily": ["day", "d", "24h"],
            "weekly": ["week", "7d"],
            "monthly": ["month", "30d"],
            "shift": ["period", "block", "rotation"],
            
            # Status Terms
            "online": ["active", "running", "operational"],
            "offline": ["inactive", "stopped", "down"],
            "fault": ["error", "alarm", "problem", "issue"],
            "normal": ["ok", "good", "stable", "healthy"],
            
            # Aggregation Terms
            "average": ["avg", "mean"],
            "maximum": ["max", "peak", "highest"],
            "minimum": ["min", "lowest"],
            "total": ["sum", "cumulative"],
            "count": ["number", "quantity", "amount"]
        }
        
        # Operator mappings
        self.operators = {
            "greater than": ">",
            "less than": "<", 
            "equals": "=",
            "not equals": "!=",
            "above": ">",
            "below": "<",
            "over": ">",
            "under": "<",
            "higher": ">",
            "lower": "<",
            "exceeds": ">",
            "beyond": ">",
            "within": "BETWEEN",
            "between": "BETWEEN",
            "like": "LIKE",
            "contains": "LIKE",
            "includes": "LIKE"
        }

    def get_table_for_domain(self, domain_term: str) -> str:
        """Get the actual table name for a business domain term"""
        domain_term = domain_term.lower().strip()
        return self.table_mappings.get(domain_term, None)
    
    def reverse_lookup_table(self, table_name: str) -> list:
        """Get all domain terms that map to a given table"""
        table_name = table_name.upper()
        return [domain for domain, table in self.table_mappings.items() if table.upper() == table_name]
    
    def expand_business_term(self, term: str) -> list:
        """Expand a business term to include synonyms"""
        term = term.lower().strip()
        
        # First check if the term itself is a key
        if term in self.business_terms:
            return [term] + self.business_terms[term]
        
        # Then check if the term appears in any synonym list
        for main_term, synonyms in self.business_terms.items():
            if term in synonyms:
                return [main_term] + synonyms
        
        return [term]
    
    def get_operator(self, operator_text: str) -> str:
        """Convert natural language operator to SQL operator"""
        operator_text = operator_text.lower().strip()
        return self.operators.get(operator_text, "=")
    
    def suggest_tables(self, query_text: str) -> list:
        """Suggest relevant tables based on query text"""
        query_lower = query_text.lower()
        suggestions = []
        scores = {}
        
        # Score tables based on keyword matches
        for domain_term, table_name in self.table_mappings.items():
            if domain_term in query_lower:
                if table_name not in scores:
                    scores[table_name] = 0
                scores[table_name] += len(domain_term)  # Longer matches get higher scores
        
        # Additional specific pattern matching
        patterns = {
            'SIGNALVALUE': [r'\bcurrent\s+values?\b', r'\blive\s+data\b', r'\blatest\s+values?\b', r'\breal\s*time\b', r'\bsignal\s+values?\b', r'\bcurrent\s+signal\b'],
            'REPDATA': [r'\bhistorical\s+data\b', r'\bhistory\b', r'\bpast\s+data\b', r'\barchived\b'],
            'SIGNALCHANNEL': [r'\bchannels?\b', r'\bcommunication\b', r'\bprotocols?\b', r'\bconnections?\b'],
            'CHANNELGROUP': [r'\bgroups?\b', r'\bareas?\b', r'\bzones?\b', r'\bequipment\s+groups?\b'],
            'REPITEM': [r'\bcalculations?\b', r'\breports?\b', r'\bcomputed\b', r'\baggregated?\b'],
            'PROCINSTANCE': [r'\bprocess\s+instances?\b', r'\bperiods?\b', r'\bbatches?\b', r'\btime\s+periods?\b'],
            'SIGNALITEM': [r'\bsignal\s+definitions?\b', r'\bsensor\s+definitions?\b', r'\bsignal\s+config\b', r'\ball\s+signals?\b', r'\ball\s+sensors?\b']
        }
        
        for table, pattern_list in patterns.items():
            for pattern in pattern_list:
                if re.search(pattern, query_lower):
                    if table not in scores:
                        scores[table] = 0
                    scores[table] += 10  # Pattern matches get high scores
        
        # Return tables sorted by score
        if scores:
            suggestions = sorted(scores.keys(), key=lambda x: scores[x], reverse=True)
        
        return suggestions[:2]  # Return top 2 suggestions
    
    def get_schema_info(self) -> dict:
        """Return schema information for the Oracle tables"""
        return {
            "SIGNALITEM": {
                "description": "Signal/sensor definitions and configurations",
                "key_columns": ["SIGID", "SIGNAME", "SIGTYPE", "OBJUNIT", "CHANNR"],
                "common_filters": ["SIGTYPE", "CHANNR", "OBJUNIT"]
            },
            "SIGNALVALUE": {
                "description": "Current/latest signal values",
                "key_columns": ["SIGID", "UPDATETIME", "SIGNUMVALUE", "SIGSTATUS"],
                "common_filters": ["SIGID", "UPDATETIME", "SIGSTATUS"]
            },
            "REPDATA": {
                "description": "Historical process data and measurements",
                "key_columns": ["PINSTID", "RICODE", "NUMVALUE", "PCTQUAL"],
                "common_filters": ["PINSTID", "RICODE", "PCTQUAL"]
            },
            "REPITEM": {
                "description": "Report item definitions and calculations",
                "key_columns": ["RICODE", "RITEXT", "RICLASS", "RIUNIT"],
                "common_filters": ["RICLASS", "RIUNIT", "LOGCLASS"]
            },
            "SIGNALCHANNEL": {
                "description": "Communication channels and data sources",
                "key_columns": ["CHANNR", "CHANNAME", "SIGPROTID", "GROUPNR"],
                "common_filters": ["GROUPNR", "SIGPROTID", "CHANNAME"]
            },
            "CHANNELGROUP": {
                "description": "Logical groupings of channels/equipment",
                "key_columns": ["GROUPNR", "GROUPNAME", "NODENR"],
                "common_filters": ["NODENR", "GROUPNAME"]
            },
            "PROCINSTANCE": {
                "description": "Process time periods and data collection intervals",
                "key_columns": ["PINSTID", "PINSTSTART", "PINSTEND", "PTYPE"],
                "common_filters": ["PTYPE", "PINSTSTART", "PINSTEND", "CLOSED"]
            },
            "ADDRESS": {
                "description": "External system addresses and endpoints",
                "key_columns": ["ADDRID", "ADDRNAME", "REMOTEURL", "NODENR"],
                "common_filters": ["NODENR", "ADDRNAME"]
            }
        }