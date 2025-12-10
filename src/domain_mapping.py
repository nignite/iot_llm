#!/usr/bin/env python3
"""
Domain Name to Table Name Mapping System for IoT Database
Maps business/domain terminology to actual database table names
Loads mappings from JSON configuration files
"""

import json
import os
from pathlib import Path

class DomainMapper:
    def __init__(self, config_path: str = None):
        """
        Initialize domain mapper with mappings loaded from JSON files
        
        Args:
            config_path: Path to the db directory containing JSON files
                        If None, will auto-detect relative to this file
        
        Raises:
            FileNotFoundError: If table_domain_mappings.json is not found
            ValueError: If JSON file contains invalid data
            RuntimeError: If mappings cannot be loaded
        """
        if config_path is None:
            # Auto-detect path relative to this file
            current_dir = Path(__file__).parent
            config_path = current_dir.parent / "db"
        else:
            config_path = Path(config_path)
        
        self.config_path = config_path
        self._load_mappings()
    
    def _load_mappings(self):
        """Load all mappings from JSON configuration files"""
        try:
            # Load comprehensive mappings
            mappings_file = self.config_path / "table_domain_mappings.json"
            with open(mappings_file, 'r') as f:
                data = json.load(f)
            
            # Extract table mappings from reverse_mappings section
            self.table_mappings = data.get("reverse_mappings", {})
            
            # Extract column mappings from tables section
            self.column_mappings = {}
            for table_name, table_info in data.get("tables", {}).items():
                if "column_mappings" in table_info:
                    self.column_mappings[table_name] = table_info["column_mappings"]
            
            # Extract business terms
            business_terms_data = data.get("business_terms", {})
            self.business_terms = business_terms_data.get("terms", {})
            
            # Store table descriptions for reference
            self.table_descriptions = {}
            for table_name, table_info in data.get("tables", {}).items():
                self.table_descriptions[table_name] = {
                    "description": table_info.get("description", ""),
                    "purpose": table_info.get("purpose", ""),
                    "domain_names": table_info.get("domain_names", [])
                }
        
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Required mapping file not found: {e}. Please ensure table_domain_mappings.json exists in {self.config_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in mapping file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading mappings: {e}")
    
    
    def get_table_name(self, domain_term: str) -> str:
        """Convert domain term to actual table name"""
        return self.table_mappings.get(domain_term.lower(), domain_term)
    
    def get_column_aliases(self, table_name: str, column_name: str) -> list:
        """Get possible domain names for a database column"""
        if table_name in self.column_mappings:
            return self.column_mappings[table_name].get(column_name, [column_name])
        return [column_name]
    
    def resolve_business_term(self, term: str) -> list:
        """Resolve business terminology to technical terms"""
        return self.business_terms.get(term.lower(), [term])
    
    def get_table_description(self, table_name: str) -> dict:
        """Get description and metadata for a table"""
        return self.table_descriptions.get(table_name, {
            "description": "No description available",
            "purpose": "No purpose defined", 
            "domain_names": []
        })
    
    def get_all_mappings(self) -> dict:
        """Return all mappings for reference"""
        return {
            "table_mappings": self.table_mappings,
            "column_mappings": self.column_mappings,
            "business_terms": self.business_terms,
            "table_descriptions": self.table_descriptions
        }
    
    def reverse_lookup_table(self, table_name: str) -> list:
        """Find all domain terms that map to a specific table"""
        return [domain for domain, table in self.table_mappings.items() if table == table_name]
    
    def reload_mappings(self):
        """Reload mappings from JSON files (useful for runtime updates)"""
        self._load_mappings()
    
    def get_config_path(self) -> Path:
        """Get the path to configuration files"""
        return self.config_path

def main():
    """Demo the Domain Mapper functionality"""
    mapper = DomainMapper()
    
    print("Domain Mapper - Loading from JSON Configuration")
    print("=" * 50)
    print(f"Config path: {mapper.get_config_path()}")
    
    print("\nDomain to Table Mappings (sample):")
    print("-" * 40)
    sample_terms = ["signal", "alerts", "devices", "thresholds", "locations"]
    for domain in sample_terms:
        table = mapper.get_table_name(domain)
        print(f"{domain:15} -> {table}")
    
    print("\nTable Descriptions:")
    print("-" * 40)
    unique_tables = set(mapper.table_mappings.values())
    for table in sorted(unique_tables):
        desc = mapper.get_table_description(table)
        print(f"{table}:")
        print(f"  Description: {desc['description']}")
        print(f"  Domain names: {', '.join(desc['domain_names'][:3])}...")
        print()
    
    print("Business Term Resolution:")
    print("-" * 40)
    business_terms = ["crossed", "offline", "high", "limits"]
    for term in business_terms:
        resolved = mapper.resolve_business_term(term)
        print(f"'{term}' -> {resolved}")
    
    print(f"\nTotal mappings loaded: {len(mapper.table_mappings)}")

if __name__ == "__main__":
    main()