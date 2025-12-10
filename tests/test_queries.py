#!/usr/bin/env python3
"""
Test script to demonstrate various natural language queries
"""

from src.claude_query_interface import ClaudeQueryInterface
from src.domain_mapping import DomainMapper
import json

def run_test_queries():
    """Run a comprehensive set of test queries"""
    interface = ClaudeQueryInterface()
    
    test_queries = [
        "Which signals crossed the value limits last week?",
        "Show me all alerts from yesterday", 
        "What devices are currently offline?",
        "Find temperature readings above 30 degrees",
        "Count how many anomalies were detected this week",
        "What was the average humidity last month?", 
        "Show me all critical alerts",
        "Which sensors had quality issues today?",
        "List all devices in Factory Floor A",
        "What are the maximum pressure readings from last week?",
        "Show me temperature sensors that exceeded limits",
        "Find all devices with status online",
        "What alerts happened in the last 7 days?",
        "Count total signals recorded",
        "Show devices in location LOC001"
    ]
    
    print("IoT Database Natural Language Query Tests")
    print("=" * 60)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n{i}. Query: {query}")
        print("-" * 50)
        
        result = interface.execute_natural_language_query(query)
        
        if result['success']:
            print(f"✓ SQL: {result['sql']}")
            if result['params']:
                print(f"✓ Params: {result['params']}")
            print(f"✓ Results: {result['count']} records found")
            
            if result['time_range']['start']:
                print(f"✓ Time Range: {result['time_range']['start']} to {result['time_range']['end']}")
            
            # Show first result as sample
            if result['results']:
                print(f"✓ Sample: {result['results'][0]}")
        else:
            print(f"✗ Error: {result['error']}")
            if result.get('sql'):
                print(f"✗ Attempted SQL: {result['sql']}")
    
    interface.close()
    print(f"\n{'=' * 60}")
    print("Test completed!")

def demonstrate_domain_mapping():
    """Demonstrate the domain mapping functionality"""
    mapper = DomainMapper()
    
    print("\nDomain Mapping Demonstration")
    print("=" * 40)
    
    # Test domain term mappings
    test_terms = ["signal", "alerts", "device_config", "thresholds", "logs"]
    
    print("\nDomain Term → Table Name:")
    for term in test_terms:
        table = mapper.get_table_name(term)
        print(f"  {term:15} → {table}")
    
    # Test business term resolution
    business_terms = ["crossed", "limits", "last week", "offline", "anomaly"]
    
    print("\nBusiness Term Resolution:")
    for term in business_terms:
        resolved = mapper.resolve_business_term(term)
        print(f"  {term:15} → {resolved}")
    
    # Show reverse mapping
    print("\nReverse Table Mapping:")
    tables = ["RepData", "AlertLog", "DevMap"]
    for table in tables:
        domain_terms = mapper.reverse_lookup_table(table)
        print(f"  {table:15} ← {', '.join(domain_terms[:5])}...")

def show_database_stats():
    """Show database statistics"""
    interface = ClaudeQueryInterface()
    
    print("\nDatabase Statistics")
    print("=" * 30)
    
    stats_queries = [
        ("Total signal records", "SELECT COUNT(*) FROM RepData"),
        ("Total devices", "SELECT COUNT(*) FROM DevMap"), 
        ("Total alerts", "SELECT COUNT(*) FROM AlertLog"),
        ("Online devices", "SELECT COUNT(*) FROM DevMap WHERE status = 'online'"),
        ("Recent alerts (24h)", "SELECT COUNT(*) FROM AlertLog WHERE timestamp > datetime('now', '-1 day')"),
        ("Sensor types", "SELECT COUNT(DISTINCT sensor_type) FROM RepData"),
        ("Locations", "SELECT COUNT(*) FROM LocRef")
    ]
    
    for description, sql in stats_queries:
        interface.cursor.execute(sql)
        result = interface.cursor.fetchone()[0]
        print(f"  {description:20}: {result:,}")
    
    interface.close()

if __name__ == "__main__":
    # Run comprehensive tests
    run_test_queries()
    
    # Demonstrate domain mapping
    demonstrate_domain_mapping()
    
    # Show database statistics
    show_database_stats()