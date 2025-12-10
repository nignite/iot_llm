#!/usr/bin/env python3
"""
Comprehensive Test Suite for CLI and Streamlit Frontends
Tests both interfaces with various scenarios and edge cases
"""

import subprocess
import sys
import time
import json
import tempfile
import os
from typing import List, Dict
import sqlite3

class FrontendTester:
    """Test suite for IoT database frontends"""
    
    def __init__(self):
        self.test_results = []
        self.db_path = "iot_production.db"
    
    def log_test(self, test_name: str, status: str, details: str = ""):
        """Log test results"""
        result = {
            'test_name': test_name,
            'status': status,
            'details': details,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        self.test_results.append(result)
        
        status_symbol = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
        print(f"{status_symbol} {test_name}: {status}")
        if details:
            print(f"   Details: {details}")
    
    def test_database_exists(self):
        """Test if database file exists and is accessible"""
        try:
            if not os.path.exists(self.db_path):
                self.log_test("Database Existence", "FAIL", "Database file not found")
                return False
            
            # Test database connectivity
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM RepData")
            count = cursor.fetchone()[0]
            conn.close()
            
            self.log_test("Database Connectivity", "PASS", f"Found {count:,} signal records")
            return True
            
        except Exception as e:
            self.log_test("Database Connectivity", "FAIL", str(e))
            return False
    
    def test_cli_basic_functionality(self):
        """Test CLI basic functionality"""
        try:
            # Test help command
            result = subprocess.run(
                [sys.executable, "iot_cli.py", "--query", "help"],
                capture_output=True, text=True, timeout=30
            )
            
            if result.returncode == 0:
                self.log_test("CLI Help Command", "PASS", "Help command executed successfully")
            else:
                self.log_test("CLI Help Command", "FAIL", f"Return code: {result.returncode}")
                return False
            
            return True
            
        except subprocess.TimeoutExpired:
            self.log_test("CLI Help Command", "FAIL", "Command timed out")
            return False
        except Exception as e:
            self.log_test("CLI Help Command", "FAIL", str(e))
            return False
    
    def test_cli_json_output(self):
        """Test CLI JSON output functionality"""
        try:
            # Test with a simple query
            result = subprocess.run(
                [sys.executable, "iot_cli.py", "--query", "count total signals recorded", "--json"],
                capture_output=True, text=True, timeout=60
            )
            
            if result.returncode == 0:
                try:
                    json_result = json.loads(result.stdout)
                    if 'success' in json_result:
                        self.log_test("CLI JSON Output", "PASS", f"Valid JSON returned: {json_result['success']}")
                    else:
                        self.log_test("CLI JSON Output", "FAIL", "Invalid JSON structure")
                except json.JSONDecodeError:
                    self.log_test("CLI JSON Output", "FAIL", "Invalid JSON format")
            else:
                self.log_test("CLI JSON Output", "FAIL", f"Return code: {result.returncode}")
                
        except subprocess.TimeoutExpired:
            self.log_test("CLI JSON Output", "FAIL", "Command timed out")
        except Exception as e:
            self.log_test("CLI JSON Output", "FAIL", str(e))
    
    def test_cli_batch_mode(self):
        """Test CLI batch processing"""
        try:
            # Create a temporary batch file
            test_queries = [
                "# This is a comment",
                "count total signals recorded",
                "what devices are currently offline?",
                "show me all critical alerts"
            ]
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
                for query in test_queries:
                    f.write(query + '\n')
                batch_file = f.name
            
            try:
                result = subprocess.run(
                    [sys.executable, "iot_cli.py", "--batch", batch_file],
                    capture_output=True, text=True, timeout=120
                )
                
                if result.returncode == 0:
                    self.log_test("CLI Batch Mode", "PASS", "Batch file processed successfully")
                else:
                    self.log_test("CLI Batch Mode", "FAIL", f"Return code: {result.returncode}")
                    
            finally:
                os.unlink(batch_file)
                
        except Exception as e:
            self.log_test("CLI Batch Mode", "FAIL", str(e))
    
    def test_query_interface_functionality(self):
        """Test the query interface directly"""
        try:
            from src.claude_query_interface import ClaudeQueryInterface
            
            interface = ClaudeQueryInterface()
            
            # Test basic query
            result = interface.execute_natural_language_query("count total signals recorded")
            
            if result['success'] and result['count'] > 0:
                self.log_test("Query Interface Basic", "PASS", f"Query returned {result['count']} results")
            else:
                self.log_test("Query Interface Basic", "FAIL", result.get('error', 'Unknown error'))
            
            # Test time-based query
            result = interface.execute_natural_language_query("show me alerts from last week")
            
            if result['success']:
                self.log_test("Query Interface Time-based", "PASS", f"Time query returned {result['count']} results")
            else:
                self.log_test("Query Interface Time-based", "FAIL", result.get('error', 'Unknown error'))
            
            # Test threshold query
            result = interface.execute_natural_language_query("which signals crossed the value limits last week?")
            
            if result['success']:
                self.log_test("Query Interface Threshold", "PASS", f"Threshold query returned {result['count']} results")
            else:
                self.log_test("Query Interface Threshold", "FAIL", result.get('error', 'Unknown error'))
            
            interface.close()
            
        except Exception as e:
            self.log_test("Query Interface Functionality", "FAIL", str(e))
    
    def test_domain_mapping(self):
        """Test domain mapping functionality"""
        try:
            from domain_mapping import DomainMapper
            
            mapper = DomainMapper()
            
            # Test table mapping
            signal_table = mapper.get_table_name("signal")
            if signal_table == "RepData":
                self.log_test("Domain Mapping Table", "PASS", "Signal maps to RepData")
            else:
                self.log_test("Domain Mapping Table", "FAIL", f"Signal maps to {signal_table}")
            
            # Test business term resolution
            crossed_terms = mapper.resolve_business_term("crossed")
            if "exceeded" in crossed_terms:
                self.log_test("Domain Mapping Business Terms", "PASS", "Crossed resolves to exceeded")
            else:
                self.log_test("Domain Mapping Business Terms", "FAIL", f"Crossed resolves to {crossed_terms}")
            
            # Test reverse lookup
            signal_domains = mapper.reverse_lookup_table("RepData")
            if "signal" in signal_domains:
                self.log_test("Domain Mapping Reverse", "PASS", "RepData reverse maps to signal")
            else:
                self.log_test("Domain Mapping Reverse", "FAIL", f"RepData reverse maps to {signal_domains}")
                
        except Exception as e:
            self.log_test("Domain Mapping", "FAIL", str(e))
    
    def test_edge_cases(self):
        """Test edge cases and error handling"""
        try:
            from src.claude_query_interface import ClaudeQueryInterface
            
            interface = ClaudeQueryInterface()
            
            # Test empty query
            result = interface.execute_natural_language_query("")
            if not result['success']:
                self.log_test("Edge Case Empty Query", "PASS", "Empty query handled correctly")
            else:
                self.log_test("Edge Case Empty Query", "FAIL", "Empty query should fail")
            
            # Test invalid query
            result = interface.execute_natural_language_query("xyzabc123nonsense")
            if not result['success'] or result['count'] == 0:
                self.log_test("Edge Case Invalid Query", "PASS", "Invalid query handled correctly")
            else:
                self.log_test("Edge Case Invalid Query", "WARN", "Invalid query returned unexpected results")
            
            # Test very long query
            long_query = "show me all devices " * 100
            result = interface.execute_natural_language_query(long_query)
            self.log_test("Edge Case Long Query", "PASS" if result else "FAIL", "Long query processed")
            
            interface.close()
            
        except Exception as e:
            self.log_test("Edge Cases", "FAIL", str(e))
    
    def test_streamlit_imports(self):
        """Test if Streamlit app can be imported without errors"""
        try:
            # Test if we can import the Streamlit app
            import importlib.util
            spec = importlib.util.spec_from_file_location("iot_streamlit_app", "iot_streamlit_app.py")
            if spec is None:
                self.log_test("Streamlit Import", "FAIL", "Could not find streamlit app file")
                return
            
            # Try to import without executing main
            module = importlib.util.module_from_spec(spec)
            
            # Mock streamlit to avoid actual web server startup
            import sys
            if 'streamlit' not in sys.modules:
                sys.modules['streamlit'] = type(sys)('mock_streamlit')
                # Add necessary mock attributes
                sys.modules['streamlit'].set_page_config = lambda **kwargs: None
                sys.modules['streamlit'].markdown = lambda *args, **kwargs: None
                sys.modules['streamlit'].header = lambda *args, **kwargs: None
                sys.modules['streamlit'].sidebar = type(sys)('sidebar')
                sys.modules['streamlit'].columns = lambda n: [type(sys)('col') for _ in range(n)]
                sys.modules['streamlit'].button = lambda *args, **kwargs: False
                sys.modules['streamlit'].text_input = lambda *args, **kwargs: ""
                sys.modules['streamlit'].spinner = lambda *args, **kwargs: type(sys)('spinner')
                sys.modules['streamlit'].cache_resource = lambda f: f
                sys.modules['streamlit'].cache_data = lambda f: f
                
            spec.loader.exec_module(module)
            self.log_test("Streamlit Import", "PASS", "Streamlit app imports successfully")
            
        except Exception as e:
            self.log_test("Streamlit Import", "FAIL", str(e))
    
    def test_performance(self):
        """Test performance with larger queries"""
        try:
            from src.claude_query_interface import ClaudeQueryInterface
            
            interface = ClaudeQueryInterface()
            
            # Test query performance
            start_time = time.time()
            result = interface.execute_natural_language_query("show me all signals from last week")
            duration = time.time() - start_time
            
            if result['success'] and duration < 10:  # Should complete in under 10 seconds
                self.log_test("Performance Test", "PASS", f"Query completed in {duration:.3f}s")
            elif result['success']:
                self.log_test("Performance Test", "WARN", f"Query slow: {duration:.3f}s")
            else:
                self.log_test("Performance Test", "FAIL", result.get('error', 'Unknown error'))
            
            interface.close()
            
        except Exception as e:
            self.log_test("Performance Test", "FAIL", str(e))
    
    def run_all_tests(self):
        """Run all tests"""
        print("🧪 Starting IoT Database Frontend Test Suite")
        print("=" * 60)
        
        # Core functionality tests
        print("\n📊 Testing Core Functionality:")
        self.test_database_exists()
        self.test_query_interface_functionality()
        self.test_domain_mapping()
        
        # CLI tests
        print("\n💻 Testing CLI Frontend:")
        self.test_cli_basic_functionality()
        self.test_cli_json_output()
        self.test_cli_batch_mode()
        
        # Web interface tests
        print("\n🌐 Testing Web Frontend:")
        self.test_streamlit_imports()
        
        # Edge cases and performance
        print("\n🔍 Testing Edge Cases and Performance:")
        self.test_edge_cases()
        self.test_performance()
        
        # Summary
        self.print_summary()
    
    def print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 60)
        print("📋 Test Summary")
        print("=" * 60)
        
        total = len(self.test_results)
        passed = len([r for r in self.test_results if r['status'] == 'PASS'])
        failed = len([r for r in self.test_results if r['status'] == 'FAIL'])
        warnings = len([r for r in self.test_results if r['status'] == 'WARN'])
        
        print(f"Total Tests: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"⚠️  Warnings: {warnings}")
        print(f"Success Rate: {(passed/total)*100:.1f}%" if total > 0 else "N/A")
        
        if failed > 0:
            print("\n❌ Failed Tests:")
            for result in self.test_results:
                if result['status'] == 'FAIL':
                    print(f"  • {result['test_name']}: {result['details']}")
        
        if warnings > 0:
            print("\n⚠️  Warnings:")
            for result in self.test_results:
                if result['status'] == 'WARN':
                    print(f"  • {result['test_name']}: {result['details']}")
        
        print(f"\n{'🎉 All tests completed successfully!' if failed == 0 else '⚠️  Some tests failed - check details above'}")
        
        # Save detailed results
        with open('test_results.json', 'w') as f:
            json.dump(self.test_results, f, indent=2)
        print(f"\n📄 Detailed results saved to: test_results.json")

def main():
    """Run the test suite"""
    tester = FrontendTester()
    tester.run_all_tests()

if __name__ == "__main__":
    main()