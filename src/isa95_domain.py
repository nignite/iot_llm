"""
ISA-95 Manufacturing Operations Management Domain Knowledge
Enhances the IoT database system with ISA-95 industry standard knowledge
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class ISA95DomainKnowledge:
    """
    ISA-95 Manufacturing Execution Systems domain knowledge
    Provides industry-standard vocabulary, patterns, and query enhancement
    """
    
    def __init__(self):
        self.hierarchy_levels = self._load_hierarchy_levels()
        self.vocabulary = self._load_vocabulary()
        self.activity_types = self._load_activity_types()
        self.query_patterns = self._load_query_patterns()
        self.common_metrics = self._load_common_metrics()
    
    def _load_hierarchy_levels(self) -> Dict[str, Any]:
        """ISA-95 Equipment and Functional Hierarchy"""
        return {
            "equipment_hierarchy": {
                "enterprise": {
                    "level": 0,
                    "description": "Entire organization or company",
                    "examples": ["Corporation", "Company", "Organization"],
                    "typical_attributes": ["name", "location", "business_type"]
                },
                "site": {
                    "level": 1, 
                    "description": "Physical location or facility",
                    "examples": ["Plant", "Factory", "Facility", "Campus"],
                    "typical_attributes": ["site_id", "location", "capacity", "status"]
                },
                "area": {
                    "level": 2,
                    "description": "Operational division within a site",
                    "examples": ["Production Area", "Warehouse", "Quality Lab", "Packaging"],
                    "typical_attributes": ["area_id", "area_name", "area_type", "supervisor"]
                },
                "process_cell": {
                    "level": 3,
                    "description": "Basic manufacturing unit within an area",
                    "examples": ["Assembly Line", "Reactor", "Mixing Cell", "Packaging Line"],
                    "typical_attributes": ["cell_id", "cell_name", "capacity", "product_type"]
                },
                "unit": {
                    "level": 4,
                    "description": "Distinct part performing specific function",
                    "examples": ["Mixer", "Heater", "Conveyor", "Robot"],
                    "typical_attributes": ["unit_id", "unit_name", "function", "status"]
                },
                "equipment_module": {
                    "level": 5,
                    "description": "Modular equipment for specific tasks",
                    "examples": ["Pump", "Motor", "Valve", "Sensor"],
                    "typical_attributes": ["module_id", "module_type", "status", "location"]
                },
                "control_module": {
                    "level": 6,
                    "description": "Individual control devices",
                    "examples": ["Temperature Sensor", "Flow Meter", "PID Controller"],
                    "typical_attributes": ["device_id", "device_type", "value", "timestamp"]
                }
            },
            "functional_hierarchy": {
                "level_4": "Enterprise Systems (ERP)",
                "level_3": "Manufacturing Operations Management (MOM/MES)", 
                "level_2": "Supervisory Control (SCADA/HMI)",
                "level_1": "Automation Control (PLC/DCS)",
                "level_0": "Physical Processes"
            }
        }
    
    def _load_vocabulary(self) -> Dict[str, Any]:
        """ISA-95 Manufacturing vocabulary mappings"""
        return {
            "manufacturing_terms": {
                # Production Management
                "work_order": ["production_order", "job", "batch", "lot"],
                "recipe": ["procedure", "formula", "process_definition"],
                "material": ["raw_material", "ingredient", "component", "product"],
                "equipment": ["asset", "machine", "device", "unit"],
                "personnel": ["operator", "worker", "staff", "technician"],
                
                # Operations
                "production": ["manufacturing", "processing", "assembly"],
                "maintenance": ["repair", "service", "upkeep", "preventive"],
                "quality": ["inspection", "testing", "validation", "compliance"],
                "inventory": ["stock", "warehouse", "storage", "materials"],
                
                # Status and States
                "available": ["ready", "idle", "standby"],
                "running": ["executing", "active", "producing"],
                "held": ["paused", "suspended", "stopped"],
                "unavailable": ["down", "offline", "maintenance"],
                
                # Time Concepts
                "cycle_time": ["processing_time", "duration"],
                "setup_time": ["changeover_time", "preparation_time"],
                "downtime": ["outage", "stoppage", "failure_time"],
                "efficiency": ["utilization", "performance", "effectiveness"],
                
                # Quality Terms
                "defect": ["fault", "error", "non_conformance"],
                "yield": ["output", "production_rate", "throughput"],
                "specification": ["requirement", "standard", "limit"],
                "batch": ["lot", "run", "campaign"]
            },
            
            "sql_mappings": {
                # Equipment hierarchy mappings
                "equipment": "DevMap",
                "devices": "DevMap", 
                "machines": "DevMap",
                "sensors": "DevMap",
                
                # Data mappings
                "readings": "RepData",
                "measurements": "RepData",
                "values": "RepData",
                "data": "RepData",
                
                # Alert mappings
                "alerts": "AlertLog",
                "alarms": "AlertLog",
                "notifications": "AlertLog",
                "warnings": "AlertLog",
                
                # Status mappings
                "status": "status",
                "state": "status", 
                "condition": "status",
                
                # Time mappings
                "timestamp": "timestamp",
                "time": "timestamp",
                "when": "timestamp",
                "date": "timestamp"
            }
        }
    
    def _load_activity_types(self) -> Dict[str, Any]:
        """ISA-95 Manufacturing Operations Management activities"""
        return {
            "production_activities": {
                "production_scheduling": {
                    "description": "Planning and scheduling of production orders",
                    "typical_queries": [
                        "show production schedule for today",
                        "what orders are running in area 1", 
                        "which batches are behind schedule"
                    ]
                },
                "production_execution": {
                    "description": "Executing and tracking production operations",
                    "typical_queries": [
                        "show current production status",
                        "what is the yield of batch 123",
                        "which units are currently producing"
                    ]
                },
                "production_tracking": {
                    "description": "Collecting and reporting production data",
                    "typical_queries": [
                        "show production data for last week",
                        "what was the output of line 2 yesterday",
                        "track material consumption for order 456"
                    ]
                }
            },
            
            "maintenance_activities": {
                "maintenance_scheduling": {
                    "description": "Planning preventive and corrective maintenance",
                    "typical_queries": [
                        "show scheduled maintenance for next week",
                        "which equipment needs preventive maintenance",
                        "what maintenance is overdue"
                    ]
                },
                "maintenance_execution": {
                    "description": "Performing maintenance activities",
                    "typical_queries": [
                        "show active maintenance work orders",
                        "which technician is working on pump 101",
                        "what maintenance was completed today"
                    ]
                }
            },
            
            "quality_activities": {
                "quality_testing": {
                    "description": "Testing and inspection activities",
                    "typical_queries": [
                        "show quality test results for batch 789",
                        "which products failed inspection",
                        "what are the current quality metrics"
                    ]
                },
                "quality_control": {
                    "description": "Monitoring and controlling quality parameters",
                    "typical_queries": [
                        "show out-of-spec readings",
                        "which parameters exceeded limits",
                        "what is the defect rate for product A"
                    ]
                }
            },
            
            "inventory_activities": {
                "inventory_tracking": {
                    "description": "Tracking material movements and levels",
                    "typical_queries": [
                        "show current inventory levels",
                        "which materials are low in stock", 
                        "track material usage for last month"
                    ]
                }
            }
        }
    
    def _load_query_patterns(self) -> Dict[str, Any]:
        """Common ISA-95 query patterns and templates"""
        return {
            "equipment_status": {
                "pattern": "SELECT equipment_info FROM equipment_table WHERE status_condition",
                "examples": [
                    "show all equipment that is offline",
                    "which machines are in maintenance mode",
                    "what is the status of line 3 equipment"
                ]
            },
            
            "production_performance": {
                "pattern": "SELECT performance_metrics FROM production_data WHERE time_period AND location",
                "examples": [
                    "show production efficiency for last week",
                    "what was the yield of area 2 yesterday",
                    "calculate OEE for all lines this month"
                ]
            },
            
            "quality_monitoring": {
                "pattern": "SELECT quality_parameters FROM quality_data WHERE specification_limits",
                "examples": [
                    "show readings that exceeded specifications",
                    "which batches failed quality tests",
                    "what is the defect rate trend"
                ]
            },
            
            "maintenance_tracking": {
                "pattern": "SELECT maintenance_info FROM maintenance_data WHERE equipment AND time_period",
                "examples": [
                    "show maintenance history for pump 101",
                    "which equipment had unplanned downtime",
                    "what maintenance is scheduled for next week"
                ]
            },
            
            "material_tracking": {
                "pattern": "SELECT material_info FROM inventory_data WHERE material_type AND location",
                "examples": [
                    "show current raw material levels",
                    "which materials were consumed in batch 456",
                    "track material movements in warehouse A"
                ]
            }
        }
    
    def _load_common_metrics(self) -> Dict[str, Any]:
        """ISA-95 common manufacturing metrics and KPIs"""
        return {
            "production_metrics": {
                "oee": {
                    "name": "Overall Equipment Effectiveness",
                    "formula": "Availability × Performance × Quality",
                    "description": "Comprehensive measure of manufacturing effectiveness"
                },
                "availability": {
                    "name": "Equipment Availability", 
                    "formula": "(Operating Time / Planned Production Time) × 100",
                    "description": "Percentage of time equipment is available for production"
                },
                "performance": {
                    "name": "Performance Efficiency",
                    "formula": "(Actual Output / Theoretical Output) × 100", 
                    "description": "How fast the equipment runs compared to its theoretical maximum"
                },
                "quality": {
                    "name": "Quality Rate",
                    "formula": "(Good Units / Total Units) × 100",
                    "description": "Percentage of units produced without defects"
                },
                "yield": {
                    "name": "Production Yield",
                    "formula": "(Actual Output / Expected Output) × 100",
                    "description": "Efficiency of converting raw materials to finished products"
                },
                "throughput": {
                    "name": "Production Throughput", 
                    "formula": "Units Produced / Time Period",
                    "description": "Rate of production output"
                }
            },
            
            "maintenance_metrics": {
                "mtbf": {
                    "name": "Mean Time Between Failures",
                    "formula": "Total Operating Time / Number of Failures",
                    "description": "Average time between equipment failures"
                },
                "mttr": {
                    "name": "Mean Time To Repair",
                    "formula": "Total Repair Time / Number of Repairs", 
                    "description": "Average time to complete repairs"
                },
                "planned_maintenance": {
                    "name": "Planned Maintenance Percentage",
                    "formula": "(Planned Maintenance Hours / Total Maintenance Hours) × 100",
                    "description": "Percentage of maintenance that is planned vs reactive"
                }
            },
            
            "quality_metrics": {
                "defect_rate": {
                    "name": "Defect Rate",
                    "formula": "(Defective Units / Total Units) × 100",
                    "description": "Percentage of units that do not meet quality standards"
                },
                "first_pass_yield": {
                    "name": "First Pass Yield", 
                    "formula": "(Units Passed First Time / Total Units) × 100",
                    "description": "Percentage of units that pass quality tests on first attempt"
                }
            }
        }
    
    def enhance_query_context(self, query: str, base_context: str) -> str:
        """Enhance database context with ISA-95 domain knowledge"""
        
        isa95_context = f"""

ISA-95 Manufacturing Domain Knowledge:

Equipment Hierarchy Levels:
- Enterprise → Site → Area → Process Cell → Unit → Equipment Module → Control Module
- Your IoT devices typically map to Equipment Modules (sensors, actuators) and Control Modules (individual sensors)

Manufacturing Activities (MOM Functions):
- Production Operations: Scheduling, execution, tracking
- Maintenance Operations: Preventive, corrective, predictive  
- Quality Operations: Testing, inspection, control
- Inventory Operations: Material tracking, consumption

Common Manufacturing Terms:
- Equipment Status: available, running, held, unavailable (offline)
- Production: work orders, batches, recipes, yield, throughput
- Quality: specifications, defects, first-pass yield, out-of-spec
- Maintenance: MTBF, MTTR, planned vs unplanned downtime
- Performance: OEE (Overall Equipment Effectiveness), availability, efficiency

Key Performance Indicators:
- OEE = Availability × Performance × Quality  
- Availability = Operating Time / Planned Time
- Yield = Good Output / Total Output
- Defect Rate = Defective Units / Total Units

Typical Manufacturing Queries:
- "Show OEE for production line 1 last week"
- "Which equipment had unplanned downtime yesterday"  
- "What batches exceeded quality specifications"
- "Show maintenance schedule for area 2"
- "Calculate yield for work order 12345"

"""
        
        return base_context + isa95_context
    
    def map_manufacturing_terms(self, query: str) -> str:
        """Map manufacturing terms to database-specific vocabulary"""
        mapped_query = query.lower()
        
        # Apply manufacturing vocabulary mappings
        for standard_term, synonyms in self.vocabulary["manufacturing_terms"].items():
            for synonym in synonyms:
                if synonym in mapped_query:
                    mapped_query = mapped_query.replace(synonym, standard_term)
        
        return mapped_query
    
    def suggest_isa95_queries(self, table_context: Dict[str, Any]) -> List[str]:
        """Suggest ISA-95 relevant queries based on available tables"""
        suggestions = []
        
        # Check available tables and suggest relevant queries
        available_tables = table_context.keys()
        
        if any("dev" in table.lower() or "equipment" in table.lower() for table in available_tables):
            suggestions.extend([
                "Show equipment availability for production area 1",
                "Which devices are currently offline or in maintenance?",
                "Calculate equipment utilization for last week",
                "Show status of all temperature sensors in the plant"
            ])
        
        if any("rep" in table.lower() or "data" in table.lower() for table in available_tables):
            suggestions.extend([
                "Show production data that exceeded specifications",
                "Calculate average cycle time for process cell 1", 
                "What was the throughput for line 2 yesterday?",
                "Show quality parameters out of specification"
            ])
        
        if any("alert" in table.lower() or "alarm" in table.lower() for table in available_tables):
            suggestions.extend([
                "Show all critical alarms from last 24 hours",
                "Which equipment triggered maintenance alerts?",
                "Calculate mean time between failures for pumps",
                "Show quality alerts for batch production"
            ])
        
        return suggestions
    
    def get_manufacturing_context(self) -> Dict[str, Any]:
        """Get comprehensive ISA-95 manufacturing context"""
        return {
            "domain": "ISA-95 Manufacturing Operations Management",
            "hierarchy": self.hierarchy_levels,
            "vocabulary": self.vocabulary,
            "activities": self.activity_types,
            "patterns": self.query_patterns,
            "metrics": self.common_metrics,
            "standard_version": "ISA-95 (ANSI/ISA-95.00.01-2010)"
        }


class ISA95QueryEnhancer:
    """
    Enhances queries with ISA-95 manufacturing intelligence
    """
    
    def __init__(self, domain_knowledge: ISA95DomainKnowledge):
        self.domain = domain_knowledge
    
    def enhance_sql_prompt(self, base_prompt: str, query: str, schema: Dict) -> str:
        """Enhance SQL generation prompt with ISA-95 context"""
        
        # Map manufacturing terms
        enhanced_query = self.domain.map_manufacturing_terms(query)
        
        # Add ISA-95 context
        isa95_context = self.domain.enhance_query_context(enhanced_query, "")
        
        # Build enhanced prompt
        enhanced_prompt = f"""{base_prompt}

{isa95_context}

IMPORTANT ISA-95 Guidelines:
1. Consider equipment hierarchy when querying device/equipment data
2. Use standard manufacturing terminology in column aliases
3. For time-based queries, consider production shifts and schedules
4. Include relevant KPIs (OEE, availability, yield) when appropriate
5. Handle manufacturing states: available, running, held, unavailable
6. Consider batch/lot tracking for production queries
7. Apply quality specifications and control limits

Enhanced Query: {enhanced_query}

Generate SQL that follows ISA-95 manufacturing principles:"""
        
        return enhanced_prompt
    
    def suggest_manufacturing_insights(self, query_result: Dict) -> List[str]:
        """Suggest manufacturing insights based on query results"""
        insights = []
        
        if not query_result.get('success') or not query_result.get('results'):
            return insights
        
        results = query_result['results']
        
        # Analyze equipment status distribution
        if any('status' in str(row).lower() for row in results):
            status_counts = {}
            for row in results:
                for key, value in row.items():
                    if 'status' in key.lower() and value:
                        status_counts[value] = status_counts.get(value, 0) + 1
            
            if status_counts:
                total = sum(status_counts.values())
                offline_count = status_counts.get('offline', 0) + status_counts.get('unavailable', 0)
                if offline_count > 0:
                    availability = ((total - offline_count) / total) * 100
                    insights.append(f"Equipment Availability: {availability:.1f}% ({offline_count}/{total} equipment offline)")
        
        # Analyze time-based patterns
        if any('timestamp' in str(row).lower() for row in results):
            insights.append("Consider analyzing production patterns by shift or time period for better insights")
        
        # Suggest manufacturing KPIs
        if len(results) > 1:
            insights.append("Available Manufacturing KPIs: OEE, Availability, Performance, Quality Rate")
        
        return insights