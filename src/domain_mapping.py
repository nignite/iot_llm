#!/usr/bin/env python3
"""
Domain Name to Table Name Mapping System for IoT Database
Maps business/domain terminology to actual database table names
"""

class DomainMapper:
    def __init__(self):
        self.table_mappings = {
            # Signal Data Mappings
            "signal": "RepData",
            "signals": "RepData", 
            "signal_data": "RepData",
            "sensor_data": "RepData",
            "sensor_readings": "RepData",
            "measurements": "RepData",
            "readings": "RepData",
            "data_points": "RepData",
            
            # Log/Aggregated Data Mappings
            "log": "RepItem",
            "logs": "RepItem",
            "calculated_data": "RepItem",
            "aggregated_data": "RepItem",
            "computed_values": "RepItem",
            "analytics": "RepItem",
            "calculations": "RepItem",
            "aggregations": "RepItem",
            
            # Device Mappings
            "device": "DevMap",
            "devices": "DevMap",
            "device_config": "DevMap",
            "device_configuration": "DevMap",
            "equipment": "DevMap",
            "sensors": "DevMap",
            "hardware": "DevMap",
            
            # Threshold/Limits Mappings
            "threshold": "ThreshSet",
            "thresholds": "ThreshSet",
            "limits": "ThreshSet",
            "boundaries": "ThreshSet",
            "constraints": "ThreshSet",
            "ranges": "ThreshSet",
            "setpoints": "ThreshSet",
            
            # Alert Mappings
            "alert": "AlertLog",
            "alerts": "AlertLog",
            "alarm": "AlertLog",
            "alarms": "AlertLog",
            "notifications": "AlertLog",
            "incidents": "AlertLog",
            "warnings": "AlertLog",
            "violations": "AlertLog",
            
            # Location Mappings
            "location": "LocRef",
            "locations": "LocRef",
            "site": "LocRef",
            "sites": "LocRef",
            "facility": "LocRef",
            "facilities": "LocRef",
            "area": "LocRef",
            "areas": "LocRef",
            "zone": "LocRef",
            "zones": "LocRef"
        }
        
        self.column_mappings = {
            "RepData": {
                "timestamp": ["time", "datetime", "recorded_at", "measured_at", "captured_at"],
                "device_id": ["device", "sensor_id", "equipment_id", "hardware_id"],
                "value": ["measurement", "reading", "data", "signal_value"],
                "sensor_type": ["type", "kind", "category", "sensor_kind"],
                "location_id": ["location", "site", "area", "zone"],
                "quality_flag": ["quality", "validity", "status", "health"]
            },
            "RepItem": {
                "log_type": ["type", "kind", "category"],
                "calculated_value": ["value", "result", "output"],
                "calculation_method": ["method", "algorithm", "function"],
                "source_signal_ids": ["sources", "inputs", "dependencies"]
            },
            "DevMap": {
                "device_id": ["device", "id", "identifier"],
                "device_name": ["name", "label", "description"],
                "location": ["site", "area", "zone"],
                "device_type": ["type", "kind", "category"],
                "status": ["state", "condition", "health"]
            },
            "ThreshSet": {
                "sensor_type": ["type", "kind", "category"],
                "min_value": ["minimum", "lower_bound", "min_limit"],
                "max_value": ["maximum", "upper_bound", "max_limit"],
                "warning_low": ["warn_low", "warning_minimum"],
                "warning_high": ["warn_high", "warning_maximum"]
            },
            "AlertLog": {
                "alert_type": ["type", "kind", "category"],
                "severity": ["level", "priority", "criticality"],
                "actual_value": ["measured_value", "current_value", "observed_value"],
                "threshold_value": ["limit", "boundary", "setpoint"]
            },
            "LocRef": {
                "location_id": ["id", "identifier", "code"],
                "location_name": ["name", "label", "description"],
                "building": ["facility", "structure"],
                "zone": ["area", "section", "region"]
            }
        }
        
        self.business_terms = {
            "crossed": ["exceeded", "violated", "breached", "surpassed"],
            "limits": ["thresholds", "boundaries", "constraints", "ranges"],
            "last week": ["past week", "previous week", "7 days ago"],
            "yesterday": ["past day", "previous day", "1 day ago"],
            "today": ["current day", "this day"],
            "offline": ["disconnected", "unavailable", "down"],
            "online": ["connected", "available", "up", "active"],
            "high": ["elevated", "maximum", "peak", "critical"],
            "low": ["minimum", "reduced", "drop", "critical"],
            "average": ["mean", "typical", "normal"],
            "trend": ["pattern", "direction", "movement"],
            "anomaly": ["outlier", "unusual", "abnormal", "deviation"]
        }
    
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
    
    def get_all_mappings(self) -> dict:
        """Return all mappings for reference"""
        return {
            "table_mappings": self.table_mappings,
            "column_mappings": self.column_mappings,
            "business_terms": self.business_terms
        }
    
    def reverse_lookup_table(self, table_name: str) -> list:
        """Find all domain terms that map to a specific table"""
        return [domain for domain, table in self.table_mappings.items() if table == table_name]

def main():
    mapper = DomainMapper()
    
    print("Domain to Table Mappings:")
    print("-" * 40)
    for domain, table in mapper.table_mappings.items():
        print(f"{domain:20} -> {table}")
    
    print("\nExample Usage:")
    print("-" * 40)
    print(f"'signal' maps to: {mapper.get_table_name('signal')}")
    print(f"'alerts' maps to: {mapper.get_table_name('alerts')}")
    print(f"'device_config' maps to: {mapper.get_table_name('device_config')}")
    
    print("\nBusiness Term Resolution:")
    print("-" * 40)
    print(f"'crossed' resolves to: {mapper.resolve_business_term('crossed')}")
    print(f"'limits' resolves to: {mapper.resolve_business_term('limits')}")

if __name__ == "__main__":
    main()