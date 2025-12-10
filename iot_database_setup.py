#!/usr/bin/env python3
import sqlite3
import random
import datetime
from typing import Dict, List
import json

class IoTDatabaseSetup:
    def __init__(self, db_path: str = "iot_production.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
    def create_tables(self):
        """Create IoT database tables with non-descriptive names"""
        
        # RepData -> Signal Data
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS RepData (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                sensor_type TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                quality_flag INTEGER DEFAULT 1,
                location_id TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # RepItem -> Log (aggregated signals/calculations)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS RepItem (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_type TEXT NOT NULL,
                source_signal_ids TEXT,
                calculated_value REAL,
                calculation_method TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'active',
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # DevMap -> Device Configuration
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS DevMap (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT UNIQUE NOT NULL,
                device_name TEXT,
                location TEXT,
                device_type TEXT,
                install_date DATE,
                status TEXT DEFAULT 'online',
                config_params TEXT,
                last_seen DATETIME
            )
        """)
        
        # ThreshSet -> Threshold/Limits Configuration
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ThreshSet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_type TEXT NOT NULL,
                device_id TEXT,
                min_value REAL,
                max_value REAL,
                warning_low REAL,
                warning_high REAL,
                critical_low REAL,
                critical_high REAL,
                active BOOLEAN DEFAULT TRUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # AlertLog -> Alert History
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS AlertLog (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                sensor_type TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                threshold_value REAL,
                actual_value REAL,
                severity TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                acknowledged BOOLEAN DEFAULT FALSE,
                ack_timestamp DATETIME,
                ack_user TEXT
            )
        """)
        
        # LocRef -> Location Reference
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS LocRef (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id TEXT UNIQUE NOT NULL,
                location_name TEXT,
                building TEXT,
                floor INTEGER,
                zone TEXT,
                coordinates TEXT,
                description TEXT
            )
        """)
        
        self.conn.commit()
        
    def generate_sample_data(self):
        """Generate realistic IoT production data"""
        
        # Sample locations
        locations = [
            ("LOC001", "Factory Floor A", "Main Building", 1, "Zone A", "40.7128,-74.0060", "Main production area"),
            ("LOC002", "Warehouse B", "Storage Building", 0, "Zone B", "40.7589,-73.9851", "Storage facility"),
            ("LOC003", "Office Wing C", "Admin Building", 2, "Zone C", "40.7614,-73.9776", "Administrative offices"),
            ("LOC004", "Lab Section D", "Research Building", 3, "Zone D", "40.7505,-73.9934", "R&D laboratory"),
            ("LOC005", "Loading Dock E", "Logistics Building", 0, "Zone E", "40.7282,-73.7949", "Shipping/receiving")
        ]
        
        for loc in locations:
            self.cursor.execute("""
                INSERT OR REPLACE INTO LocRef 
                (location_id, location_name, building, floor, zone, coordinates, description)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, loc)
        
        # Sample devices
        devices = [
            ("DEV001", "Temperature Sensor Alpha", "LOC001", "temperature_sensor", "2023-01-15", "online"),
            ("DEV002", "Humidity Monitor Beta", "LOC001", "humidity_sensor", "2023-01-16", "online"),
            ("DEV003", "Pressure Gauge Gamma", "LOC002", "pressure_sensor", "2023-01-20", "online"),
            ("DEV004", "Vibration Detector Delta", "LOC003", "vibration_sensor", "2023-02-01", "maintenance"),
            ("DEV005", "Air Quality Sensor Epsilon", "LOC004", "air_quality_sensor", "2023-02-10", "online"),
            ("DEV006", "Power Monitor Zeta", "LOC005", "power_meter", "2023-02-15", "online"),
            ("DEV007", "Flow Meter Eta", "LOC001", "flow_sensor", "2023-03-01", "online"),
            ("DEV008", "Light Sensor Theta", "LOC002", "light_sensor", "2023-03-05", "offline"),
            ("DEV009", "Motion Detector Iota", "LOC003", "motion_sensor", "2023-03-10", "online"),
            ("DEV010", "Sound Level Meter Kappa", "LOC004", "sound_sensor", "2023-03-15", "online")
        ]
        
        for device in devices:
            config = json.dumps({
                "sampling_rate": random.randint(1, 60),
                "calibration_date": "2023-06-15",
                "firmware_version": f"v{random.randint(1,5)}.{random.randint(0,9)}"
            })
            self.cursor.execute("""
                INSERT OR REPLACE INTO DevMap 
                (device_id, device_name, location, device_type, install_date, status, config_params, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (*device, config, datetime.datetime.now()))
        
        # Generate threshold settings
        sensor_types = ["temperature_sensor", "humidity_sensor", "pressure_sensor", "vibration_sensor", 
                       "air_quality_sensor", "power_meter", "flow_sensor", "light_sensor", "sound_sensor"]
        
        thresholds = {
            "temperature_sensor": (15, 35, 18, 32, 10, 40),
            "humidity_sensor": (30, 70, 35, 65, 20, 80),
            "pressure_sensor": (0.8, 1.2, 0.85, 1.15, 0.7, 1.3),
            "vibration_sensor": (0, 50, 5, 45, 0, 60),
            "air_quality_sensor": (0, 100, 10, 90, 0, 150),
            "power_meter": (100, 1000, 150, 950, 50, 1200),
            "flow_sensor": (0, 100, 10, 90, 0, 120),
            "light_sensor": (100, 1000, 150, 900, 50, 1200),
            "sound_sensor": (30, 85, 35, 80, 25, 90)
        }
        
        for sensor_type, (min_val, max_val, warn_low, warn_high, crit_low, crit_high) in thresholds.items():
            self.cursor.execute("""
                INSERT OR REPLACE INTO ThreshSet 
                (sensor_type, min_value, max_value, warning_low, warning_high, critical_low, critical_high)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (sensor_type, min_val, max_val, warn_low, warn_high, crit_low, crit_high))
        
        # Generate signal data for the past 30 days
        start_date = datetime.datetime.now() - datetime.timedelta(days=30)
        
        for i in range(50000):  # Generate 50k data points
            device_id = random.choice([d[0] for d in devices])
            device_info = next(d for d in devices if d[0] == device_id)
            sensor_type = device_info[3]
            location = device_info[2]
            
            # Generate realistic values based on sensor type
            if "temperature" in sensor_type:
                value = random.gauss(25, 5)
                unit = "°C"
            elif "humidity" in sensor_type:
                value = random.gauss(50, 15)
                unit = "%"
            elif "pressure" in sensor_type:
                value = random.gauss(1.0, 0.1)
                unit = "bar"
            elif "vibration" in sensor_type:
                value = random.gauss(20, 10)
                unit = "Hz"
            elif "air_quality" in sensor_type:
                value = random.gauss(50, 20)
                unit = "AQI"
            elif "power" in sensor_type:
                value = random.gauss(500, 100)
                unit = "W"
            elif "flow" in sensor_type:
                value = random.gauss(50, 15)
                unit = "L/min"
            elif "light" in sensor_type:
                value = random.gauss(500, 200)
                unit = "lux"
            elif "sound" in sensor_type:
                value = random.gauss(60, 15)
                unit = "dB"
            else:
                value = random.gauss(50, 10)
                unit = "units"
            
            # Add some anomalies
            if random.random() < 0.05:  # 5% chance of anomaly
                value *= random.choice([0.3, 2.5])  # Very low or very high
            
            timestamp = start_date + datetime.timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            quality_flag = 1 if random.random() > 0.02 else 0  # 2% bad quality
            
            self.cursor.execute("""
                INSERT INTO RepData 
                (device_id, sensor_type, value, unit, timestamp, quality_flag, location_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (device_id, sensor_type, value, unit, timestamp, quality_flag, location))
        
        # Generate some aggregated log entries
        for i in range(1000):
            log_types = ["daily_average", "hourly_max", "anomaly_detection", "efficiency_calc"]
            log_type = random.choice(log_types)
            
            source_ids = ",".join([str(random.randint(1, 1000)) for _ in range(random.randint(1, 5))])
            
            if log_type == "daily_average":
                calc_value = random.gauss(25, 5)
                calc_method = "AVG"
            elif log_type == "hourly_max":
                calc_value = random.gauss(40, 10)
                calc_method = "MAX"
            elif log_type == "anomaly_detection":
                calc_value = random.choice([0, 1])
                calc_method = "ANOMALY_SCORE"
            else:
                calc_value = random.gauss(0.85, 0.15)
                calc_method = "EFFICIENCY_RATIO"
            
            timestamp = start_date + datetime.timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23)
            )
            
            metadata = json.dumps({
                "calculation_params": {"window_size": random.randint(1, 24)},
                "data_quality": random.choice(["high", "medium", "low"])
            })
            
            self.cursor.execute("""
                INSERT INTO RepItem 
                (log_type, source_signal_ids, calculated_value, calculation_method, timestamp, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (log_type, source_ids, calc_value, calc_method, timestamp, metadata))
        
        # Generate some alerts
        for i in range(200):
            device_id = random.choice([d[0] for d in devices])
            device_info = next(d for d in devices if d[0] == device_id)
            sensor_type = device_info[3]
            
            alert_types = ["threshold_exceeded", "sensor_offline", "data_quality_low", "anomaly_detected"]
            alert_type = random.choice(alert_types)
            
            severity = random.choice(["low", "medium", "high", "critical"])
            threshold_val = random.uniform(20, 80)
            actual_val = threshold_val * random.uniform(1.1, 2.0) if alert_type == "threshold_exceeded" else None
            
            timestamp = start_date + datetime.timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            
            acknowledged = random.choice([True, False])
            ack_time = timestamp + datetime.timedelta(hours=random.randint(1, 48)) if acknowledged else None
            ack_user = random.choice(["admin", "operator1", "manager", "tech1"]) if acknowledged else None
            
            self.cursor.execute("""
                INSERT INTO AlertLog 
                (device_id, sensor_type, alert_type, threshold_value, actual_value, severity, 
                 timestamp, acknowledged, ack_timestamp, ack_user)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (device_id, sensor_type, alert_type, threshold_val, actual_val, severity, 
                  timestamp, acknowledged, ack_time, ack_user))
        
        self.conn.commit()
        print(f"Generated {self.cursor.rowcount} records across all tables")

def main():
    db_setup = IoTDatabaseSetup()
    db_setup.create_tables()
    db_setup.generate_sample_data()
    db_setup.conn.close()
    print("IoT database created successfully with sample data!")

if __name__ == "__main__":
    main()