#!/usr/bin/env python3
"""
Populate Oracle-based IoT schema with realistic sample data
"""

import sqlite3
import random
import datetime
from typing import List

def populate_oracle_iot_db():
    """Populate the Oracle-based IoT database with sample data"""
    
    conn = sqlite3.connect('oracle_iot_db.db')
    cursor = conn.cursor()
    
    print("Populating Oracle-based IoT database...")
    
    # 1. Create Channel Groups
    channel_groups = [
        (1, 'Production_Line_A', 'ProdLineA', 1, 'Main production line sensors', 0, 0, 1000),
        (2, 'Warehouse_Sensors', 'Warehouse', 2, 'Warehouse monitoring systems', 1, 1, 500),
        (3, 'Quality_Control', 'QC_Lab', 3, 'Quality control laboratory', 0, 0, 2000),
        (4, 'HVAC_Systems', 'HVAC', 4, 'Building climate control', 1, 0, 1000),
        (5, 'Power_Monitoring', 'PowerMon', 5, 'Electrical systems monitoring', 0, 1, 100)
    ]
    
    for group in channel_groups:
        cursor.execute("""
            INSERT OR REPLACE INTO CHANNELGROUP 
            (GROUPNR, GROUPNAME, ALIASNAME, NODENR, DESCRIPTION, BUFFERED, FASTDATAAC, FDATIMESLOT, DEFDATE, ATCREATOR)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (*group, datetime.datetime.now(), 'system'))
    
    print("Created channel groups")
    
    # 2. Create Signal Channels
    channels = [
        (101, 'MODBUS_PLC_01', 'ModbusPLC1', 1, 0, 'D', 'Primary PLC connection', 1, 'plc-server-01', 'MODBUS_PROG', 'HDA_PROG', 1),
        (102, 'OPC_SERVER_01', 'OPCServ1', 2, 0, 'H', 'OPC Data Server', 2, 'opc-server-01', 'OPC_PROG', 'OPC_HDA', 1),
        (103, 'ETHERNET_IP_01', 'EtherNetIP1', 3, 0, 'D', 'EtherNet/IP Scanner', 3, 'ethernet-gateway', 'ENIP_PROG', 'ENIP_HDA', 1),
        (104, 'PROFINET_01', 'ProfiNet1', 4, 0, 'H', 'PROFINET IO Controller', 4, 'profinet-ctrl', 'PN_PROG', 'PN_HDA', 1),
        (105, 'TCP_MODBUS_01', 'TCPModbus1', 5, 0, 'D', 'TCP Modbus Gateway', 5, 'modbus-gateway', 'TCPMB_PROG', 'TCPMB_HDA', 1)
    ]
    
    for channel in channels:
        cursor.execute("""
            INSERT OR REPLACE INTO SIGNALCHANNEL 
            (CHANNR, CHANNAME, ALIASNAME, SIGPROTID, SLAVE, DEFLOGCLASS, CHANDESCR, GROUPNR, HOSTNAME, PROGID, HDAPROGID, EXTSYNCHRO, DEFDATE, ATCREATOR)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (*channel, datetime.datetime.now(), 'admin'))
    
    print("Created signal channels")
    
    # 3. Create Signal Items (Sensors)
    signal_types = [
        (1, 'ANALOG', 'Temperature'),
        (2, 'ANALOG', 'Pressure'), 
        (3, 'ANALOG', 'Flow'),
        (4, 'ANALOG', 'Level'),
        (5, 'DIGITAL', 'Status'),
        (6, 'ANALOG', 'Vibration'),
        (7, 'ANALOG', 'Power'),
        (8, 'ANALOG', 'Humidity')
    ]
    
    signals = []
    sigid = 1000
    
    for channr in [101, 102, 103, 104, 105]:
        for i in range(20):  # 20 signals per channel
            sig_type = random.choice(signal_types)
            signals.append((
                sigid,
                f'{sig_type[2]}_Sensor_{channr}_{i+1:02d}',
                f'{sig_type[2][0:4]}{i+1:02d}_{channr}',
                channr,
                1,  # IOGROUP
                sig_type[0],  # SIGTYPE (1=analog, 5=digital)
                1 if sig_type[1] == 'ANALOG' else 5,  # DATATYPE
                f'{sig_type[2]} measurement point {i+1}',
                '°C' if sig_type[2] == 'Temperature' else 'bar' if sig_type[2] == 'Pressure' else 'L/min' if sig_type[2] == 'Flow' else '%',
                1.0,
                0,
                datetime.datetime.now(),
                'system',
                datetime.datetime.now(),
                'admin',
                0.0 if sig_type[1] == 'DIGITAL' else -100.0,
                1.0 if sig_type[1] == 'DIGITAL' else 1000.0
            ))
            sigid += 1
    
    cursor.executemany("""
        INSERT OR REPLACE INTO SIGNALITEM 
        (SIGID, SIGNAME, ALIASNAME, CHANNR, IOGROUP, SIGTYPE, DATATYPE, OBJDESCR, OBJUNIT, OBJSCALE, 
         CTROVERFLOW, DEFDATE, ATMODIFIER, ATCREDATE, ATCREATOR, MINVALUE, MAXVALUE)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, signals)
    
    print(f"Created {len(signals)} signal items")
    
    # 4. Create Process Instances (time periods)
    start_date = datetime.datetime.now() - datetime.timedelta(days=30)
    process_instances = []
    pinstid = 1
    
    for day in range(30):  # 30 days of hourly instances
        for hour in range(24):
            pstart = start_date + datetime.timedelta(days=day, hours=hour)
            pend = pstart + datetime.timedelta(hours=1)
            
            process_instances.append((
                pinstid,
                pstart,
                pend,
                'H',  # Hourly
                1,    # Closed
                1,    # Valid
                f'Hour_{day+1:02d}_{hour:02d}',
                f'Hourly data collection for day {day+1}, hour {hour}',
                pstart
            ))
            pinstid += 1
    
    cursor.executemany("""
        INSERT OR REPLACE INTO PROCINSTANCE 
        (PINSTID, PINSTSTART, PINSTEND, PTYPE, CLOSED, VALID, PINSTNAME, DESCRIPTION, CREDATE)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, process_instances)
    
    print(f"Created {len(process_instances)} process instances")
    
    # 5. Create Report Items (calculations/aggregations)
    report_items = []
    ricode = 10000
    
    calc_types = [
        ('AVG_TEMP', 'Average Temperature', '°C', 'H'),
        ('MAX_PRESSURE', 'Maximum Pressure', 'bar', 'H'), 
        ('MIN_FLOW', 'Minimum Flow Rate', 'L/min', 'H'),
        ('SUM_POWER', 'Total Power Consumption', 'kWh', 'D'),
        ('COUNT_ALARMS', 'Alarm Count', 'count', 'H')
    ]
    
    for calc in calc_types:
        for group in range(1, 6):  # 5 groups
            report_items.append((
                ricode,
                'CAL',  # RICLASS - calculation
                'H' if calc[3] == 'H' else 'D',  # LOGCLASS
                f'{calc[1]} - Group {group}',
                f'{calc[0]}_GRP{group}',
                calc[2],  # Unit
                2,  # Decimals
                1.0,  # Scale
                f'PLC_{group}',  # PLC code
                None, None, None, None,  # Additional PLC codes
                1000 + group,  # VSIGID1
                None, None, None, None, None, None, None, None, None,  # Other signal IDs
                None, None, None,  # CSIGID
                None,  # SORTRICODE
                None, None, None, None, None, None, None,  # AUXNUMVAL
                None, None,  # AUXDATEVAL
                None, None,  # CUM fields
                None, None, None, None,  # Other fields
                0.1,  # DEADBAND
                0,  # STOPLOG
                f'Calculated {calc[1]} for equipment group {group}',
                f'CALC_{ricode}',  # EXTGUID
                datetime.datetime.now(),
                'system',
                datetime.datetime.now(),
                'admin',
                0, None, 1  # LIMITLOG, LIMITPTYPE, LIMITLEVEL
            ))
            ricode += 1
    
    cursor.executemany("""
        INSERT OR REPLACE INTO REPITEM 
        (RICODE, RICLASS, LOGCLASS, RITEXT, ALIASNAME, RIUNIT, RIDECIMALS, RISCALE, PLCRICODE,
         PLCRICODE2, PLCRICODE3, PLCRICODE4, PLCRICODE5, VSIGID1, VSIGID2, VSIGID3, VSIGID4, VSIGID5,
         VSIGID6, VSIGID7, VSIGID8, VSIGID9, VSIGID10, CSIGID1, CSIGID2, CSIGID3, SORTRICODE,
         AUXNUMVAL1, AUXNUMVAL2, AUXNUMVAL3, AUXNUMVAL4, AUXNUMVAL5, AUXNUMVAL6, AUXNUMVAL7,
         AUXDATEVAL1, AUXDATEVAL2, CUMPTYPE, CUMRICODE, OSIGID1, LIMITID, LOCID, MSIGID,
         DEADBAND, STOPLOG, DESCRIPTION, EXTGUID, DEFDATE, ATMODIFIER, ATCREDATE, ATCREATOR,
         LIMITLOG, LIMITPTYPE, LIMITLEVEL)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, report_items)
    
    print(f"Created {len(report_items)} report items")
    
    # 6. Generate Signal Values (current values)
    signal_values = []
    now = datetime.datetime.now()
    
    for sigid in range(1000, 1100):  # Sample of signals
        if random.random() > 0.1:  # 90% of signals have current values
            value = random.gauss(50, 15)  # Random value around 50
            status = 0 if random.random() > 0.05 else 1  # 95% good quality
            
            signal_values.append((
                sigid,
                now,
                value,
                f'Value_{value:.2f}',
                status
            ))
    
    cursor.executemany("""
        INSERT OR REPLACE INTO SIGNALVALUE 
        (SIGID, UPDATETIME, SIGNUMVALUE, SIGTEXTVALUE, SIGSTATUS)
        VALUES (?, ?, ?, ?, ?)
    """, signal_values)
    
    print(f"Created {len(signal_values)} current signal values")
    
    # 7. Generate Historical Data (REPDATA)
    print("Generating historical data...")
    rep_data = []
    
    # Generate data for each process instance and report item
    for pinstid in range(1, 101):  # First 100 process instances
        for ricode in range(10000, 10010):  # First 10 report items
            if random.random() > 0.1:  # 90% data availability
                value = random.gauss(25, 10)  # Random historical value
                quality = random.uniform(0.8, 1.0)  # Quality percentage
                
                rep_data.append((
                    pinstid,
                    ricode,
                    value,
                    f'Hist_{value:.1f}',
                    quality,
                    random.gauss(1.0, 0.1),  # AUXVAL1
                    random.gauss(0.5, 0.2),  # AUXVAL2
                    random.gauss(0.1, 0.05), # AUXVAL3
                    f'SORT_{ricode}',
                    f'LOC_{(ricode % 5) + 1}',
                    1.0,  # PLCFACTOR
                    f'ORDER_{pinstid}',
                    f'TASK_{ricode}',
                    'H'   # DUMMY
                ))
    
    cursor.executemany("""
        INSERT OR REPLACE INTO REPDATA 
        (PINSTID, RICODE, NUMVALUE, TEXTVALUE, PCTQUAL, AUXVAL1, AUXVAL2, AUXVAL3, 
         SORTNAME, SAMPLELOC, PLCFACTOR, ORDERID, TASKID, DUMMY)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rep_data)
    
    print(f"Created {len(rep_data)} historical data records")
    
    conn.commit()
    conn.close()
    
    print("Oracle IoT database populated successfully!")
    print(f"Database location: oracle_iot_db.db")

if __name__ == "__main__":
    populate_oracle_iot_db()