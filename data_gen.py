from azure.iot.device import IoTHubDeviceClient, Message
import time
import json
import random
from datetime import datetime
import mysql.connector

connection = "HostName=elecH.azure-devices.net;DeviceId=iotdevice;SharedAccessKey=TQ5shcqIv/LfrFaZeiyhLQK3dyzjasN46Hi1rPnB76w="
host='127.0.0.1'
user='root'
password='admin.'
port = '3306'
db = 'iotdevice1'

def main():
    try:
        client = IoTHubDeviceClient.create_from_connection_string(connection)
        print('connected to iot hub')
        # conn = mysql.connector.connect(
        #             host= host,
        #             user= user,
        #             password= password,
        #             database=db
        #                 )
        # curr = conn.cursor() 
        # print("database connected")
        # database_conn()
        while True:
            consumer_id = random.randint(1001, 1010)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            energy_consumption = round(random.uniform(0.1, 5.0), 3)
            peak_demand = round(random.uniform(0.5, 10.0), 2)
            threshold_limit = random.choice([3.5, 4.0, 4.5, 5.0])
            device_id = f"D{random.randint(1, 5):03}"
            voltage_level = random.randint(220, 240)

            data = {
                "Consumer_ID": consumer_id,
                "Device_ID": device_id,
                "Timestamp": timestamp,
                "Energy_Consumption_kWh": energy_consumption,
                "Peak_Demand_kW": peak_demand,
                "Threshold_Limit_kWh": threshold_limit,
                "Voltage_Level": voltage_level
            }
            # insert_data(conn,curr,data)
            message = Message(json.dumps(data))
            client.send_message(message)
            print(f"Message of device {data['Device_ID']} was sent successfully")
            time.sleep(1)
    except KeyboardInterrupt:
        print("program was interruped")


    

def insert_data(conn, curr, data):
    curr.execute("""
        INSERT INTO iotdata (
            Consumer_ID,
            Device_ID,
            Timestamp,
            Energy_Consumption_kWh,
            Peak_Demand_kW,
            Threshold_Limit_kWh,
            Voltage_Level
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        data["Consumer_ID"],
        data["Device_ID"],
        data["Timestamp"],
        data["Energy_Consumption_kWh"],
        data["Peak_Demand_kW"],
        data["Threshold_Limit_kWh"],
        data["Voltage_Level"]
    ))
    conn.commit()
    print("Data inserted successfully.")

if __name__ == "__main__":
    main()





