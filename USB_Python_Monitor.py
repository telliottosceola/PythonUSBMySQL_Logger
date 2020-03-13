# import paho.mqtt.client as mqtt
import pymysql.cursors
import sys
import json
from ncd_enterprise import NCDEnterprise

SERIAL_PORT = "/dev/tty.usbserial-AC3CQ7J9"
BAUD_RATE = 115200

#User variable for database name
dbName = "sensors"
mysqlHost = "localhost"
mysqlUser = "python_logger"
mysqlPassword = "XXXXXXXX"

# it is expected that this Database will already contain one table called sensors.  Create that table inside the Database with this command:
# CREATE TABLE sensors(device_id char(23) NOT NULL, transmission_count INT NOT NULL, battery_level FLOAT NOT NULL, type INT NOT NULL, node_id INT NOT NULL, rssi INT NOT NULL, last_heard TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);

# This function updates the sensor's information in the sensor index table
def sensor_update(db, payload):
    cursor = db.cursor()
    # See if sensor already exists in sensors table, if not insert it, if so update it with latest information.
    deviceQuery = "EXISTS(SELECT * FROM sensors WHERE device_id = '%s')"%(payload['source_address'])
    cursor.execute("SELECT "+deviceQuery)
    data = cursor.fetchone()
    if(data[deviceQuery] >= 1):
        updateRequest = "UPDATE sensors SET transmission_count = %i, battery_level = %s, last_heard = CURRENT_TIMESTAMP WHERE device_id = '%s'" % (int(payload['counter']), payload['battery_percent'].replace('%',''), payload['source_address'])
        cursor.execute(updateRequest)
        db.commit()
    else:
        insertRequest = "INSERT INTO sensors(device_id, transmission_count, battery_level, type, node_id, last_heard) VALUES('%s',%i,%s,%i,%i,CURRENT_TIMESTAMP)" % (payload['source_address'], int(payload['counter']), payload['battery_percent'].replace('%',''), int(payload['sensor_type_id']), int(payload['nodeId']))
        print(insertRequest)
        cursor.execute(insertRequest)
        db.commit()

# This function determines if there is a table for the sensor telemetry, if not it creates one, then it logs sensor telemetry to the appropriate table
def log_telemetry(db, payload):
    cursor = db.cursor()
    # See if a table already exists for this sensor type, if not then create one
    tableExistsQuery = "SHOW TABLES LIKE 'type_%i'" % payload['sensor_type_id']
    cursor.execute(tableExistsQuery)
    data = cursor.fetchone()

    if(data == None):
        # No table for that sensor type yet so create one
        createTableRequest = "CREATE TABLE type_%i(device_id CHAR(23) NOT NULL,timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," % payload['sensor_type_id']

        for key in payload['sensor_data']:
            if type(payload['sensor_data'][key]) == int:
                newColumn = key+" INT,"
                createTableRequest += newColumn
            if type(payload['sensor_data'][key]) == float:
                newColumn = key+" FLOAT,"
                createTableRequest += newColumn
        # remove last comma from string
        createTableRequest = createTableRequest[:-1]
        # close the command with parenthases
        createTableRequest += ')'
        cursor.execute(createTableRequest)
        db.commit()
    # Log sensor data
    logInsertRequest = "INSERT INTO type_%i(device_id,timestamp," % payload['sensor_type_id']
    for key in payload['sensor_data']:
        columnKey = key+","
        logInsertRequest+=columnKey
    # remove last comma from string
    logInsertRequest = logInsertRequest[:-1]

    logInsertRequest += ") VALUES('" + payload['source_address'] +"',CURRENT_TIMESTAMP,"

    for key in payload['sensor_data']:
        columnData = str(payload['sensor_data'][key])+","
        logInsertRequest += columnData
    # remove last comma from string
    logInsertRequest = logInsertRequest[:-1]
    logInsertRequest += ')'
    cursor.execute(logInsertRequest)
    db.commit()

# The callback for when a PUBLISH message is received from the MQTT Broker.
def on_message(msg):
    print("Transmission received")
    # payload = json.loads((msg))
    payload = msg
    if 'source_address' in payload and 'sensor_data' in payload:
        print('source_address and sensor_data in payload')
        if 'counter' in payload and 'battery_percent' in payload and 'sensor_type_id' in payload and 'nodeId' in payload:
            if payload['sensor_type_id'] == 40:
                if payload['counter'] == 'NA':
                    return
            db = pymysql.connect(host="localhost", user=mysqlUser, password=mysqlPassword, db=dbName,charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
            sensor_update(db,payload)
            log_telemetry(db,payload)
            print('data logged')
            db.close()

def error_callback(error_data):
    print('Error detected:')
    print(error_data)

db = pymysql.connect(host=mysqlHost, user=mysqlUser, password=mysqlPassword, db=dbName, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
db.close()
print("MySQL Client Connected")
ncdModem = NCDEnterprise(SERIAL_PORT, BAUD_RATE, on_message, {'error_handler': error_callback})
print("connected to xbee modem")
