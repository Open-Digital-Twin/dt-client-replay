import time
import json
from paho.mqtt import client as mqtt_client

broker = 'localhost'
port = 1883
topic = "/sensor/python"
client_id = f'python-mqtt-pub'
#username = 'emqx'
#password = 'public'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    #client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client):
     msg_count = 1
     while msg_count <= 10000:
         time.sleep(0.0001)
         msg = {"count": msg_count, "time": time.time()}
         result = client.publish(topic, json.dumps(msg))
         # result: [0, 1]
         status = result[0]
         if status == 0:
             if msg_count % 100 == 0:
                print(f"Sent `{msg}` to topic `{topic}`")
         else:
             print(f"Failed to send message to topic {topic}")
         msg_count += 1

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

    client.subscribe(topic)
    client.on_message = on_message

def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)

if __name__ == '__main__':
    run()