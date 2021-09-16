import json
import datetime
import paho.mqtt.client as mqtt

broker = 'localhost'
port = 1883
topic = "/sensor/python"
client_id = f'python-mqtt-sub'
#username = 'emqx'
#password = 'public'

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(topic)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    payload = json.loads(msg.payload)
    pubtime = datetime.datetime.fromisoformat(payload["time"])
    delta = datetime.datetime.today() - pubtime
    if payload["count"] % 100 == 0:
        print(f"{msg.topic} {payload} {delta}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, port, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()