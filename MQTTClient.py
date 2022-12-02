import sys
import paho.mqtt.client as mqtt
import sparkplug_b as sparkplug
import time
import random
import string

from sparkplug_b import *

# Application Variables
serverUrl = "YourDomainStuffHere"
myGroupId = "YourGroup"
myNodeName = "YourNode"
publishPeriod = 5000
myUsername = "test"
myPassword = ""

class AliasMap:
    Next_Server = 0
    Rebirth = 1
    Reboot = 2
    Dataset = 3
    VV_Loading = 4

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected with result code "+str(rc))
    else:
        print("Failed to connect with result code "+str(rc))
        sys.exit()

    global myGroupId
    global myNodeName

    client.subscribe("spBv1.0/" + myGroupId + "/NDATA/" + myNodeName)
    client.subscribe("spBv1.0/" + myGroupId + "/DCMD/" + myNodeName)

def on_message(client, userdata, msg):
    #print("Message arrived: " + msg.topic)
    tokens = msg.topic.split("/")

    if tokens[0] == "spBv1.0" and tokens[1] == myGroupId and (tokens[2] == "NDATA" or tokens[2] == "DCMD") and tokens[3] == myNodeName:
        inboundPayload = sparkplug_b_pb2.Payload()
        inboundPayload.ParseFromString(msg.payload)
        for metric in inboundPayload.metrics:
            if metric.name == "WhatYou'reLookingFor":
                print(metric.float_value)
                #Or think of something way cooler than just printing it. That code goes here.
                break

    else:
        print ("Unknown command...")


def publishBirth():
    publishNodeBirth()

def publishNodeBirth():
    print ("Publishing Node Birth")

    payload = sparkplug.getNodeBirthPayload()

    # Publish the node birth certificate
    byteArray = bytearray(payload.SerializeToString())
    client.publish("spBv1.0/" + myGroupId + "/NBIRTH/" + myNodeName, byteArray, 0, False)

##########################################################################################################################################


##########################################################################################################################################
print ("Starting main application....")
print("")
print('Listening to Some Metrics at some place')
print("#################################################################################################################################################")
print("")
print("#################################################################################################################################################")



# Create the node death payload
deathPayload = sparkplug.getNodeDeathPayload()

# Start of main program - Set up the MQTT client connection
client = mqtt.Client(serverUrl, 1883, 60)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(myUsername, myPassword)
deathByteArray = bytearray(deathPayload.SerializeToString())
client.will_set("spBv1.0/" + myGroupId + "/NDEATH/" + myNodeName, deathByteArray, 0, False)
client.connect(serverUrl, 1883, 60)

# Short delay to allow connect callback to occur
time.sleep(.1)
client.loop()

# Publish the birth certificates
publishBirth()

while True:
    # Periodically publish some new data
    payload = sparkplug.getDdataPayload()

    # Publish a message data
    byteArray = bytearray(payload.SerializeToString())
    #client.publish("spBv1.0/" + myGroupId + "/DDATA/" + myNodeName + "/" + myDeviceName, byteArray, 0, False)

    # Sit and wait for inbound or outbound events
    for _ in range(1):
        time.sleep(.1)
        client.loop()