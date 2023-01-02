"""immerSUN UDP proxy server"""
#
#   Adapted from https://gist.github.com/vxgmichel/b2cf8536363275e735c231caef35a5df
#   (copied at 2022-12-04T17:00)
#
#   This is a UDP proxy server for an immerSUN solar generation diverter with optional
#   immerLINK bridge that is sending readings to myimmerSUN.com.
#
#   If UDP datagrams are artifically directed at this proxy server (rather than being
#   sent direct to the myimmerSUN.com server) they will have various data values extracted 
#   and then the original datagrams will be forwarded unmodified.
#   The response (inbound) datagrams will be forwarded back to the immerLINK bridge.
#   The extracted data values will be published as MQTT messages.
#
#
#   Configuration settings for the UDP Proxy Server:
#       The BIND_ADDRESS is the IPv4 address this proxy should listen on
#           0.0.0.0 will listen on all local addresses
#           If you have multiple local addresses and you want to use a particular one,
#           specify it here
BIND_ADDRESS = "0.0.0.0"
#       The REMOTE_HOST is the IPv4 address this proxy should forward datagrams to
#           136.243.233.46 is the 'Cloud IP' for the myimmerSUN.com server (as of 2022-12)
REMOTE_HOST = "136.243.233.46"
#       The UDP_PORT
UDP_PORT = 87
#
#   Configuration settings for MQTT:
#       The hostname or IPv4 address of the MQTT Broker
MQTT_BROKER = "mymqttbroker"
#       The MQTT port number 
MQTT_PORT = 1883
#       The MQTT username
MQTT_USER = "immersunuser"
#       The MQTT password
MQTT_PASSWORD = "mysecretpassword"
#       The MQTT client id
MQTT_CLIENT = "immersun2mqtt"
#       The MQTT Topic base path
MQTT_BASE_TOPIC = "raw/immersun/"
#
#
DEBUG = False


import asyncio
import paho.mqtt.client as mqtt


class ProxyDatagramProtocol(asyncio.DatagramProtocol):

    def __init__(self, remote_address, mqtt_client):
        self.remote_address = remote_address
        self.mqtt_client = mqtt_client
        self.remotes = {}
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # This is the outbound *request* to the immerSUN server
        # 'data' is a 'bytes object' - generally 56 bytes long
        numbytes = len(data)
        if (DEBUG):
            print("Outbound datagram... " + str(numbytes) + " bytes long")
            print(data.hex())
        # Extract the known data fields, stored in little-endian byte order
        exporting  = int.from_bytes(data[28:30], "little", signed=True)
        serial     = int.from_bytes(data[36:40], "little", signed=True)
        generating = int.from_bytes(data[52:54], "little", signed=True)
        diverting  = int.from_bytes(data[54:56], "little", signed=True)
        consuming = 0 - (exporting - generating + diverting)
        importing = 0 - exporting
        #
        #   Check for '3094' condition
        if exporting == 3094 and consuming == -3094:
            print("3094 condition detected - data values ignored")
        else:
            if (DEBUG):
                print("Exporting:  " + str(exporting))
                print("Generating: " + str(generating))
                print("Diverting:  " + str(diverting))
                print("Consuming:  " + str(consuming))
            #
            # Publish the gathered data to MQTT
            topic = MQTT_BASE_TOPIC + str(serial) + "/generating/power"
            result = self.mqtt_client.publish(topic, generating)
            if result.rc != 0:
                print("Error publishing to MQTT Broker: " + str(result.rc))
            topic = MQTT_BASE_TOPIC + str(serial) + "/consuming/power"
            result = self.mqtt_client.publish(topic, consuming)
            if result.rc != 0:
                print("Error publishing to MQTT Broker: " + str(result.rc))
            topic = MQTT_BASE_TOPIC + str(serial) + "/diverting/power"
            result = self.mqtt_client.publish(topic, diverting)
            if result.rc != 0:
                print("Error publishing to MQTT Broker: " + str(result.rc))
            topic = MQTT_BASE_TOPIC + str(serial) + "/importing/power"
            result = self.mqtt_client.publish(topic, importing)
            if result.rc != 0:
                print("Error publishing to MQTT Broker: " + str(result.rc))
        #
        if addr in self.remotes:
            self.remotes[addr].transport.sendto(data)
            return
        loop = asyncio.get_event_loop()
        self.remotes[addr] = RemoteDatagramProtocol(self, addr, data)
        coro = loop.create_datagram_endpoint(
            lambda: self.remotes[addr], remote_addr=self.remote_address)
        asyncio.ensure_future(coro)


class RemoteDatagramProtocol(asyncio.DatagramProtocol):

    def __init__(self, proxy, addr, data):
        self.proxy = proxy
        self.addr = addr
        self.data = data
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.data)

    def datagram_received(self, data, _):
        # This is the inbound *response* from the immerSUN server
        # 'data' is a 'bytes object' - expected to always be 56 bytes long
        numbytes = len(data)
        if (DEBUG):
            print("Inbound datagram...  " + str(numbytes) + " bytes long")
            print(data.hex())
        self.proxy.transport.sendto(data, self.addr)

    def connection_lost(self, exc):
        self.proxy.remotes.pop(self.attr)


async def start_datagram_proxy(bind, port, remote_host, remote_port, mqtt_client):
    loop = asyncio.get_event_loop()
    protocol = ProxyDatagramProtocol((remote_host, remote_port), mqtt_client)
    # create_datagram_endpoint() returns an instance of class 'DatagramTransport'
    return await loop.create_datagram_endpoint(
        lambda: protocol, local_addr=(bind, port))


#
# This is the 'main' function that starts everything up then loops forever
#
def main(bind=BIND_ADDRESS, port=UDP_PORT, remote_host=REMOTE_HOST, remote_port=UDP_PORT):
    loop = asyncio.get_event_loop()

    # Open the connection to the MQTT server
    if (DEBUG):
        print("Connecting to MQTT server...")
    mqtt_client = mqtt.Client(MQTT_CLIENT)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)

    # Start the UDP proxy
    if (DEBUG):
        print("Starting datagram proxy...")
    coro = start_datagram_proxy(bind, port, remote_host, remote_port, mqtt_client)
    transport, _ = loop.run_until_complete(coro)
    if (DEBUG):
        print("Datagram proxy is running...")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    if (DEBUG):
        print("Disconnecting from MQTT server...")
    mqtt_client.disconnect()
    if (DEBUG):
        print("Closing transport...")
    transport.close()
    loop.close()


if __name__ == '__main__':
    main()

