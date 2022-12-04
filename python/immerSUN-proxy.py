"""UDP proxy server."""

#
# Adapted from https://gist.github.com/vxgmichel/b2cf8536363275e735c231caef35a5df (copied at 2022-12-04T17:00)
#
# This is a UDP proxy server for an immerSUN solar generation diverter with an optional immerLINK bridge.
# If UDP datagrams are artifically directed at this proxy server (rather than being sent direct to the immerSUN server)
# they will have various data values extracted - and then the original datagrams will be forwarded unmodified.
# The response (inbound) datagrams will be forwarded back to the immerLINK bridge.
#

import asyncio


class ProxyDatagramProtocol(asyncio.DatagramProtocol):

    def __init__(self, remote_address):
        self.remote_address = remote_address
        self.remotes = {}
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # This is the outbound *request* to the immerSUN server
        # 'data' is a 'bytes object' - expected to always be 56 bytes long
        numbytes = len(data)
        print("Outbound datagram... " + str(numbytes) + " bytes long")
        print(data.hex())
        # Extract the known data fields
        exporting =  int.from_bytes(data[28:30], "little", signed=True)
        generating = int.from_bytes(data[52:54], "little", signed=True)
        diverting  = int.from_bytes(data[54:56], "little", signed=True)
        consuming = 0 - (exporting - generating + diverting)
        print("Exporting:  " + str(exporting))
        print("Generating: " + str(generating))
        print("Diverting:  " + str(diverting))
        print("Consuming:  " + str(consuming))
        # 
        # Report the unknown data fields
        ##print("INT16 at 25-26 is " + str(int.from_bytes(data[24:26], "little", signed=True)))
        ##print("INT16 at 27-28 is " + str(int.from_bytes(data[26:28], "little", signed=True)))
        ##print("INT16 at 35-36 is " + str(int.from_bytes(data[34:36], "little", signed=True)))
        ##print("INT16 at 41-42 is " + str(int.from_bytes(data[40:42], "little", signed=True)))
        ##print("INT16 at 45-46 is " + str(int.from_bytes(data[44:46], "little", signed=True)))
        ##print("INT16 at 49-50 is " + str(int.from_bytes(data[48:50], "little", signed=True)))
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
        print("Inbound datagram...  " + str(numbytes) + " bytes long")
        print(data.hex())
        self.proxy.transport.sendto(data, self.addr)

    def connection_lost(self, exc):
        self.proxy.remotes.pop(self.attr)


async def start_datagram_proxy(bind, port, remote_host, remote_port):
    loop = asyncio.get_event_loop()
    protocol = ProxyDatagramProtocol((remote_host, remote_port))
    return await loop.create_datagram_endpoint(
        lambda: protocol, local_addr=(bind, port))


#
# The 'bind' value is the IPv4 address this proxy should listen on
#     0.0.0.0 will listen on all local addresses
# The 'remote_host_ value is the IPv4 address this proxy should forward datagrams to
#     136.243.233.46 is the 'Cloud IP' for the immerSUN server (as of 2022-12)
#
def main(bind='172.16.40.81', port=87,
        remote_host='136.243.233.46', remote_port=87):
    loop = asyncio.get_event_loop()
    print("Starting datagram proxy...")
    coro = start_datagram_proxy(bind, port, remote_host, remote_port)
    transport, _ = loop.run_until_complete(coro)
    print("Datagram proxy is running...")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    print("Closing transport...")
    transport.close()
    loop.close()


if __name__ == '__main__':
    main()

