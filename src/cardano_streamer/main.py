import socket
def openSocket() -> socket:
    print('Opening socket and waiting for client...')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = "0.0.0.0"
    port = 12345
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((host, port))
    print("Listening on port: %s" % str(port))
    s.listen(5)
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    return c_socket

import ogmios
import socket
import jsonpickle
import configparser

config = configparser.RawConfigParser()
config.read('app.properties')
config_dict = dict(config.items('APP'))
print(f'Using configs: {config_dict}')

headers = {}
if 'cardano.ogmios.demeter.apikey' in config_dict and config_dict['cardano.ogmios.demeter.apikey'] is not None:
    headers = {'dmtr-api-key': config_dict['cardano.ogmios.demeter.apikey']}

client = ogmios.Client(host=config_dict['cardano.ogmios.address'],
                       port=int(config_dict['cardano.ogmios.port']),
                       secure=config_dict['cardano.ogmios.ssl.enabled'] == "True",
                       additional_headers=headers)

start_block = ogmios.Point(id=config_dict['cardano.start.point.block'],
                           slot=int(config_dict['cardano.start.point.slot']))


def streamTheData(c_socket):
    blocks_printed = 0
    try:
        point, tip, id = client.find_intersection.execute([start_block])
        print(f"Found intersection: {point}")
    except ogmios.ResponseError:
        print("Intersection not found. Make sure you're connected to the proper network.")

    while True:
        direction, tip, block, id = client.next_block.execute()
        if direction.value == "forward":
            blocks_printed += 1
            print(f"Block #{blocks_printed}, {block.height}")
            block_json = jsonpickle.encode(block)
            c_socket.send(bytes("{}\n".format(block_json), "utf-8"))

def waitForConnectionThenStartStreaming():
    while True:
        c_socket = openSocket()
        try:
            print("Restarting..")
            streamTheData(c_socket)
        except socket.error as msg:
            print(f"{msg}")
            c_socket.close()
            continue

if __name__ == "__main__":
    waitForConnectionThenStartStreaming()


