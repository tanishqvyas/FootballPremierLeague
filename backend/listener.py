import socket
from socket import *
import json

serverPort = 6100
serverSocket = socket(AF_INET, SOCK_STREAM)
# Avoiding : Python [Errno 98] Address already in use


serverSocket.bind(("", serverPort))
serverSocket.listen(1)

# TCP Server
print("The server is listening on PORT 6100")

while 1:

	connectionSocket, addr = serverSocket.accept()
	data = connectionSocket.recv(1024)

	# De-serializing data
	data_loaded = json.loads(data)
	
	print("client Sent :\n ", data_loaded)
	
	# Sending response
	connectionSocket.send(str.encode("Mil gya data"))


connectionSocket.close()