from socket import *
import sys
import json


serverName = ""
serverPort = 6100


clientSocket = socket(AF_INET, SOCK_STREAM)
clientSocket.connect((serverName, serverPort))

sample_data = {
	"Aparna" : 1,
	"Pooja" : 2,
	"Shreya" : 3,
	"Tanishq" : 4
}

serialized_data = json.dumps(sample_data) #data serialized

# clientSocket.send(str.encode(sample_data))
clientSocket.send(str.encode(serialized_data))

response_data = clientSocket.recv(1024)
print("Response data from server : ", response_data.decode())

clientSocket.close()