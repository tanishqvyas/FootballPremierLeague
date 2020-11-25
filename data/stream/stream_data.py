#!/usr/bin/python3

import socket, sys, requests, json, time, csv, cryptography
from cryptography.fernet import Fernet
key = b'TXG4f9eYOgV2F_Wkt4-AAg1xgmXafvVnCF9XNTCvKbg='
fer = Fernet(key)

def send_data_to_spark(tcp_connection, eve, mat):
    m = eve[0]['matchId']
    fl = 0
    msg = ''
    for i in eve:
        if i['matchId'] == m:
            msg = json.dumps(i)
        else:
            fl = 0
            time.sleep(5)
            m = i['matchId']
            msg = json.dumps(i)
        if fl == 0:
            fl = 1
            for j in mat:
                if j['wyId'] == m:
                    tcp_connection.send((json.dumps(j)+'\n').encode())
                    break

        tcp_connection.send((msg+'\n').encode())


TCP_IP = 'localhost'
TCP_PORT = 6100
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
with open('eve.txt', 'rb') as (f):
    t = f.read()
    da = fer.decrypt(t)
    js1 = json.loads(da)
with open('mat.txt', 'rb') as (f):
    t = f.read()
    da = fer.decrypt(t)
    js2 = json.loads(da)
time.sleep(2)
s.listen(1)
print('Waiting for connection...')
conn, addr = s.accept()
print('Connected... Starting to push EPL data')
send_data_to_spark(conn, js1, js2)