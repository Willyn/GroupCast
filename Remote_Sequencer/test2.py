from pyactor.context import set_context, create_host, sleep, shutdown, interval, serve_forever, later, Host
import time
import random
import signal
import sys
from pyactor.exceptions import TimeoutError
from member import Peer
import threading

set_context()
port = 1283
url_temp = "http://127.0.0.1:%d/" % (port)
print url_temp
h = create_host(url_temp)

atomicas = False
while not atomicas:
    try:
        group_proxy = h.lookup_url('http://127.0.0.1:1680/group', 'Group', 'group')
        printer_proxy = h.lookup_url('http://127.0.0.1:1680/printer', 'Printer', 'printer')
        atomicas = True
    except TimeoutError:
        sleep(1)
total=4
peers = {}
mutex = threading.Lock()
for i in range(1,total):
    peer = "Peer%d" % (i)
    peers[i] = h.spawn(peer, Peer)
    peers[i].setProxys(group_proxy,printer_proxy)
    peers[i].setLeader(None)
    peers[i].join()
    sleep(2)
sleep(3)

for i in range(1,total):
    msg = "Peer%d" % (i)
    peers[i].send_msg(msg)

sleep(15)

for i in range(1,total):
    msg = "Peer%d" % (i)
    peers[i].send_msg(msg)
    sleep(2)
    
serve_forever()