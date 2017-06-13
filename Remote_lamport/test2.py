from pyactor.context import set_context, create_host, sleep, shutdown, interval, serve_forever, later, Host
import time
import random
import signal
import sys
from pyactor.exceptions import TimeoutError
from member import Peer


total=4
peers={}

def inicializa_peers():
    global peers
    port = 1281
    url_temp = "http://127.0.0.1:%d/" % (port)
    print url_temp
    h = create_host(url_temp)
    
    atomicas = False
    while not atomicas:
        try:
            group_proxy = h.lookup_url('http://127.0.0.1:1677/group', 'Group', 'group')
            printer_proxy = h.lookup_url('http://127.0.0.1:1677/printer', 'Printer', 'printer')
            atomicas = True
        except TimeoutError:
            sleep(1)
    for i in range(1,total+1):
        peer = "Peer%d" % (i)
        peers[i] = h.spawn(peer, Peer)
        peers[i].setProxys(group_proxy,printer_proxy)
        peers[i].join()
        sleep(1)
    sleep(3)
        
def prueba1():
    print "En esta prueba enviaremos 4 mensajes:"
    print "- peer3 envia el mensaje 1"
    print "- peer1 envia el mensaje 2"
    print "- peer3 envia el mensaje 3"
    print "- peer2 envia el mensaje 4"
    
    peers[3].send_msg("Mensaje 1")
    sleep(2)
    peers[1].send_msg("Mensaje 2")
    sleep(2)
    peers[3].send_msg("Mensaje 3")
    sleep(2)
    peers[2].send_msg("Mensaje 4")
    sleep(2)
    
    sleep(60)
    for i in range(1,total+1):
        print(peers[i])
        peers[i].process_all()


def prueba2():
    print "En esta prueba primero enviaremos 2 mensajes:"
    print "- peer3 envia el mensaje 1"
    print "- peer1 envia el mensaje 2"
    peers[3].send_msg("Mensaje 1")
    sleep(2)
    peers[1].send_msg("Mensaje 2")
    sleep(20)
    print "Despues, el peer 1 se cae..."
    peers[1].keep_alive_stop()
    sleep(60)
    print "Ahora Peer 2 envia mensaje 3..."
    peers[2].send_msg("Mensaje 3")
    sleep(15)
    print "Ahora el peer 1 reconnecta..."
    peers[1].join()
    sleep(10)
    print "Y peer 1 envia el mensaje 4"
    peers[1].send_msg("Mensaje 4")
    sleep(25)
    for i in range(1,total+1):
        print(peers[i])
        peers[i].process_all()

if __name__ == "__main__":
    if (len(sys.argv)!=2):
        print "\nSintaxis: python %s [opcion]\n" % (sys.argv[0])
        print "-----------------------------------------------------------------"
        print "| OPCIONES                                                      |"
        print "-----------------------------------------------------------------"
        print "<  1- Prueba 1: Se crean 4 peers y se envian 3 mensajes.        >"
        print "<  2- Prueba 2: Sale un peer y reconecta.                       >"
        print "-----------------------------------------------------------------"
        exit()
        
    set_context()
    inicializa_peers()
    
    opciones = {
        '1': prueba1, 
        '2': prueba2
    }
    
    op=sys.argv[1]
    print "OPCION SELECCIONADA:",op
    
    try:
        opciones[op]()
    except:
        print("Opcion invalida")
        exit()
        

    serve_forever()