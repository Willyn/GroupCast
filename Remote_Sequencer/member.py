from pyactor.context import set_context, create_host, sleep, shutdown, interval, serve_forever, later, Host
import time
import random
import signal
import sys
import socket
from pyactor.exceptions import *


class Peer(object):
    _tell = ['join','setProxys','group_join','recieve_message','keep_alive_stop','leave','stop_interval','send_msg','process','procesarMensajes','setLeader','activeLeadership','zombie','fixLeader']
    _ask = ['getId', 'getCont', 'isLeader','searchNewLeader']
    
    def __init__(self):
        self.siguiente = 1
        self.messages = {}
        self.cont = 0
        self.is_leader = False
        self.leader = None
        self.leader_id = None
        self.cacheURL = {}
        self.procesados = []

    def getCont(self):
        self.cont += 1
        self.printer.msg(self.id,"Soy secuencer estoy repartiendo turno")
        return (self.cont - 1)
        
    def isLeader(self):
        return self.is_leader
        
    def activeLeadership(self,peers,leave):
        if not self.is_leader:
            self.printer.msg(self.id,"Hola! Soy el nuevo lider!!")
            self.is_leader = True
            self.leader = self
            self.leader_id = self.id
            cont = 0
            #Localizamos el ultimo numero dado por el leader, sera el numero desde el que seguir repartiendo turno ahora que este sera el nuevo leader
            for turn in self.messages.keys():
                if turn > cont:
                    cont = turn
            self.cont = cont + 1
            atomica = False
            if leave:
                #Ahora es turno de decirle a todos que somos el nuevo leader
                if not peers == None:
                    for peer in peers.keys():
                        if not peer == self.id:
                            atomicas=False
                            while not atomicas:
                                try:
                                    if self.cacheURL.has_key(peer):
                                        temp = self.cacheURL[peer]
                                    else:
                                        temp = self.host.lookup_url(peers[peer]['url'],Peer)
                                        self.cacheURL[peer] = temp
                                    self.printer.msg(peer,"Me van a hacer fixLeader")
                                    temp.fixLeader(self.id,self.url)
                                    self.printer.msg(peer,"<- se le ha asignado nuevo leader")
                                    atomicas=True
                                except TimeoutError:
                                    sleep(0.2)

        
    def fixLeader(self, peer, url):
        atomica=False
        self.printer.msg(self.id,"Acabo de entrar en fixLeader")
        while not atomica:
            try:
                if self.cacheURL.has_key(peer):
                    temp = self.cacheURL[peer]
                else:
                    temp = self.host.lookup_url(url,Peer)
                    self.cacheURL[peer] = temp
                self.leader=temp
                self.leader_id = peer
                msg_temp = "Tengo nuevo lider %s" % (peer)
                self.printer.msg(self.id,msg_temp)
                atomica=True
            except TimeoutError:
                sleep(0.2)       
        
    def setLeader(self,peers):
        #Esta funcion es con la que los Members al entrar saben quien es el Leader, pero es poco optima para la asignacion de uno nuevo cuando cae, para ello se usara fixLeader
        atomica = False
        self.printer.msg(self.id,"Entrando a setLeader")
        while not atomica:
            try:
                if peers == None:
                    peers = self.group.get_members()
                if peers == None:
                    self.leader = self
                    self.leader_id = self.id
                else:
                    for peer in peers.keys():
                        if not peer == self.id:
                            if self.cacheURL.has_key(peer):
                                temp = self.cacheURL[peer]
                            else:
                                temp = self.host.lookup_url(peers[peer]['url'],Peer)
                                self.cacheURL[peer] = temp
                            if temp.isLeader():
                                self.leader = temp
                                self.leader_id = peer
                                msg_temp = "Mi nuevo lider es %s" % (peer)
                                self.printer.msg(self.id,msg_temp)
                                break
                atomica=True
            except TimeoutError:
                None
        
    def join(self):
        self.group.join(self.url,self.id)
        self.interval=interval(self.host, 3, self.proxy, "group_join")
        
    def setProxys(self,group,printer):
        self.group = group
        self.printer = printer

    def group_join(self):
        self.process()
        self.group.keep_alive(self.url,self.id)
       
     
    def zombie(self):
        sleep(10000)
     
    def getId(self):
        return self.id
        
    def keep_alive_stop(self):    
        self.interval.set()
        
    def recieve_message(self, turn, message):
        self.messages[turn] = message
    
    def stop_interval(self):
        self.interval.set()
    
    
    def leave(self):
        self.stop_interval()
        self.group.leave(self.id)
        self.printer.msg(self.id,"Me voyy")
        if self.is_leader:
            self.printer.msg(self.id,"Soy el lider y activo busqueda de nuevo")
            peers = self.group.get_members()
            self.searchNewLeader(peers,True)
        
    
    def process(self):
        while self.messages.has_key(self.siguiente):
            self.printer.msg(self.id,"Procesando mensajes nuevo")
            msg = "Posicion:%d  Mensaje:%s" % (self.siguiente,self.messages[self.siguiente])
            self.procesados.append(self.messages[self.siguiente])
            self.printer.msg(self.id,msg)
            self.siguiente +=1
        self.printer.msg(self.id,"Diccionario")
        self.printer.msg(self.id,self.messages)
        self.printer.msg(self.id,"Procesados")
        self.printer.msg(self.id,self.procesados)
        
        
    def procesarMensajes(self):
        print(self.id,self.messages)

    def searchNewLeader(self,peers,leave):
        #Esta funcion localiza al peer con ID mas alto para activarle liderazgo, no puede fallar.
        if peers == None:
            self.activeLeadership(peers,leave)
            return True
        else:
            cont_max=0
            id_max=0
            #Localizamos el ID mas alto
            for peer in peers.keys():
                if int(peer.replace("Peer","")) >= cont_max:
                    cont_max = int(peer.replace("Peer",""))
                    id_max=peer
            atomicas=False
            while not atomicas:
                try:
                    #Una vez localizado, si somos nosotros asignamos
                    if id_max == self.id:
                        self.activeLeadership(peers,leave)
                        return True
                    else:
                        if leave:
                            if self.cacheURL.has_key(id_max):
                                temp = self.cacheURL[id_max]
                            else:
                                temp = self.host.lookup_url(peers[id_max]['url'],Peer)
                                self.cacheURL[id_max] = temp
                            temp.activeLeadership(peers,leave)
                        return False
                    atomicas=True
                except TimeoutError:
                    sleep(0.2)
        
    def send_msg(self,msg):
        #Aseguramos que las operaciones son atomicas, no podemos obtener un Turno y no enviar ese mensaje, puesto que sin el los siguientes jamas se procesarian
        turnRec = False
        membersRec = False
        cont_timeout = 0
        while not membersRec:
            try:
                peers = self.group.get_members()
                membersRec = True
            except TimeoutError:
                self.printer.msg(self.id,"Timeout get_members")
                membersRec = False
                sleep(1)
        while not turnRec:
            try:
                turn = self.leader.getCont()
                turnRec = True
            except TimeoutError:
                #Si obtenemos 3 timeouts seguidos, damos por muerto a leader y vamos a localizar a uno nuevo
                turnRec = False
                cont_timeout += 1
                self.group_join()
                if cont_timeout >= 1:
                    self.printer.msg(self.id,"Tenemos un Timeout de turno excedido de rango, Sequencer caido.")
                    self.printer.msg(self.id,"Vamos a buscar nuevo leader!")
                    if not self.searchNewLeader(peers,False):
                        msg_temp = "Yo no soy el nuveo leader, sigamos buscando, le doy tiempo para que se setee"
                        self.printer.msg(self.id,msg_temp)
                        #Nos cargamos al sequencer caido de la lista de members, para no buscar sobre el
                        peers.pop(self.leader_id)
                        sleep(1)
                        self.setLeader(peers)
                    cont_timeout = 0
                sleep(0.3)
            except Exception:
                #Si caemos en esta excepcion es que Leader a caido de forma brusca y hay que localizar a uno nuevo
                self.printer.msg(self.id,"Se ha caido el sequencer")
        for peer in peers.keys():
            atomicas = False
            while not atomicas:
                try:
                    if self.cacheURL.has_key(peer):
                        temp = self.cacheURL[peer]
                    else:
                        temp = self.host.lookup_url(peers[peer]['url'],Peer)
                        self.cacheURL[peer] = temp
                    temp.recieve_message(turn,msg)
                    atomicas = True
                except TimeoutError:
                    sleep(1)
                msg_temp = "Escribiendo mensaje en %s , tenemos turno %d" % (peer,turn)
                self.printer.msg(self.id,msg_temp)
        
