from pyactor.context import set_context, create_host, sleep, shutdown, interval, serve_forever, later, Host
import time
import random
import signal
import sys
import socket
import operator
from pyactor.exceptions import TimeoutError


class Peer(object):
    _tell = ['join','setProxys','group_join','keep_alive_stop','leave','stop_interval','send_msg','process','process_all','check_ts','send_ack','recive_ack','recieve_message']
    _ask = ['getId','get_ts']
    _ref = ['setProxys','send_msg','setProxys','send_ack','recieve_message']

    def __init__(self):
        self.messages = {}
        self.ts = 0
        self.cacheURL = {}

    def join(self):
        self.check_ts()
        self.group.join(self.url,self.id)
        self.interval=interval(self.host, 3, self.proxy, "group_join")
        
    def setProxys(self,group,printer):
        self.group = group
        self.printer = printer

    def group_join(self):
        self.group.keep_alive(self.url,self.id)
     
    def getId(self):
        return self.id
        
    def keep_alive_stop(self):    
        self.interval.set()
        
    def send_ack(self,id_mensaje,ack,members):
        for peer in members.keys():
            if peer!=self.id: # Envio ACK a todos menos a mi mismo
                atomicas = False
                while not atomicas:
                    try:
                        if self.cacheURL.has_key(peer):
                            temp = self.cacheURL[peer]
                        else:
                            temp = self.host.lookup_url(members[peer]['url'],Peer)
                            self.cacheURL[peer] = temp
                        temp.recive_ack(id_mensaje,self.id,ack,len(members))
                        atomicas = True
                    except TimeoutError:
                        sleep(1)
                msg_temp = "He enviado a %s ACK=%d" % (peer,ack)
                self.printer.msg(self.id,msg_temp)
    
    def recive_ack(self,id_mensaje,id_peer,ack,num_members):
        if not self.messages.has_key(id_mensaje):
            self.messages[id_mensaje]={}
        if not self.messages[id_mensaje].has_key('acks'):
            self.messages[id_mensaje]['acks']={}
        if not self.messages[id_mensaje].has_key('mensaje'):
            self.messages[id_mensaje]['mensaje']="MENSAJE NO RECIBIDO :("
        self.messages[id_mensaje]['acks'][id_peer]=ack
        if ack>=self.ts:
            self.ts=ack+1
        #SOLO PROCESAR EL MENSAJE CUANDO SE HAN RECIBIDO TODOS LOS ACK
        if len(self.messages[id_mensaje]['acks']) == num_members:
            self.process(num_members)
    
    def recieve_message(self, id_mensaje, id_peer, ts, message, members):
        if not self.messages.has_key(id_mensaje):
            self.messages[id_mensaje]={}
        if not self.messages[id_mensaje].has_key('acks'):
            self.messages[id_mensaje]['acks']={}
        self.messages[id_mensaje]['acks'][self.id]=-1 # No recibo mi propio ACK asi que le pongo -1
        self.messages[id_mensaje]['acks'][id_peer]=-1 # No recibo el ACK del que envia el msg, asi que le pongo -1
        self.messages[id_mensaje]['mensaje']=message
        
        self.ts=self.ts+1
        if self.ts<=ts:
            self.ts=ts+1
        self.send_ack(id_mensaje,self.ts,members)
        
    def stop_interval(self):
        self.interval.set()
    
    def leave(self):
        self.group.leave(self.proxy)
        self.stop_interval()
    
    def process(self,num_members):
        msg="\n-MENSAJES PROCESADOS HASTA EL MOMENTO..."
        for clave in self.messages.keys():
            if num_members==len(self.messages[clave]['acks']):
                msg = "%s\nID Mensaje: %s\nACKS: %s\nMensaje:%s\n\n" % (msg,clave,self.messages[clave]['acks'],self.messages[clave]['mensaje'])
        self.printer.msg(self.id,msg)
    
    def process_all(self):
        msg="\n-TODOS LOS MENSAJES PROCESADOS HASTA EL MOMENTO-"
        for clave in self.messages.keys():
            msg = "%s\nID Mensaje: %s\nACKS: %s\nMensaje:%s\n\n" % (msg,clave,self.messages[clave]['acks'],self.messages[clave]['mensaje'])
        self.printer.msg(self.id,msg)
            
        #Comprovamos si estan todos los ack de ese mensaje
        #Si estan procesamos el mensaje y actualizamos 
        #for clave in self.messages.keys():
        #    temp=clave.split("-")
            
        #mensajes_ordenados = sorted(self.messages.items(), key=operator.itemgetter(0))
        #self.printer.msg(self.id,mensajes_ordenados)
        
    def send_msg(self,message):
        #Aseguramos que las operaciones son atomicas, no podemos obtener un Turno y no enviar ese mensaje, puesto que sin el los siguientes jamas se procesarian
        membersRec = False
        while not membersRec:
            try:
                members = self.group.get_members()
                membersRec = True
                self.printer.msg(self.id,"get_members concedido desde send_msg()")
            except TimeoutError:
                self.printer.msg(self.id,"Timeout get_members")
                membersRec = False
                sleep(1)
        
        self.ts=self.ts+1
        id_mensaje="%d-%s" % (self.ts,self.id)
        self.messages[id_mensaje]={}
        self.messages[id_mensaje]['acks']={}
        self.messages[id_mensaje]['acks'][self.id]=-1 # No recibo mi propio ACK asi que le pongo -1
        self.messages[id_mensaje]['mensaje']=message
        
        for peer in members.keys():
            if peer!=self.id:
                atomicas = False
                while not atomicas:
                    try:
                        if self.cacheURL.has_key(peer):
                            temp = self.cacheURL[peer]
                        else:
                            temp = self.host.lookup_url(members[peer]['url'],Peer)
                            self.cacheURL[peer] = temp
                        temp.recieve_message(id_mensaje,self.id,self.ts,message,members) #future = temp.recieve_message(self.id,self.ts,msg,future=True)
                        atomicas = True
                    except TimeoutError:
                        sleep(1)
                msg_temp = "He enviado \"%s\" a %s con TS=%d" % (message,peer,self.ts)
                self.printer.msg(self.id,msg_temp)

        
    def get_ts(self):
        return self.ts
        
    def check_ts(self):
        membersRec = False
        while not membersRec:
            try:
                members=self.group.get_members()
                membersRec = True
                self.printer.msg(self.id,"get_members concedido desde check_ts()")
            except TimeoutError:
                self.printer.msg(self.id,"Timeout get_members")
                membersRec = False
                sleep(1)
        
        if members==None:
            self.ts=0
        else:
            for peer in members.keys():
                atomicas = False
                while not atomicas:
                    try:
                        if self.cacheURL.has_key(peer):
                            temp = self.cacheURL[peer]
                        else:
                            temp = self.host.lookup_url(members[peer]['url'],Peer)
                            self.cacheURL[peer] = temp
                        ts_temp=temp.get_ts()
                        atomicas = True
                        if self.ts<ts_temp:
                            self.ts=ts_temp
                    except TimeoutError:
                        sleep(1)
            msg_temp = "Mi TimeStamp se ha actualizado: TS=%d" % (self.ts)
            self.printer.msg(self.id,msg_temp)
            