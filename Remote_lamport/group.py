from pyactor.context import set_context, create_host, sleep, shutdown, interval, later, serve_forever, Host
import time
import random
import sys
from pyactor.exceptions import TimeoutError
class Group(object):
    _tell = ['setProxys','active_interval','stop_interval','announce','leave','garbage_cleanner','keep_alive','join']
    _ask = ['get_members']
    _ref = ['join','keep_alive','get_members','setProxys']
    
    def __init__(self):
        self.members = {}
        
    def setProxys(self,printer):
        self.printer = printer
        
    def join(self, peer, id_peer):
        timestamp = time.time()
        if not self.members.has_key(id_peer):
            self.members[id_peer]={}
            self.members[id_peer]['url']=peer
            msg_temp = "Agregando a peer %s" % (id_peer)
            self.printer.msg(self.id,msg_temp)
        self.members[id_peer]['time']=timestamp
        
    def keep_alive(self, peer,id_peer):
        if not self.members.has_key(id_peer):
            self.join(peer,id_peer)
        else:
            timestamp = time.time()
            self.members[id_peer]['time']=timestamp
        
        
    def leave(self, peer,id_peer):
        msg_temp = "Haciendo leave de %s" % (id_peer)
        self.printer.msg(self.id,msg_temp)
        self.members.pop(id_peer)
        

    def stop_interval(self):
        self.interval1.set()
    
    def active_interval(self):
        self.printer.msg(self.id,"Activando intervalo garbage_cleanner")
        self.interval1 = interval(self.host, 20, self.proxy, "garbage_cleanner")

    def garbage_cleanner(self):
        timestamp = time.time()
        for clave in self.members.keys():
            temp=self.members.get(clave)
            #print "analizando peer",peer,"de hash",clave,"tiempo",temp,"actual",timestamp
            if (timestamp >= (temp['time'] +20) ):
                self.members.pop(clave)
                msg_temp = "Ha caido peer %s" % (clave)
                self.printer.msg(self.id,msg_temp)
    
    def get_members(self):
        if len(self.members) > 0:
            return self.members
        return None

