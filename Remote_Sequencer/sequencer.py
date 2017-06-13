from pyactor.context import set_context, create_host, sleep, shutdown, interval, later, serve_forever, Host
import time
import random
import sys
from pyactor.exceptions import TimeoutError

class Sequencer(object):
    _tell = []
    _ask = ['getCont']
    
    def __init__(self):
        self.cont = 0
        
    def getCont(self):
        self.cont += 1
        return (self.cont - 1)
        
if __name__ == "__main__":
    set_context()
    h = create_host('http://127.0.0.1:1678')
    sequencer = h.spawn('sequencer',Sequencer)
    serve_forever()
