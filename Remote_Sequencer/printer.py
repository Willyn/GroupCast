from pyactor.context import set_context, create_host, sleep, shutdown, interval, later, serve_forever, Host
import time
import random
import sys
from pyactor.exceptions import TimeoutError

class Printer(object):
    _tell = ['msg']
    def msg(self,idm,msg):
        print idm,msg
        
        