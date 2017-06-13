from pyactor.context import set_context, create_host, sleep, shutdown, interval, serve_forever, later, Host
import time
import random
import signal
import sys
from pyactor.exceptions import TimeoutError
from group import Group
from printer import Printer
from member import Peer

if __name__ == "__main__":
    set_context()
    h = create_host('http://127.0.0.1:1677')
    printer = h.spawn('printer',Printer)
    group = h.spawn('group', Group)
    group.setProxys(printer)
    sleep(2)
    group.active_interval()
    serve_forever()