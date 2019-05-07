from hashlib import sha1
import mmh3
from thread import *
import threading
import socket
import sys
import os
from os import system, name
#test for FIRST = id=> 8
#SECOND = id => 43
#THIRD = id => 0
#FOURTH = id => 32
M_bits = 6
localhost = ''
userid = sys.argv[2]

class NodeInfo:
    def __init__(self,id=0,ip=0,port=0):
        self.id = id
        self.ip = ip
        self.port = port

    def setValues(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port

    def printInfo(self):
        print 'ID: ',self.id
        print 'IP: ',self.ip
        print 'Port: ',self.port
        return



class fingerTableEntry:
    def __init__(self, entryNumber, id,node):
        self.start = (id+(2**(entryNumber-1))) % 2**M_bits
        # print self.start, 2**(entryNumber-1), id
        self.intervalfrom = self.start
        self.intervalto = (id+2**(entryNumber))%2**M_bits
        self.successor = NodeInfo(node.successor.id,node.ip,node.port)



     # all attributes are integers/long
    def printFTentry(self):
        print 'start: ',self.start
        print 'interval: ', self.intervalfrom, '-',self.intervalto
        print 'successor: ', self.successor.id



class Node:
    def __init__(self,ip,port):
        self.ip = localhost
        self.port = int(port)
        # self.id = getObjectId((self.ip+str(self.port)))
        self.id = int(userid)
        self.m_bits = M_bits
        self.fingerTable=[]
        self.successor = NodeInfo(self.id,self.ip,self.id)
        self.second_successor = NodeInfo(self.id,self.ip,self.port)
        self.predecessor = NodeInfo(self.id,self.ip,self.port)
        self.list_of_nodes_id = [] #this is a set of all the nodes (represented by their port numbers)
        self.nodes_dict = {}
        self.initialize()

    def initialize(self):
        for x in range(1,self.m_bits+1):
            entry = fingerTableEntry(x,self.id,self)
            self.fingerTable.append(entry)
        # print len(self.fingerTable)
        self.nodes_dict[self.id] = self.port
        self.list_of_nodes_id.append(self.id)

    def add_new_node_in_list(self,new_node_id,new_node_port):
        if new_node_id not in self.list_of_nodes_id:
            self.list_of_nodes_id.append(new_node_id)
        if new_node_id not in self.nodes_dict.keys():
            self.nodes_dict[new_node_id] = new_node_port

        self.list_of_nodes_id = sorted(self.list_of_nodes_id)
        sorted(self.nodes_dict.keys())

    # def remove_node_in_list(self,node_id,node_port):
        

    def print_known_nodes_list(self):
        print '\n printing list of known nodes:'
        print self.nodes_dict
        # print self.list_of_nodes_id
            

    def printFingerTable(self):
        # print 'pk'
        for x in range(self.m_bits):
            print 'entry number: ', x
            self.fingerTable[x].printFTentry()
            print '\n\n'
        print '-------end print finger table----------'

    def printThisNodeInfo(self):
        print 'id:', self.id
        print 'port', self.port
        print 'ip', self.ip
        print 'successor', self.successor.id
        print 'second successor', self.second_successor.id
        print 'predecessor', self.predecessor.id
        print '--------------print this node info--------------------'

    def get_successor(self,id):
        # print 'get_successor input:', id,'type:', type(id)
        if id == self.id: #case where id == your own key
            print 'looking for myself'
            return self.id, self.port
        for x in range(self.m_bits):
            if self.fingerTable[x].intervalfrom <= id and self.fingerTable[x].intervalto > id:
                 #case when id lies between some interval where to > from
                # self.fingerTable[x].successor.printInfo()
                return self.fingerTable[x].successor.id, self.fingerTable[x].successor.port

            elif id < self.id and id <= self.fingerTable[x].intervalfrom and id < self.fingerTable[x].intervalto and self.fingerTable[x].intervalto < self.fingerTable[x].intervalfrom:
                 #case where id lies between some interval where to < from
                return self.fingerTable[x].successor.id, self.fingerTable[x].successor.port                
            elif id == self.fingerTable[x].intervalfrom and id > self.fingerTable[x].intervalto:
                 #case where id == intervalfrom
                return self.fingerTable[x].successor.id, self.fingerTable[x].successor.port
            elif id > self.fingerTable[x].intervalfrom and id > self.fingerTable[x].intervalto and self.fingerTable[x].intervalfrom > self.fingerTable[x].intervalto:
                return self.fingerTable[x].successor.id, self.fingerTable[x].successor.port 

    def find_successor(self,id):
        #this function looks at the known list of nodes to return which node is responsible for the id
        temp = min(self.nodes_dict)
        if id < min(self.nodes_dict):
            return min(self.nodes_dict), self.nodes_dict[min(self.nodes_dict)]
        if id > max(self.nodes_dict):
            return min(self.nodes_dict), self.nodes_dict[min(self.nodes_dict)]

        else:
            for x in sorted(self.nodes_dict):
                if id > x:
                    temp = x
                elif id <= x:
                    temp = x
                    print temp
                    return temp,self.nodes_dict[temp]
            # print temp



    def listen_for_new_nodes(self):
        # print 'listening at port', self.port
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.bind((self.ip, self.port))
        s.listen(100)
        while True:
            self.use_known_nodes_to_update_finger_table()
            # self.ask_successor_for_sec_successor
            c,addr = s.accept()
            print 'connected to new node at address', addr[0],'and port', addr[1]
            protocol = str(c.recv(1024).decode('ascii'))
            c.sendall('ack')
            print protocol

            if protocol == "_send_my_successor_":
                print 'entered 1'
                self.send_new_node_successor(c)

            elif protocol =="_you are my new successor_":
                print 'entered 2'
                old_pred_port = self.receive_new_predecessor(c)
                print 'entered 2.1'
                self.inform_old_pred_of_its_new_successor(old_pred_port)

            elif protocol =="_you_have_a_new_successor_":
                print 'entered 3'
                self.change_to_new_successor(c)

            elif protocol == "_i_am_your_new_successor_":
                self.receive_my_new_successor(c)
                # self.ask_successor_for_sec_successor()

            elif protocol == "_who is my second successor_":
                self.give_pred_my_successor(c)

            elif protocol =="_i am your pred, leaving_":
                self.handle_pred_leaving(c)
            
            elif protocol == "_i am your succ, leaving_" :
                self.handle_succ_leaving(c)


    def send_new_node_successor(self,c):
        c.sendall(str(self.id).encode('ascii'))
        ack = c.recv(1024)
        port_of_new_node = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        print 'port of new node:',port_of_new_node
        # print 'received 1'
        # ip_of_new_node = int(c.recv(1024).decode('ascii'))
        # print 'received 2'
        id_of_new_node = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        print 'id_of_new_node:', id_of_new_node
        # new_node_successor_id, new_node_successor_port = self.get_successor(int(id_of_new_node))
        new_node_successor_id, new_node_successor_port = self.find_successor(int(id_of_new_node))
        c.sendall(str(new_node_successor_id).encode('ascii'))
        self.add_new_node_in_list(id_of_new_node,port_of_new_node)
        ack = c.recv(1024)
        c.sendall(str(new_node_successor_port).encode('ascii'))
        ack = c.recv(1024)
        # self.updatefingertableEntry(id_of_new_node,port_of_new_node)
        # print 'received 3'

        for x in range(self.m_bits):
            print 'sendalling entry:',x
            entry_start = int(c.recv(1024).decode('ascii'))
            c.sendall('ack')
            print 'entry_start', entry_start, ':type:',type(entry_start)
            # print 'done 1'
            entry_start = int(entry_start)
            entry_successor_id,entry_successor_port = self.find_successor(int(entry_start))
            c.sendall(str(entry_successor_id).encode('ascii'))
            ack = c.recv(1024)
            # print 'done 2'
            c.sendall(str(entry_successor_port).encode('ascii'))
            ack = c.recv(1024)
            # print 'done 3'
        # c.shutdown()
        print 'closing connection'
        c.close()
        self.updatefingertableEntry(id_of_new_node,port_of_new_node)
        # thread.start_new_thread(give_successor_for_new_node,(self,c,))
                
    def ask_known_node_for_successor(self,some_port):
        #this function also sets up current nodes finger table based on successor's finger table
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        host = ''
        s.connect((host,some_port))
        # print 'connected to node at port', some_port
        # sendall the protocol
        s.sendall(str("_send_my_successor_").encode('ascii'))
        ack = s.recv(1024)
        # print 'sent 1'
        #receive known node id
        known_node_id = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        self.add_new_node_in_list(known_node_id,some_port)

        s.sendall(str(self.port).encode('ascii'))
        ack = s.recv(1024)
        # s.sendall(str('').encode('ascii'))
        # print 'sent 2'
        s.sendall(str(self.id).encode('ascii'))
        ack = s.recv(1024)
        # print 'sent 3'
        my_successor_id = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        # print 'sent 4'
        my_successor_port = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        self.add_new_node_in_list(my_successor_id,my_successor_port)
        # print 'sent 5'
        for i in range(self.m_bits):
            print 'recieving entry:',i,'entry.start',self.fingerTable[i].start
            s.sendall(str(self.fingerTable[i].start).encode('ascii'))
            ack = s.recv(1024)
            entry_successor_id = int(s.recv(1024).decode('ascii'))
            # print 'received'
            print 'received successor:',entry_successor_id
            s.sendall('ack')
            entry_successor_port = int(s.recv(1024).decode('ascii'))
            s.sendall('ack')
            self.fingerTable[i].successor.id,self.fingerTable[i].successor.port = entry_successor_id,entry_successor_port
        print 'closing connection'
        # s.shutdown()
        s.close()
        self.successor.id = my_successor_id
        self.successor.port = my_successor_port
        self.updatefingertableEntry(self.id,self.port)
        self.updatefingertableEntry(self.successor.id,self.successor.port)

    def contact_my_new_successor(self):
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        # print self.successor.port
        s.connect(('',self.successor.port))
        s.sendall(str("_you are my new successor_").encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.id).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.port).encode('ascii'))
        ack = s.recv(1024)
        self.fingerTable[0].successor.id = self.successor.id
        self.fingerTable[0].successor.port = self.successor.port
        #receive new predecessor from current successor(jiska pred ab tumhara pred hai)
        new_pred_id = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        new_pred_port = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        self.add_new_node_in_list(new_pred_id,new_pred_port)
        print 'i have just obtained my predecessor: ', new_pred_id
        print 'from my new successor:', self.successor.id
        self.predecessor.id = int(new_pred_id)
        self.predecessor.port = int(new_pred_port)
        self.ask_successor_for_sec_successor() #update second successor. ask from current successor
        s.close()
        
    def receive_new_predecessor(self,c):
        #this function is called by a successor who has been contacted by a new node (its new predecessor)
        print '1'
        new_pred_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        print '2'
        new_pred_port = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        self.add_new_node_in_list(new_pred_id,new_pred_port)
        self.updatefingertableEntry(new_pred_id,new_pred_port)
        print '3'
        old_pred_port = self.predecessor.port
        #inform new pred of its new pred which is your old pred (e.g) then: A->B, now A->C->B
        #so inform C about A
        c.sendall(str(self.predecessor.id).encode('ascii'))
        ack = c.recv(1024)
        print '4'
        c.sendall(str(self.predecessor.port).encode('ascii'))
        ack = c.recv(1024)
        print '5'
        self.predecessor.id = new_pred_id
        self.predecessor.port = new_pred_port
        # if len(self.list_of_nodes_id) == 2: #means now there are two nodes in dht. you and your pred, then your pred is your successor
        
        c.close()
        return old_pred_port
        #return old predecessor port

    def inform_old_pred_of_its_new_successor(self,old_pred_port):
        print 'inform old pred of its new successor input:', old_pred_port
        protocol = "_you_have_a_new_successor_"
        if old_pred_port == self.port:
            print 'hahahah'
            self.successor.id = self.predecessor.id
            self.successor.port = self.predecessor.port
            return
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(('',old_pred_port))
        s.sendall(str(protocol).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.predecessor.id).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.predecessor.port).encode('ascii'))
        ack = s.recv(1024)
        s.close()

    def change_to_new_successor(self,c):
        new_succ_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        new_succ_port = int(c.recv(1024).decode('ascii'))
        self.add_new_node_in_list(new_succ_id,new_succ_port)
        c.sendall('ack')
        self.successor.id = int(new_succ_id)
        self.successor.port = int(new_succ_port)
        self.fingerTable[0].successor.id = new_succ_id
        self.fingerTable[0].successor.port = new_succ_port
        c.close()

    def updatefingertableEntry(self,id_of_new_node,port_of_new_node):
        #this method can be called whenever a new node joins
        #use new nodes ip and port to adjust it inside the fingertable
        print '\n'
        print 'id of new node:', id_of_new_node
        print 'current node id', self.id
        # print 'port of new node:', port_of_new_node
        for x in range(self.m_bits):
            print 'x:',x
            self.fingerTable[x].printFTentry()
            print '2**(x)',2**(x)
            print 'distance', ((id_of_new_node - self.id)%2**self.m_bits)
            if id_of_new_node == self.id:
                validate_distance = True
            else:
                validate_distance = abs((id_of_new_node - self.id)%2**self.m_bits) >= 2**(x)
            # print 'start:',self.fingerTable[x].start
            if  validate_distance and id_of_new_node < self.fingerTable[x].successor.id and id_of_new_node >= self.fingerTable[x].start:
                #case where self.fingertable[x].successor.id is greater than new incoming node, replace it.
                # example: start = 4, successor = 9, newnode = 6
                self.fingerTable[x].successor.id = id_of_new_node
                self.fingerTable[x].successor.port = port_of_new_node

            elif validate_distance and id_of_new_node > self.fingerTable[x].successor.id and id_of_new_node >= self.fingerTable[x].start:
                # example: start 6, successor = 0, newnode = 7
                self.fingerTable[x].successor.id = id_of_new_node
                self.fingerTable[x].successor.port = port_of_new_node

            elif validate_distance and id_of_new_node < self.fingerTable[x].successor.id and self.fingerTable[x].successor.id < self.fingerTable[x].start:
                #example: start 12, successor = 11, newnode = 1
                self.fingerTable[x].successor.id = id_of_new_node
                self.fingerTable[x].successor.port = port_of_new_node
            self.fingerTable[x].printFTentry()
            print '\n\n'
            
    def contact_my_new_predecessor(self):
        #this function is called as soon as a new node discovers its predecessor
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        protocol = "_i_am_your_new_successor_"
        s.connect(('',self.predecessor.port))
        s.sendall(str(protocol).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.id).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.port).encode('ascii'))
        ack = s.recv(1024)
        #now receive pred credentials

        # self.predecessor.id = int(s.recv(1024).decode('ascii'))
        # s.sendall('ack')
        # self.predecessor.port = int(s.recv(1024).decode('ascii'))
        # s.sendall('ack')
        s.close()

    def receive_my_new_successor(self,c):
        new_succ_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        new_succ_port = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')

        #now send your own credentials

        # c.sendall(str(self.id).decode('ascii'))
        # ack = c.recv(1024)
        # c.sendall(str(self.port).decode('ascii'))        
        # ack = c.recv(1024)
        c.close()

    def use_known_nodes_to_update_finger_table(self):
        for x in range(self.m_bits):
            self.fingerTable[x].successor.id,self.fingerTable[x].successor.port = self.find_successor(self.fingerTable[x].start)

    def leave_dht(self):
        #node calls this whenever wants to leave
        #inform your pred and succ.
        #transfer files to succ -> work to be done on this
        protocol = "_i am your pred, leaving_"
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('',self.successor.port))
        s.sendall(str(protocol).encode('ascii'))
        ack = s.recv(1024)
        #inform successor of its new predecessor('in this case your pred)
        s.sendall(str(self.predecessor.id).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.predecessor.port).encode('ascii'))
        ack = s.recv(1024)
        s.close()

        #now contact predeccesorr
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(('',self.predecessor.port))
        protocol = "_i am your succ, leaving_"
        s.sendall(str(protocol).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.successor.id).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.successor.port).encode('ascii'))
        ack = s.recv(1024)
        s.close()
        

    def handle_pred_leaving(self,c):
        #this function is called whenever the pred is leaving the dht
        new_pred_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        new_pred_port = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        self.predecessor.id = new_pred_id
        self.predecessor.port = new_pred_port

        c.close()

    def handle_succ_leaving(self,c):
        #this function is calledwhenever the pred is leaving the dht
        new_succ_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        new_succ_port = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        self.successor.id = new_succ_id
        self.successor.port = new_succ_port
        c.close()

    def ask_successor_for_sec_successor(self):
        if self.successor.id == self.id:
            'returning in ask succ for sec succ'
            return
        print 'asking successor',self.second_successor.id,'for its successor'
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(('',self.successor.port))
        protocol = "_who is my second successor_"
        s.sendall(str(protocol).encode('ascii'))
        ack = s.recv(1024)
        second_successor_id = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        second_successor_port = int(s.recv(1024).decode('ascii'))
        print 'my second successor is', second_successor_id
        s.sendall('ack')        
        self.second_successor.id = second_successor_id
        self.second_successor.port = second_successor_port
        s.close()

    def give_pred_my_successor(self,c):
        #this function is called whenever the predecessor asks the successor to return pred's
        #second successor (meaning current node's successor)
        c.sendall(str(self.successor.id).encode('ascii'))
        ack = c.recv(1024)
        c.sendall(str(self.successor.port).encode('ascii'))
        ack = c.recv(1024)
        c.close()





def getObjectId(str):
    hashed = sha1(str).hexdigest()
    hashed_int = int(hashed,16)
    hashed_int = hashed_int % (2**M_bits)
    # print int(hashed_int)
    return int(hashed_int)

def printSuccessorinFT(node):
    for x in range(M_bits):
        # print '{}'.format(x)
        node.fingerTable[x].successor.printInfo()
        print '\n'


def Main(port):
    print 'Welcome to DHT \n'
    some_port = raw_input ('Do you know any node in the current DHT? \n Enter port number of known DHT. Else enter 0 \n')
    ip = localhost
    if some_port == '0':
        node = Node(ip,str(port))
        node.printThisNodeInfo()
        node.printFingerTable()
        # start_new_thread(node.listen_for_new_nodes,(,))
        listening_new_node_thread = threading.Thread(target=node.listen_for_new_nodes)
        listening_new_node_thread.start()
        
        # node.printThisNodeInfo()
        while True:
            print 'enter 1 to view this node info'
            print '2 to view fingertable'
            print '3 to view list of known nodes'
            print '"c" to clear the screen'
            print '0 to exit'
            
            user_input = raw_input()
            # system('clear')
            node.ask_successor_for_sec_successor()
            if user_input == '1':
                node.printThisNodeInfo()
            elif user_input == '2':
                node.printFingerTable()
            elif user_input == '3':
                node.print_known_nodes_list()
            elif user_input == "c":
                system('clear')
            elif user_input == '0':
                # try:
                #     node.leave_dht()
                # except:
                #     print "conn failed"
                node.leave_dht()

                os._exit(1)
                break

    elif int(some_port) < 0:
        print 'incorrect input'
    else:
        node = Node(ip,int(port))
        node.printThisNodeInfo()
        node.ask_known_node_for_successor(int(some_port))
        node.contact_my_new_successor()
        node.contact_my_new_predecessor()
        listening_new_node_thread = threading.Thread(target=node.listen_for_new_nodes)
        listening_new_node_thread.start()
        while True:
            print 'enter 1 to view this node info'
            print '2 to view fingertable'
            print '3 to view list of known nodes'
            print '"c" to clear the screen'
            print '0 to exit'
            
            user_input = raw_input()
            # system('clear')
            node.ask_successor_for_sec_successor()
            if user_input == '1':
                node.printThisNodeInfo()
            elif user_input == '2':
                node.printFingerTable()
            elif user_input == '3':
                node.print_known_nodes_list()
            elif user_input == "c":
                system('clear')
            elif user_input == '0':
                node.leave_dht()
                os._exit(1)
                break


if __name__ == "__main__":
  port = sys.argv[1]
  # port = 12
  ip = localhost
  Main(port)
  # node = Node(ip,str(port))
  # node.printThisNodeInfo()
  # node.printFingerTable()
  # node.get_successor(9)
