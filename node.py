from hashlib import sha1
import mmh3
from thread import *
import threading
import socket
import sys
import os
from os import system, name
import time


#test for FIRST = id=> 8
#SECOND = id => 43
#THIRD = id => 0
#FOURTH = id => 32
M_bits = 6
localhost = ''
# userid = sys.argv[2]

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
        self.id = getObjectId((self.ip+str(self.port)))
        # self.id = int(userid)
        self.m_bits = M_bits
        self.fingerTable=[]
        self.successor = NodeInfo(self.id,self.ip,self.id)
        self.second_successor = NodeInfo(self.id,self.ip,self.port)
        self.predecessor = NodeInfo(self.id,self.ip,self.port)
        self.list_of_nodes_id = [] #this is a set of all the nodes (represented by their port numbers)
        self.nodes_dict = {}
        self.files = []
        self.file_dict = {}
        self.initialize()

    def initialize(self):
        for x in range(1,self.m_bits+1):
            entry = fingerTableEntry(x,self.id,self)
            self.fingerTable.append(entry)
        # print len(self.fingerTable)
        self.nodes_dict[self.id] = self.port
        self.list_of_nodes_id.append(self.id)
        self.init_file_storage()

    def init_file_storage(self):
        # files = []
        path = './'
        for r,d,f in os.walk(path):
            for file in f:
                if '.txt' in file or '.mp4' in file:
                    # files.append(os.path.join(r,file))
                    self.files.append(file)
                    self.file_dict[file] = getObjectId(file)
        # return files

    def print_stored_files(self):
        # for x in self.files:
        #     print x
        for x in self.file_dict:
            print x, ':' ,self.file_dict[x]

    def add_new_file_to_storage(self,filename):
        if filename not in self.files:
            self.files.append(filename)
        if filename not in self.file_dict.keys():
            self.file_dict[filename] = getObjectId(filename)

    def remove_file_from_storage(self,filename):
        if filename in self.files:
            self.files.remove(filename)
        if filename in self.file_dict.keys():
            del self.file_dict[filename]


    def add_new_node_in_list(self,new_node_id,new_node_port):
        if new_node_id not in self.list_of_nodes_id:
            self.list_of_nodes_id.append(new_node_id)
        if new_node_id not in self.nodes_dict.keys():
            self.nodes_dict[new_node_id] = new_node_port

        self.list_of_nodes_id = sorted(self.list_of_nodes_id)
        sorted(self.nodes_dict.keys())

    def remove_node_in_list(self,node_id,node_port):
        if node_id in self.list_of_nodes_id:
            self.list_of_nodes_id.remove(node_id)
        if node_id in self.nodes_dict.keys():
            del self.nodes_dict[node_id]    

    def print_known_nodes_list(self):
        # print '\n printing list of known nodes:'
        print self.nodes_dict
        # print self.list_of_nodes_id
        print self.list_of_nodes_id
            

    def printFingerTable(self):
        # print 'pk'
        for x in range(self.m_bits):
            print 'entry number: ', x
            self.fingerTable[x].printFTentry()
            print '\n\n'
        # print '-------end print finger table----------'

    def printThisNodeInfo(self):
        print 'id:', self.id
        print 'port', self.port
        print 'ip', self.ip
        print 'successor', self.successor.id
        print 'second successor', self.second_successor.id
        print 'predecessor', self.predecessor.id
        # print '--------------print this node info--------------------'

    def get_successor(self,id):
        # print 'get_successor input:', id,'type:', type(id)
        if id == self.id: #case where id == your own key
            # print 'looking for myself'
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
            # self.check_successor_state() #ensures that the successor is online. if not, update successor to sec_successor
            # self.ask_successor_for_sec_successor
            c,addr = s.accept()
            print 'connected to new node at address', addr[0],'and port', addr[1]
            protocol = str(c.recv(1024).decode('ascii'))
            c.sendall('ack')
            print 'incoming message:', protocol

            if protocol == "_send_my_successor_":
                # print 'entered 1'
                self.send_new_node_successor(c)

            elif protocol =="_you are my new successor_":
                # print 'entered 2'
                old_pred_port = self.receive_new_predecessor(c)
                # print 'entered 2.1'
                self.inform_old_pred_of_its_new_successor(old_pred_port)

            elif protocol =="_you_have_a_new_successor_":
                # print 'entered 3'
                self.change_to_new_successor(c)
                # self.share_all_files_with_successor()

            elif protocol == "_i_am_your_new_successor_":
                self.receive_my_new_successor(c)
                # self.share_all_files_with_successor()

            elif protocol == "_who is my second successor_":
                self.give_pred_my_successor(c)

            elif protocol =="_i am your pred, leaving_":
                self.handle_pred_leaving(c)
            
            elif protocol == "_i am your succ, leaving_" :
                self.handle_succ_leaving(c)

            elif protocol == "_succ left suddenly, you my new succ_":
                self.recv_new_pred_after_old_left(c)
            
            elif protocol == "your pred, routine check":
                self.routine_reply_to_pred(c)

            elif protocol == "_you are successor. keep my files_":
                self.receive_all_files_from_predecessor(c)

            elif protocol == "find me a file":
                self.service_a_file_request(c)
            
            elif protocol == '_this file belongs to you_':
                self.receive_request_for_uploading(c)


    def send_new_node_successor(self,c):
        c.sendall(str(self.id).encode('ascii'))
        ack = c.recv(1024)
        port_of_new_node = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        # print 'port of new node:',port_of_new_node
        # print 'received 1'
        # ip_of_new_node = int(c.recv(1024).decode('ascii'))
        # print 'received 2'
        id_of_new_node = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        # print 'id_of_new_node:', id_of_new_node
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
            # print 'sendalling entry:',x
            entry_start = int(c.recv(1024).decode('ascii'))
            c.sendall('ack')
            # print 'entry_start', entry_start, ':type:',type(entry_start)
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
        # print 'closing connection'
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
            # print 'recieving entry:',i,'entry.start',self.fingerTable[i].start
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
        #this function is called the first time when a node joins. there is another similar function that get's called when a node leaves dht and its pred needs to contact its new successor
        #it works almost similarly
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
        # print 'i have just obtained my predecessor: ', new_pred_id
        # print 'from my new successor:', self.successor.id
        self.predecessor.id = int(new_pred_id)
        self.predecessor.port = int(new_pred_port)
        self.ask_successor_for_sec_successor() #update second successor. ask from current successor
        s.close()
        
    def receive_new_predecessor(self,c):
        #this function is called by a successor who has been contacted by a new node (its new predecessor)
        # print '1'
        new_pred_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        # print '2'
        new_pred_port = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        self.add_new_node_in_list(new_pred_id,new_pred_port)
        self.updatefingertableEntry(new_pred_id,new_pred_port)
        # print '3'
        old_pred_port = self.predecessor.port
        #inform new pred of its new pred which is your old pred (e.g) then: A->B, now A->C->B
        #so inform C about A
        c.sendall(str(self.predecessor.id).encode('ascii'))
        ack = c.recv(1024)
        # print '4'
        c.sendall(str(self.predecessor.port).encode('ascii'))
        ack = c.recv(1024)
        # print '5'
        self.predecessor.id = new_pred_id
        self.predecessor.port = new_pred_port
        # if len(self.list_of_nodes_id) == 2: #means now there are two nodes in dht. you and your pred, then your pred is your successor
        
        c.close()
        return old_pred_port
        #return old predecessor port

    def inform_old_pred_of_its_new_successor(self,old_pred_port):
        # print 'inform old pred of its new successor input:', old_pred_port
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
        # print '\n'
        # print 'id of new node:', id_of_new_node
        # print 'current node id', self.id
        # print 'port of new node:', port_of_new_node
        for x in range(self.m_bits):
            # print 'x:',x
            self.fingerTable[x].printFTentry()
            # print '2**(x)',2**(x)
            # print 'distance', ((id_of_new_node - self.id)%2**self.m_bits)
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
            # print '\n\n'
            
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
        if self.successor.id == self.id:
            return
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
        #remove the entry from the finger table
        self.remove_node_in_list(self.predecessor.id,self.predecessor.port)
        self.use_known_nodes_to_update_finger_table()
        self.predecessor.id = new_pred_id
        self.predecessor.port = new_pred_port
        c.close()


    def handle_succ_leaving(self,c):
        #this function is calledwhenever the pred is leaving the dht
        new_succ_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        new_succ_port = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        # print 'removing successor from the list: ', self.successor.id
        self.remove_node_in_list(self.successor.id,self.successor.port)
        self.use_known_nodes_to_update_finger_table()
        self.successor.id = new_succ_id
        self.successor.port = new_succ_port
        c.close()

    def ask_successor_for_sec_successor(self):
        if self.successor.id == self.id:
            'returning in ask succ for sec succ'
            return
        # print 'asking successor',self.successor.id,'for its successor'
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(('',self.successor.port))
        protocol = "_who is my second successor_"
        # print 'connections established'
        s.sendall(str(protocol).encode('ascii'))
        ack = s.recv(1024)
        second_successor_id = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        second_successor_port = int(s.recv(1024).decode('ascii'))
        # print 'connections established'
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

    def handle_successor_thread(self):
        #a function called by the thread that ensures that whenever the successor changes, the states are changed accordingly
        print 'handle successor thread'
        while True:
            time.sleep(10)
            self.check_successor_state()

    def check_successor_state(self):
        #this function is routinely called to check if the successor is online. if not, automatically update the states
        count = 0
        if self.successor.id == self.id:
            return
        while True:
            s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            print 'entered here'
            try:
                s.connect(('',self.successor.port))
                # print 'connection established'
                protocol = "your pred, routine check"
                s.sendall(str(protocol).encode('ascii'))
                ack = s.recv(1024)
                s.sendall(str('All ok').encode('ascii'))
                ack = s.recv(1024)
                s.close()
                print 'My successor:',self.successor.id,'is active'
                return
            except socket.error:
                count+=1
                print 'successor is not replying...'
                time.sleep(3)
            if count == 3:
                print 'successor did not respond 3 times. changing states'
                s.close()
                self.remove_node_in_list(self.successor.id,self.successor.port)
                self.use_known_nodes_to_update_finger_table()
                self.successor.id = self.second_successor.id
                self.successor.port = self.second_successor.port
                if self.successor.id == self.id:
                    self.predecessor.id = self.id
                    self.predecessor.port = self.port
                    # self.contact_new_successor_after_old_left()
                    return
                else:
                    self.contact_new_successor_after_old_left()

    def routine_reply_to_pred(self,c):
        #your predecessor routinely checks on you. Reply it
        #if you fail to reply three times then the pred will assume you have left and will contact your successor
        ack = c.recv(1024)
        c.sendall('ack')
        c.close()



    def contact_new_successor_after_old_left(self):
        
        #this function gets called when the successor has left and your second_successor is now your new successor
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(('',self.successor.port))
        protocol = "_succ left suddenly, you my new succ_"
        #pass on your credentials to the new successor
        s.sendall(str(protocol).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.id).encode('ascii'))
        ack = s.recv(1024)
        s.sendall(str(self.port))
        ack = s.recv(1024)

        #now receive your new second successor
        new_sec_succ_id = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        new_sec_succ_port = int(s.recv(1024).decode('ascii'))
        s.sendall('ack')
        self.second_successor.id = new_sec_succ_id
        self.second_successor.port = new_sec_succ_port
        s.close()
        return

    def recv_new_pred_after_old_left(self,c):
        #this function is called when your pred left suddenly and the new pred is trying to contact you.
        new_pred_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        new_pred_port = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        self.remove_node_in_list(self.predecessor.id,self.predecessor.port)
        self.use_known_nodes_to_update_finger_table()

        #now send your successor
        c.sendall(str(self.successor.id).encode('ascii'))
        ack = c.recv(1024)
        c.sendall(str(self.successor.port).encode('ascii'))
        ack = c.recv(1024)
        self.predecessor.id = new_pred_id
        self.predecessor.port = new_pred_port
        c.close()
        return

    def share_all_files_with_successor(self):
        if self.successor.id == self.id:
            return
        if len(self.files) < 1:
            return
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(('',self.successor.port))
        protocol = "_you are successor. keep my files_"
        s.sendall(protocol.encode('ascii'))
        ack = s.recv(1024)
        for x in self.file_dict:
            s.sendall(str(x).encode('ascii'))
            response = str(s.recv(1024).decode('ascii'))
            if response == "send":
                #case when successor does not have this file
                f = open(x,'rb')
                l = f.read(1024)
                while(l):
                    # print 'Sending'
                    s.sendall(l)
                    ack = s.recv(1024)
                    l = f.read(1024)
                s.sendall("done") #sent when done with this file
                ack = s.recv(1024)
                f.close()
                print x, 'sent successfully'
            elif response == "have_file": # case when successor already has the file
                'successor already has' , x
                continue
        s.sendall("all_done")
        ack = s.recv(1024)
        print 'closing connection'
        s.close()

    def receive_all_files_from_predecessor(self,c):
        while True:
            #receive file names, if have, reply 'have_file', if not, reply 'send'
            #when pred says done, meaning a particular file sent
            #when pred says all_done, meaning all files sent
            file_name = str(c.recv(1024).decode('ascii'))
            if file_name in self.files:
                c.sendall(str('have_file').encode('ascii'))
                print 'already have this file:', file_name
                continue
            if file_name == 'all_done':
                c.sendall('ack')
                print 'closing connection'
                c.close()
                return
            else:
                c.sendall(str('send').encode('ascii'))
                with open(file_name,'wb') as file_to_receive:
                    while True:
                        data = c.recv(1024)
                        c.sendall('ack')
                        if data == 'done':
                            self.add_new_file_to_storage(file_name)
                            file_to_receive.close()
                            print 'received this file:', file_name
                            break
                        if data == 'all_done':
                            print 'received all files'
                            c.close()
                            return
                        # print 'receiving'
                        file_to_receive.write(data)

    
    def send_file_to_this_node(self,file_name,node_id,node_port):
        self.add_new_node_in_list(node_id,node_id)
        self.use_known_nodes_to_update_finger_table() 
        protocol = "_have file you are looking for_"
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        s.connect(('',node_port))
        s.sendall(protocol.encode('ascii'))
        ack = s.recv(1024)
        f = open(file_name,'rb')
        l = f.read(1024)
        while l:
            s.sendall(l)
            ack = s.recv(1024)
            l = f.read(1024)
        s.sendall('done')
        ack = s.recv(1024)
        s.close()

    def receive_file_from_node(self,c,filename):
        with open(filename,'wb') as file_to_receive:
            while True:
                data = c.recv(1024)
                if data == 'done':
                    c.sendall('ack')
                    c.close()
                    file_to_receive.close()
                    self.add_new_file_to_storage(filename)
                    return
                c.sendall('ack')
                file_to_receive.write(data)
        c.close()
            
    def request_a_file(self,filename,some_node_port):
        servicing_node_port = some_node_port
        while True:
            s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect(('',servicing_node_port))
            protocol = "find me a file"
            s.sendall(protocol.encode('ascii'))
            ack = s.recv(1024)

            s.sendall(str(self.id).encode('ascii'))
            ack = s.recv(1024)

            s.sendall(str(self.port).encode('ascii'))
            ack = s.recv(1024)
            
            s.sendall(filename.encode('ascii'))
            response = s.recv(1024).decode('ascii')
            if response == "I have it":
                s.sendall('ack')
                self.receive_file_from_node(s,filename)
                return
            else:
                #else some_node will tell this node the id and port of the node that might have the file
                s.sendall('ack')
                servicing_node_id = int(s.recv(1024).decode('ascii'))
                s.sendall('ack')
                servicing_node_port = int(s.recv(1024).decode('ascii'))
                s.sendall('ack')
                self.add_new_node_in_list(servicing_node_id,servicing_node_port)
                print some_node_port, 'didnt have the file, now contacting',servicing_node_id, 'at port', servicing_node_port
                some_node_port = servicing_node_port
                s.close()
                continue

    def service_a_file_request(self,c):
        #this function is called whenever a node/clients to request a file
        node_id = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        node_port = int(c.recv(1024).decode('ascii'))
        c.sendall('ack')
        self.add_new_node_in_list(node_id,node_port)
        filename = c.recv(1024).decode('ascii')
        if filename in self.file_dict.keys():
            response = "I have it"
            c.sendall(response.encode('ascii'))
            ack = c.recv(1024)
            while True:
                f = open(filename,'rb')
                l = f.read(1024)
                while l:
                    c.sendall(l)
                    ack = c.recv(1024)
                    l = f.read(1024)
                c.sendall('done')
                ack = c.recv(1024)
                c.close()
                return

        else:
            response = "dont have it"
            c.sendall(response)
            ack = c.recv(1024)
            c.sendall(str(self.successor.id).encode('ascii'))
            ack = c.recv(1024)
            c.sendall(str(self.successor.port).encode('ascii'))
            ack = c.recv(1024)
            c.close()
            return

    def reset_file_log(self):
        self.files = []
        self.file_dict = {}

    def check_for_new_file_in_directory(self):
        files = []
        path = './'
        self.reset_file_log()
        for r,d,f in os.walk(path):
            for file in f:
                if '.txt' in file or '.mp4' in file:
                    # files.append(os.path.join(r,file))
                    # self.add_new_file_to_storage(file)
                    self.service_an_upload_request(file)
                    # file_dict[file] = getObjectId(file)


    def service_an_upload_request(self,file_name):
        #this function is called whenever a new file is added to the current node's directory
        #first check if this file should be kept locally based on id
        #otherwise send it to the node whose id is just lower than the id of the file
        file_id = getObjectId(file_name)
        succ_node_id,succ_node_port = self.find_successor(file_id)
        print 'servicing upload request'
        print file_name,': ',file_id
        print 'going to:',succ_node_id
        node_id = succ_node_id
        node_port = succ_node_port
        if succ_node_id == self.id:
            print 'i am keeping this file', file_name
            self.add_new_file_to_storage(file_name)
            return

        while True:
            s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect(('',node_port))
            protocol = '_this file belongs to you_'

            s.sendall(str(protocol).encode('ascii'))
            ack = s.recv(1024)
            
            s.sendall(str(file_name).encode('ascii'))
            # ack = s.recv(1024)

            response = s.recv(1024).decode('ascii')

            if response == 'send':
                print 'sending file',file_name,'with id:',file_id
                print 'to node:', node_port
                f = open(file_name,'rb')
                l = f.read(1024)
                while l:
                    s.sendall(l)
                    ack = s.recv(1024)
                    l = f.read(1024)
                s.sendall('done')
                ack = s.recv(1024)
                f.close()
                s.close()
                return
            elif response == 'already have it':
                s.sendall('ack')
                s.close()
                return
            else:
                #case when the file belongs to some other node
                s.sendall('ack')
                some_other_node_id = s.recv(1024).decode('ascii')
                s.sendall('ack')
                some_other_node_port = s.recv(1024).decode('ascii')
                s.sendall('ack')
                s.close()
                node_id = some_other_node_id
                node_port = some_other_node_port
                print 'this file belongs to some other node'
                self.add_new_node_in_list(node_id,node_port)


    def receive_request_for_uploading(self,c):
        file_name = c.recv(1024).decode('ascii')
        if file_name in self.file_dict.keys():
            c.sendall('already have it'.encode('ascii'))
            ack = c.recv(1024)
            c.close()
            return
        else:
            file_id = getObjectId(file_name)
            file_succ_node_id,file_succ_node_port = self.find_successor(file_id)
            if self.id == file_succ_node_id:
                c.sendall('send'.encode('ascii'))
                with open(file_name,'wb') as file_to_receive:
                    while True:
                        data = c.recv(1024)
                        if data == 'done':
                            c.sendall('ack')
                            self.add_new_file_to_storage(file_name)
                            file_to_receive.close()
                            c.close()
                            return
                        c.sendall('ack')
                        file_to_receive.write(data)
            else:
                c.sendall('send to other node')
                ack = c.recv(1024)
                c.sendall(str(file_succ_node_id).encode('ascii'))
                ack = c.recv(1024)
                c.sendall(str(file_succ_node_port).encode('ascii'))
                ack = c.recv(1024)
                c.close()
                return


        




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
        # check_succ_thread = threading.Thread(target = node.handle_successor_thread)
        # check_succ_thread.start()
        print 'handle successor thread'

        
        # node.printThisNodeInfo()
        while True:
            print '....................'
            print 'enter 1 to view this node info'
            print '2 to view fingertable'
            print '3 to view list of known nodes'
            print '4 to continue'
            print '5 to leave suddenly'
            print '6 to show self owned files'
            print '7 to share all files with successor'
            print '8 to download a file'
            print '9 to check for new files to upload'
            print '"c" to clear the screen'
            print '0 to exit'
            print '....................'            
            
            user_input = raw_input()
            # system('clear')
            # node.check_successor_state()
            # node.ask_successor_for_sec_successor()
            if user_input == '1':
                node.printThisNodeInfo()
            elif user_input == '2':
                node.printFingerTable()
            elif user_input == '3':
                node.print_known_nodes_list()
            elif user_input == '4':
                node.check_successor_state()
                node.ask_successor_for_sec_successor()
                continue
            elif user_input == '5':
                os._exit(1)     
            elif user_input =='6':
                node.print_stored_files()   
            elif user_input =='7':
                node.share_all_files_with_successor()
            elif user_input == '8':
                system('clear')
                file_name = raw_input('\n Enter the filename ')
                if node.successor.id != node.id:
                    some_node_port = int(raw_input('\n Enter port of a node you want to send this request to. Press 0 to send it to your successor '))
                    if some_node_port == '0':
                        node.request_a_file(file_name,node.successor.port)
                    else:
                        node.request_a_file(file_name,some_node_port)
                else:
                    node.request_a_file(file_name,node.successor.port)     

            elif user_input == '9':
                node.check_for_new_file_in_directory()   
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
        # print 'len files = ',len(node.files)
        # node.share_all_files_with_successor()
        # print 'welcome'
        # check_succ_thread = threading.Thread(target = node.handle_successor_thread)
        # check_succ_thread.start()
        while True:
            print '....................'
            print 'enter 1 to view this node info'
            print '2 to view fingertable'
            print '3 to view list of known nodes'
            print '4 to continue'
            print '5 to leave suddenly'
            print '6 to show self owned files' 
            print '7 to share all files with successor'
            print '8 to download a file'
            print '9 to check for new files to upload'
            print '"c" to clear the screen'
            print '0 to exit'
            print '....................'
            
            user_input = raw_input()
            # system('clear')
            # node.ask_successor_for_sec_successor()
            if user_input == '1':
                node.printThisNodeInfo()
            elif user_input == '2':
                node.printFingerTable()
            elif user_input == '3':
                node.print_known_nodes_list()
            elif user_input == '4':
                node.check_successor_state()
                node.ask_successor_for_sec_successor()
            elif user_input == '5':
                os._exit(1)  
            elif user_input =='6':
                node.print_stored_files()
            elif user_input =='7':
                node.share_all_files_with_successor() 
            elif user_input == '8':
                system('clear')
                file_name = raw_input('\n Enter the filename ')
                if node.successor.id != node.id:
                    some_node_port = int(raw_input('\n Enter port of a node you want to send this request to. Press 0 to send it to your successor '))
                    if some_node_port == '0':
                        node.request_a_file(file_name,node.successor.port)
                    else:
                        node.request_a_file(file_name,some_node_port)
                else:
                    node.request_a_file(file_name,node.successor.port)

            elif user_input == '9':
                node.check_for_new_file_in_directory()
                
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

