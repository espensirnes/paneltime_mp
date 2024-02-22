
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import pickle
import traceback
import datetime
import time
import gc
import transact
import sys
import socket
import threading

import importlib

import importlib.util
import importlib.machinery


class SlaveServer:
	def __init__(self,i):
		self.host = '127.0.0.1'
		self.port = 64512 + i
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.socket.bind((self.host, self.port))
		self.socket.listen(1)
		threading.Thread(target=self.accept, daemon=True).start()
		self.connection = None
		self.f = f

	def accept(self):
		self.connection, self.address = self.socket.accept()

	def kill_request(self):
		if self.connection is None:
			return False
		self.connection.setblocking(0)
		try:
			command = self.connection.recv(1024).decode('utf-8')
		except BlockingIOError:
			return False
		return command == "STOP"

    


class Session:
	def __init__(self, t, s_id, f, server):
		self.d=dict()
		self.d['slave_server'] = server
		while 1:
			(msg,obj) = t.receive()
			response=None
			if msg=='kill':
				sys.exit()
				response=True
			elif msg == 'dict':#a dictionary to be used in the batch will be passed
				self.dict(obj)
			elif msg=='exec':				
				self.exec(f, obj)
			elif msg=='eval':	
				response = self.eval(f, obj)			
			else:
				raise RuntimeError('No valid directive supplied')
			t.send(response)		
			gc.collect()
	
	
	def dict(self, obj):
		f_dict = open(obj, 'rb')
		u = pickle.Unpickler(f_dict)
		d_new = u.load()
		f_dict.close()
		add_to_dict(self.d,d_new)
		
	def exec(self, f, obj):
		sys.stdout = f
		t = time.time()
		exec(obj,globals(),self.d)
		print(f'Procedure: {obj} \nTime used: {time.time()-t}')
		sys.stdout = sys.__stdout__	

	
	def eval(self, f, obj):
		
		sys.stdout = f
		response = eval(obj,globals(),self.d)
		response = dict(response)
		response.pop('slave_server')
		sys.stdout = sys.__stdout__
		return response	


	
def add_to_dict(to_dict,from_dict):
	for i in from_dict:
		to_dict[i]=from_dict[i]

def write(f,txt):
	f.write(str(txt)+'\n')
	f.flush()
	

try: 
	
	t = transact.Transact(sys.stdin, sys.stdout, True)
	fname=os.path.join(t.fpath,f'thread.txt')
	f = open(fname, 'w')	
	
	#Handshake:
	t.send(os.getpid())
	msg, s_id = t.receive()
	server = SlaveServer(s_id)
	t.send((server.host, server.port))
	msg, _ = t.receive()
	fname=os.path.join(t.fpath,f'thread {s_id}.txt')
	f = open(fname, 'w')	
	#Wait for instructions:

	Session(t, s_id, f, server)
except Exception as e:
	
	f.write('SID: %s      TIME:%s \n' %(s_id,datetime.datetime.now()))
	traceback.print_exc(file=f)

	f.flush()
	f.close()
