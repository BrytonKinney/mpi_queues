import pika
import boto3
import random
import socket
from datetime import datetime

class HealthSpotPacker:
	def __init__(self):
		self.sensor_data = {
			'room_num': '',
			'received_datetime': '',
			'override': '',
			'sensor_data': {
				'heart': '',
				'emergency': '',
				'inbed': '',
				'inroom': '',
				'state': ''
			}
		}
		self.dynamodb = boto3.resource('dynamodb')
		self.sensor_table = self.dynamodb.Table('healthspot_sensors')
		
	def check_ready(self):
		for key in self.sensor_data:
			if key != 'sensor_data':
				if self.sensor_data[key] == '' or self.sensor_data[key] == 0:
					return False
			else:
				for key2 in self.sensor_data['sensor_data']:
					if self.sensor_data['sensor_data'][key2] == '':
						return False
		
		return True

	
	def parse_msg(self, queue_name, msg):
		msg = msg.split(',')
		# if heart, return room #, reading, then if the override switch has been flipped
		if queue_name == 'heart':
			return (msg[0], msg[1], msg[2])
		else:
			return (msg[0], msg[1], '')
	
	def add_reading(self, queue_name, reading):
		room_num, reading, override = self.parse_msg(queue_name, reading)
		if override != '':
			self.sensor_data['override'] = int(override)
		self.sensor_data['room_num'] = int(room_num)
		self.sensor_data['received_datetime'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
		self.sensor_data['sensor_data'][queue_name] = reading
		if self.check_ready():
			print("Object filled, sending to AWS DynamoDB.")
			self.sensor_table.put_item(Item = self.sensor_data)
			self.sensor_data = {
				'room_num': 0,
				'received_datetime': '',
				'override': '',
				'sensor_data': {
					'heart': '',
					'emergency': '',
					'inbed': '',
					'inroom': '',
					'state': ''
				}
			}
			

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
ip = s.getsockname()[0]
print("Listening to queues thru RabbitMQ server @ %r" % ip)
s.close()


cred = pika.PlainCredentials("healthspot", 'healthspot')
connection = pika.BlockingConnection(pika.ConnectionParameters(host=ip, port=5672, credentials=cred))
channel = connection.channel()


channel.queue_declare(queue='heart', durable=True, auto_delete=True)
channel.queue_declare(queue='emergency', durable=True, auto_delete=True)
channel.queue_declare(queue='inbed', durable=True, auto_delete=True)
channel.queue_declare(queue='inroom', durable=True,auto_delete=True)
channel.queue_declare(queue='state', durable=True, auto_delete=True)

hspack = HealthSpotPacker()

def callback(ch, method, properties, body):
	print("Received from -> %r" % method.routing_key)
	print(" [=] Received %r" % body)
	hspack.add_reading(method.routing_key, body.decode())
	
	
channel.basic_consume(callback,
                      queue='heart',
                      no_ack = False)
channel.basic_consume(callback,
					  queue='emergency',
					  no_ack = False)
channel.basic_consume(callback,
					  queue='inbed',
					  no_ack = False)
channel.basic_consume(callback,
					  queue='inroom',
					  no_ack = False)
channel.basic_consume(callback,
					  queue='state',
					  no_ack=False)

channel.start_consuming()
