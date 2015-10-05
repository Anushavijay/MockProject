__author__ = 'ayush'
import rabbitmq_API
rabbitmq_API.send_file('/home/ayush/Downloads/arch.pdf','172.25.1.193','ayush')
rabbitmq_API.recieve_single('172.25.1.193','ayush')