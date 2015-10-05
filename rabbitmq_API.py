# ----------------------------------------------------------#
# ---   AUTHOR       :  AYUSH SINGH                      ---#
# ---                                                    ---#
# ---   DESCRIPTION  :  Program to create a rabbitMQ     ---#
# ---                   send and receive operation       ---#
# ---                                                    ---#
# ---   DATE         :  29 SEPTEMBER 2015                ---#
# ---                                                    ---#
# ---   VERSION      :  1.0                              ---#
# ----------------------------------------------------------#

# IMPORTING THE MODULES

import pika
import ConfigParser
import logging
import ssl
import six
import cv2
import numpy
from requests.exceptions import ConnectionError

import os
import pickle

ssl_option = {'certfile': '/etc/rabbitmq/client/cert.pem',
              'keyfile': '/etc/rabbitmq/client/key.pem',
              'ca_certs': '/etc/rabbitmq/testca/cacert.pem',
              'cert_reqs': ssl.CERT_REQUIRED,
              'server_side': False
              }


class messageBean:
    # Constructor to initalize the object
    def __init__(self, fileExtension, file):
        self.fileExtension = fileExtension
        self.file = file


# --- CODE TO READ THE CONFIGURATION FILE ---#
config = ConfigParser.ConfigParser()
config.read("properties.ini")
file_type = config.get('file_type', 'opcv2')
logging.basicConfig(filename='rabbitmq_log2.log', level=logging.DEBUG)


# ---------------------------------------------------------------------------------------#
# --- DESCRIPTION  : FUNCTION DEF TO SEND A FILE, IT TAKES FILE SOURCE AS AN ARGUMENT ---#
# ---------------------------------------------------------------------------------------#
def getfile(filesrc):
    try:

        # check if the received argument is a file
        if str(type(filesrc)).find(file_type):
            msgBean = messageBean(filesrc, None)
            data_string = pickle.dumps(msgBean, -1)
            return data_string

        elif os.path.isfile(filesrc):
            file = open(filesrc, 'r')
            # reading the image file
            data = file.read()
            filext = str(os.path.split(filesrc)[1])
            file.close()
            # file and file name is bundled into a class object and is then serialized using pickler
            msgBean = messageBean(filext, data)
            data_string = pickle.dumps(msgBean, -1)
            print " file sent!"
            return data_string

        else:
            print " file sent!"
            data_string = pickle.dumps(filesrc, -1)
            return data_string
    except {ConnectionError, RuntimeError} as e:
        logging.info(e)


# ---------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO ESTABLISH A BLOCKING CONNECTION       ---#
# ---                   IT TAKES IP ADDRESS AS ARGUMENT                   ---#
# ---------------------------------------------------------------------------#


def connect(ip_addr, q_id):
    try:
        credential = pika.PlainCredentials('im_user', 'im_pass')
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=ip_addr, ssl=True, ssl_options=ssl_option, port=5145))
        chanl = connection.channel()
        qid = config.get('queue', 'queueid')
        chanl.queue_declare(queue=q_id)
        return chanl
    except ConnectionError as e:
        logging.info(e)


# -------------------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO SEND FILE ACROSS THE ESTABLISHED CONNECTION     ---#
# ---                   IT HAS DEPENDENCY ON GETFILE() AND CONNECT() FUNCTIONS      ---#
# ---                   AND TAKES FILESOURCE AND IPADDRESS AS ARGUMENTS             ---#
# -------------------------------------------------------------------------------------#

def send_file(file_src, ip_add, q_id):
    try:
        bdy = getfile(file_src)
        channel = connect(ip_add, q_id)
        qid = config.get('queue', 'queueid')
        channel.basic_publish(exchange='',
                              routing_key=q_id,
                              body=bdy)
    except ConnectionError as e:
        logging.info(e)


# ---------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO ESTABLISH A BLOCKING CONNECTION       ---#
# ---                   IT TAKES IP ADDRESS AS ARGUMENT                   ---#
# ---------------------------------------------------------------------------#
def connect(ipaddr, q_id):
    try:
        qid = config.get('queue', 'queueid')
        credential = pika.PlainCredentials('im_user1', 'im_pass')
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=ipaddr, ssl=True, ssl_options=ssl_option, port=5145))
        channel = connection.channel()
        channel.queue_declare(queue=q_id)
        return channel
    except ConnectionError as e:
        logging.info(e)


# ---------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO retrieve the queue all at once        ---#
# ---------------------------------------------------------------------------#

def recieveQ(ipaddr, q_id):
    try:
        channel = connect(ipaddr, q_id)
        qid = config.get('queue', 'queueid')

        def callback(ch, method, properties, body):
            data_loaded = pickle.loads(body)
            if isinstance(data_loaded, six.string_types):
                logging.info(data_loaded)
            elif str(type(data_loaded.fileExtension)).find(file_type):
                cv2.imshow('SERVER', data_loaded.fileExtension)
                cv2.waitKey(0)
            else:
                im = open(str(data_loaded.fileExtension), 'w')
                im.write(data_loaded.file)
                im.close
                im.close()

        channel.basic_consume(callback,
                              queue=q_id,
                              no_ack=True)

        channel.start_consuming()

    except ConnectionError as e:
        logging.info(e)


# -----------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO EXTRACT ONE ELEMENT AT A TIME FROM QUEUE---#
# -----------------------------------------------------------------------------#

def recieve_single(ipaddr, q_id):
    try:
        channel = connect(ipaddr, q_id)
        qid = config.get('queue', 'queueid')
        method_frame, header_frame, body = channel.basic_get(q_id)

        if method_frame:
            data_loaded = pickle.loads(body)

            if isinstance(data_loaded, six.string_types):
                logging.critical(data_loaded)

            elif str(type(data_loaded.fileExtension)).find(file_type):
                cv2.imshow('SERVER', data_loaded.fileExtension)
                cv2.waitKey(0)

            else:
                im = open(str(data_loaded.fileExtension), 'w')
                im.write(data_loaded.file)
                im.close
                im.close()
            channel.basic_ack(method_frame.delivery_tag)

        else:
            print 'No message returned'
            # channel.start_consuming()
    except ConnectionError as e:
        logging.info(e)
