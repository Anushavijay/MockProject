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


#---------------IMPORTING THE MODULES-----------------------#

import pika
import ConfigParser
import logging
import ssl
from requests.exceptions import ConnectionError

import os
import pickle


#Setup our SSL options which gives the path for CA certificates, server certificates
#and client keys.
ssl_option = { 'certfile': '/etc/rabbitmq/client/cert.pem',
               'keyfile': '/etc/rabbitmq/client/key.pem',
               'ca_certs': '/etc/rabbitmq/testca/cacert.pem',
               'cert_reqs': ssl.CERT_REQUIRED ,
               'server_side': False
               }


#---------- CONSTRUCTOR TO INITITIALIZE THE OBJECT----------#

class messageBean:
    def __init__(self, fileExtension, file):
        #Assigning the file's extention to fileExtension
        self.fileExtension = fileExtension
        #Assigning the file's data to file
        self.file = file


#----------- CODE TO READ THE CONFIGURATION FILE -----------#

config = ConfigParser.ConfigParser()
config.read("properties.ini")


#----------- INITIALIZING THE LOG FILE----------------------#

logging.basicConfig(filename='rabbitmq_log2.log', level=logging.DEBUG)


# ----------------------------------------------------------------------------------------------#
# --- DESCRIPTION  : FUNCTION DEFINITION TO SEND A FILE, IT TAKES FILE SOURCE AS AN ARGUMENT ---#
#-----------------   BUNDLES IT A CLASS OBJECT WHICH IS SERIALIZED AND RETURNED ----------------#
# ----------------------------------------------------------------------------------------------#

def getfile(filesrc):
    try:
        # Reading the file from the source given by the user and assigning it to data
        file = open(filesrc, 'r')
        data = file.read()

        # Extracting the file extensions into filext
        filext = str(os.path.split(filesrc)[1])
        file.close()

        # File and file name is bundled into a class object and is then serialized using pickler
        msgBean = messageBean(filext, data)

        # Pickeled class is stored in data_string
        data_string = pickle.dumps(msgBean, -1)
        logging.CRITICAL("FILE SENT TO CONSUMER")
        return data_string

    #Log connection and runtime error
    except {ConnectionError, RuntimeError} as e:
        logging.info(e)


# ---------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO ESTABLISH A BLOCKING CONNECTION       ---#
# ---                   IT TAKES IP ADDRESS AND QUEUE ID AS ARGUMENT      ---#
# ---------------------------------------------------------------------------#

def connect(ip_addr,q_id):
    try:
        credential = pika.PlainCredentials('im_user', 'im_pass')

        # Initializing a blocking connection to connection and making it secure with RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=ip_addr, ssl=True, ssl_options=ssl_option, port=5145))
        chanl = connection.channel()

        # Reading queueid from the queue section in the properties file
        qid = config.get('queue', 'queueid')
        chanl.queue_declare(queue=q_id)
        return chanl

    # Log connection error
    except ConnectionError as e:
        logging.info(e)


# -------------------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO SEND FILE ACROSS THE ESTABLISHED CONNECTION     ---#
# ---                   IT HAS DEPENDENCY ON GETFILE() AND CONNECT() FUNCTIONS      ---#
# ---                   AND TAKES FILESOURCE AND IPADDRESS AS ARGUMENTS             ---#
# -------------------------------------------------------------------------------------#

def send_file(file_src, ip_add,q_id):
    try:
        # Assigning the serialized datastring from function getfile to variable bdy
        bdy = getfile(file_src)

        # Calling the fuction that establishes a secure connection with RabbitMQ
        channel = connect(ip_add,q_id)

        # Reading the queue Id from the queue section in the properties file
        qid = config.get('queue', 'queueid')
        channel.basic_publish(exchange='',
                              routing_key=q_id,
                              body=bdy)
    # Log connection error
    except ConnectionError as e:
        logging.info(e)


# ---------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO ESTABLISH A BLOCKING CONNECTION       ---#
# ---                   IT TAKES IP ADDRESS AS ARGUMENT                   ---#
# ---------------------------------------------------------------------------#
def connect(ipaddr,q_id):
    try:
        qid = config.get('queue', 'queueid')
        credential = pika.PlainCredentials('im_user1', 'im_pass')
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=ipaddr,ssl=True,ssl_options=ssl_option, port=5145))
        channel = connection.channel()
        channel.queue_declare(queue=q_id)
        return channel
    except ConnectionError as e:
        logging.info(e)


# ---------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO RETRIEVE ALL MESSAGES FROM            ---#
# ---                   QUEUE ALL AT ONCE                                 ---#
# ---------------------------------------------------------------------------#

def recieveQ(ipaddr,q_id):
    try:
        # Establishing a secure connection to the RabbitMQ server
        channel = connect(ipaddr,q_id)

    # Function that overrides the callback
        def callback(ch, method, properties, body):

            # Deserialized the recieved object file
            data_loaded = pickle.loads(body)

            # Opens the fileExtension in write mode
            im = open(str(data_loaded.fileExtension), 'w')

            # Write the data of recieved file
            im.write(data_loaded.file)

            # Close the connection
            im.close
            im.close()
    # Reading the queue Id from the queue section in the properties file
        qid = config.get('queue', 'queueid')

    #This callback function receives messages from the queue and no acknowledgment is received
        channel.basic_consume(callback,
                              queue=q_id,
                              no_ack=True)

        channel.start_consuming()
    except ConnectionError as e:
        logging.info(e)


# -----------------------------------------------------------------------------#
# ---   DESCRIPTION  :  FUNCTION TO EXTRACT ONE ELEMENT AT A TIME FROM QUEUE---#
# -----------------------------------------------------------------------------#

def recieve_single(ipaddr,q_id):
    try:
        channel = connect(ipaddr,q_id)
        qid = config.get('queue', 'queueid')
        method_frame, header_frame, body = channel.basic_get(q_id)
        if method_frame:
            data_loaded = pickle.loads(body)
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
