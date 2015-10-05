
import rabbitmq_API
import cv2


cap = cv2.VideoCapture(0)


ret, frame = cap.read()

#print type(frame)
rabbitmq_API.send_file(frame,"172.25.1.193",'ayush')
rabbitmq_API.recieveQ("172.25.1.193",'ayush')