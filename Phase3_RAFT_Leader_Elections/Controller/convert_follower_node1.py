import json
import socket
import traceback
import time

# Wait following seconds below sending the controller request
time.sleep(120)

# Read Message Template
msg = json.load(open("Message.json"))

# Initialize
sender = "controller"
target = "Node1"
port = 5555

# Request
msg['sender_name'] = sender
msg['request'] = "view log"
print(f"Request Created : {msg}")

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))
# time.sleep(3)
counter = 0


# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

while True:
    try:
        counter+=1
        msg, addr = skt.recvfrom(1024)
    except:
        print(f"ERROR while fetching from socket : {traceback.print_exc()}")
    print(msg)
    if(counter >= 4):
        break