import json
import socket
import time
import threading
import random
import traceback


def create_msg(sender_name,request,Term,key,value):
    msg = {"sender_name": sender_name,
           "request": request,
           "term": Term,
           "key": key,
           "value": value,
           }
    msg_bytes = json.dumps(msg).encode()
    return msg_bytes

# Listener
def listener(skt):
    print(f"Starting Listener ")
    counter= 0
    while True:
        try:
            counter+=1
            msg, addr = skt.recvfrom(1024)
        except:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}")

        # Decoding the Message received from Node 1
        decoded_msg = json.loads(msg.decode('utf-8'))
        print(f"Message Received : {decoded_msg} From : {addr}")
        # print(f"-----------------Here-----------------")
        # print(decoded_msg["term"])
        if(decoded_msg["request"]=="CONVERT_FOLLOWER"):
            print("boxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
            database["dont_sent_and_become"]=True
            pass
        elif(decoded_msg["request"] == "view log"):
            print("cccccccccccccccccccccccccccccccccccccc")
            print(database["log"])
            UDP_Socket.sendto(bytes(str(database["log"]), 'utf-8'), (decoded_msg["sender_name"], 5555))

        elif(decoded_msg["value"]["who_am_I"]=="leader"):
            database["counter"] = database["counter"]- database["HeartBeat"]
            if (len(decoded_msg["value"]["log"])==0):
                latest={}
            else:
                latest=decoded_msg["value"]["log"][int(decoded_msg["value"]["lastLogIndex"])-1]

            if(len(database["log"]) != decoded_msg["value"]["current Term"]-1 ):
                database["log_redo_new_data"] = True
                database["log"]= []
                latest = decoded_msg["value"]["log"]
            appendEntries(term=decoded_msg["term"],
                          leaderId=addr ,
                          prevLogIndex= decoded_msg["value"]["lastLogIndex"],
                          prevLogTerm=decoded_msg["value"]["lastLogTerm"],
                          Entries=latest,
                          leaderCommit=decoded_msg["value"]["leader"])
            pass
        elif(database["voted For"]=="null"):
            database["dont_sent"]=True
            database["vote amount"]=decoded_msg["value"]["vote amount"]
            database["vote amount"].append(decoded_msg["sender_name"])
            if (len(decoded_msg["value"]["log"])==0):
                latest={}
            else:
                latest=decoded_msg["value"]["log"][int(decoded_msg["value"]["lastLogIndex"])-1]

            if(len(database["log"]) != decoded_msg["value"]["current Term"]-1 ):
                database["log_redo_new_data"] = True
                database["log"]= []
                latest = decoded_msg["value"]["log"]
            appendEntries(term=decoded_msg["term"],
                          leaderId=addr ,
                          prevLogIndex= decoded_msg["value"]["lastLogIndex"],
                          prevLogTerm=decoded_msg["value"]["lastLogTerm"],
                          Entries=latest,
                          leaderCommit=decoded_msg["value"]["leader"])
            VOTE_ACK()# this should sent back of who sent them
            countdownHeartBeat()
        elif (database["who_am_I"]== "CONVERT_CO"):
            timeOutHeatBeat()
            pass
        else:
            database["vote amount"].append(decoded_msg["sender_name"])
            database["who_am_I"]=decoded_msg["request"]
            latest=decoded_msg["value"]["log"][int(decoded_msg["value"]["lastLogIndex"])-1]
            appendEntries(term=decoded_msg["term"],
                          leaderId=addr ,
                          prevLogIndex= decoded_msg["value"]["lastLogIndex"],
                          prevLogTerm=decoded_msg["value"]["lastLogTerm"],
                          Entries=latest,
                          leaderCommit=decoded_msg["value"]["leader"])
            database["counter"]=0
            sent_massage()
        if counter>= 4:
            break

def countdownHeartBeat():
    heartbeat_apply= database["HeartBeat"]+database["Timeout"] +database["counter"]
    while True:
        if(database["counter"] != heartbeat_apply):
            threading.Thread(target=listener, args=[UDP_Socket]).start()

            database["counter"]+=1
            time.sleep(1)
            print("COUNTDOWN :coutner--------------------")
            print(database["counter"])
        else:
            requesttype= requestVote(term=database["current Term"],
                                     candidateId=database["candidateId"],
                                     lastLogIndex=database["current Term"],
                                     lastLogTerm=database["current Term"]
                                     )
            node2_msg_test=create_msg(sender_name="node5",
                                      request="CONVERT_CO",
                                      Term=database["current Term"],
                                      key="request",
                                      value=requesttype)
            UDP_Socket.sendto(node2_msg_test, (target, 5555))
            UDP_Socket.sendto(node2_msg_test, (target2, 5555))
            UDP_Socket.sendto(node2_msg_test, (target3, 5555))
            UDP_Socket.sendto(node2_msg_test, (target4, 5555))
            break

def VOTE_ACK( ):
    requesttype= requestVote(term=database["current Term"],
                             candidateId=database["candidateId"],
                             lastLogIndex=database["current Term"],
                             lastLogTerm=database["current Term"]
                             )
    node2_msg_test=create_msg(sender_name="node5",
                              request="vote",
                              Term=database["current Term"],
                              key="vote_ACK",
                              value=requesttype)
    UDP_Socket.sendto(node2_msg_test, (target, 5555))
    UDP_Socket.sendto(node2_msg_test, (target2, 5555))
    UDP_Socket.sendto(node2_msg_test, (target3, 5555))
    UDP_Socket.sendto(node2_msg_test, (target4, 5555))

    pass

def appendEntries(term, leaderId, prevLogIndex, prevLogTerm, Entries, leaderCommit):
    if( database["who_am_I"]=="leader"):
        contain_entry= 0
        for x in database["log"]:
            if "key" in x:
                if (prevLogIndex) == x["term"]:
                    contain_entry+= 1
        if(contain_entry== 0):
            return False
    if(term<database["current Term"]):
        return False
    else:
        database["current Term"]=term
        database["voted For"]=leaderId

        database["lastLogIndex"]=prevLogIndex
        database["lastLogTerm"]=prevLogTerm

        if (database["log_redo_new_data"] == True):
            database["log_redo_new_data"] = False
            database["log"]= Entries
        else:

            #note one more to add
            database["log"].append(Entries)
            #note one more to add
        database["leader"]=leaderCommit

        database["counter"]=0
        print("node5: DataBase Information")
        print(database)
        print(term,
              leaderId,
              prevLogIndex,
              prevLogTerm,
              Entries,
              leaderCommit)

def requestVote(term,candidateId, lastLogIndex,lastLogTerm):
    term +=1
    if(term>database["current Term"]):
        database["candidateId"]=candidateId
        if database["lastLogIndex"]=="null":
            database["lastLogIndex"] =term
        else:
            database["lastLogIndex"] =lastLogIndex
        if database["lastLogTerm"]=="null":
            database["lastLogTerm"]=term
        else:
            database["lastLogTerm"]=lastLogTerm
        database["current Term"]=term
        return database
    else:
        return False

def sent_massage():
    print("--------------------a")
    print(database["who_am_I"])
    print("--------------------a")
    if(database["who_am_I"]=="follower"):
        while True:
            if(database["dont_sent"]==True):
                break
                pass
            elif(database["counter"] != database["Timeout"]):
                database["counter"]+=1
                time.sleep(1)
                print("NODE: 5 :coutner--------------------")
                print(database["counter"])
            else:
                database["vote amount"].append("node5")
                database["voted For"]= ["node5"]
                requesttype= requestVote(term=database["current Term"],
                                         candidateId=database["candidateId"],
                                         lastLogIndex=database["current Term"],
                                         lastLogTerm=database["current Term"]
                                         )
                node2_msg_test=create_msg(sender_name="node5",
                                          request="CONVERT_CO",
                                          Term=database["current Term"],
                                          key="request",
                                          value=requesttype)
                UDP_Socket.sendto(node2_msg_test, (target, 5555))
                UDP_Socket.sendto(node2_msg_test, (target2, 5555))
                UDP_Socket.sendto(node2_msg_test, (target3, 5555))
                UDP_Socket.sendto(node2_msg_test, (target4, 5555))
                database["who_am_I"]="CONVERT_CO"

                break

def timeOutHeatBeat():
    database["counter"]=0
    while True:
        if(database["dont_sent_and_become"]==True):

            break
        elif(database["counter"] != database["HeartBeat"]):
            # threading.Thread(target=listener, args=[UDP_Socket]).start()
            database["counter"]+=1
            time.sleep(1)
            print("NODE: 2 :heartbeat count down--------------------")
            print(database["counter"])

        else:
            if (len(database["log"])==0):
                entris_value={}
            else:
                entris_value ={"term":database["current Term"],"key": "keep alive","value":"oh hi" }

            database["log"].append(entris_value)
            requesttype= requestVote(term=database["current Term"],
                                     candidateId=database["candidateId"],
                                     lastLogIndex=database["current Term"],
                                     lastLogTerm=database["current Term"]
                                     )
            database["vote amount"]=[]
            database["voted For"]="null"
            database["vote amount"]=[]
            database["who_am_I"]="leader"
            node2_msg_test=create_msg(sender_name="node5",
                                      request="CONVERT_Leader",
                                      Term=database["current Term"],
                                      key="keep alive",
                                      value=requesttype)

            UDP_Socket.sendto(node2_msg_test, (target, 5555))
            UDP_Socket.sendto(node2_msg_test, (target2, 5555))
            UDP_Socket.sendto(node2_msg_test, (target3, 5555))
            UDP_Socket.sendto(node2_msg_test, (target4, 5555))
            break
    if(database["dont_sent_and_become"]==True):
        database["counter"]=0
        sent_massage()
    else:
        timeOutHeatBeat()
    pass

timeout_random = random.randrange(10,101)

database= {
    "current Term": 0,
    "voted For": "null",
    "log": [],
    "vote amount":[],
    "Timeout": timeout_random,
    "HeartBeat": 100,
    "candidateId": "null",
    "lastLogIndex":"null",
    "lastLogTerm":"null",
    "leader":"null",
    "counter":0,
    "who_am_I":"follower",
    "dont_sent":False,
    "dont_sent_and_become":False,
    "log_redo_new_data": False,
    "dont_sent_and_become":False
}

if __name__ == "__main__":
    print(f"Starting Node 5")
    sender = "Node5"
    target = "Node1"
    target2 = "Node2"
    target3 = "Node3"
    target4 = "Node4"

    # Creating Socket and binding it to the target container IP and port
    UDP_Socket = socket.socket(family=socket.AF_INET,
                               type=socket.SOCK_DGRAM)

    # Bind the node to sender ip and port
    UDP_Socket.bind((sender, 5555))

    #Starting thread 1
    threading.Thread(target=listener, args=[UDP_Socket]).start()

    sent_massage()

    # print(f"Completed Node Main Thread Node 2")