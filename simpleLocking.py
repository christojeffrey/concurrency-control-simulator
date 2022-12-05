# simple locking simulator (exclusive locks only)
import copy

# VARIABLES
FILENAME = "inputSL.txt"
ITEMS = ["A", "B", "C", "D", "E"]

timeStamp = 1
tableItems = []
transactionItems = []
waitingItems = []


# STRUCTURES
class table():
    def __init__(self, item, key):
        self.item = item
        self.key = key
    

class transaction():
    def __init__(self, key, state, time):
        self.key = key
        self.state = state
        self.time = time
        self.lockedItem = []
        self.blockedItem = []
    
    def locked(self, item):
        self.lockedItem.append(item)
    
    def blocked(self, operation):
        self.blockedItem.append(operation)

    def setBlocked(self, operations):
        self.blockedItem = operations

    def setState(self, state):
        self.state = state


# OPERATION TYPES
def begin(operation):
    global timeStamp
    number = getNumber(operation)
    transactionItems.append(transaction(number, "active", timeStamp))
    timeStamp += 1

def read(operation):
    number = getNumber(operation)
    item = getItem(operation, ITEMS)
    print("[>] R" + str(number) + "(" + item + ")")

    if (tableItems != []):
        for t in tableItems:
            if (t.item == item) and (t.key != number):
                t1 = findTransaction(number)
                t2 = findTransaction(t.key)
                wait(t1, t2, operation)

def write(operation):
    flag = False
    number = getNumber(operation)
    item = getItem(operation, ITEMS)
    print("[>] W" + str(number) + "(" + item + ")")

    if (tableItems == []):
        t = table(item, number)
        tableItems.append(t)
        trans = findTransaction(number)
        trans.locked(item)
    else:
        for t in tableItems:
            if (t.item == item) and (t.key != number):
                flag = True
                print("    [PENDING] W" + str(number) + "(" + item + ")")
                t1 = findTransaction(number)
                t2 = findTransaction(t.key)
                wait(t1, t2, operation)
        if (flag == False):
            newTable = table(item, number)
            tableItems.append(newTable)
            t = findTransaction(number)
            t.locked(item)

def commit(operation):
    number = getNumber(operation)
    item = getItem(operation, ITEMS)
    print("[>] C" + str(number))
    for item in transactionItems:
        if (item.key == number):
            item.setState("committed")
    unlock(number)

def wait(t1, t2, operation):
    if t1.time < t2.time:
        print("[ABORT] T" + t2.key)
        t2.setState("aborted")
        t1.setState("waiting")
        
        if not operation in t1.blockedItem:
            t1.blocked(operation)
        if not t1 in waitingItems:
            waitingItems.append(t1)
        if t2 in waitingItems:
            waitingItems.remove(t2)
        unlock(t2.key)
    else:
        t1.setState("waiting")
        if not operation in t1.blockedItem:
            t1.blocked(operation)
        if not t1 in waitingItems:
            waitingItems.append(t1)

def unlock(key):
    for item in transactionItems:
        if (item.key == key):
            for itemLocked in item.lockedItem:
                for t in tableItems:
                    if (t.item == itemLocked):
                        tableItems.remove(t)
    resume()

def resume():
    if (waitingItems == []):
        return
    else:
        for t in waitingItems:
            temp = copy.deepcopy(t.blockedItem)
            for blockedItem in t.blockedItem:
                t.setState("active")
                check(blockedItem)
                if (t.state != "waiting"):
                    temp.remove(blockedItem)
            t.setBlocked(temp)
            if (t.blockedItem == []):
                waitingItems.remove(t)

def isWaiting(operation):
    number = getNumber(operation)
    item = getItem(operation, ITEMS)
    for t in transactionItems:
        if (t.key == number) and (t.state == "aborted"):
            t.blocked(operation)
            return True
        elif (t.key == number) and (t.state == "waiting"):
            t.blocked(operation)
            return True
    return False


def check(operation):
    flag = isWaiting(operation)
    if (flag == False):
        if (operation.find("R") != -1):
            read(operation)
        elif (operation.find("W") != -1):
            write(operation)
        elif (operation.find("C") != -1):
            commit(operation)
        elif (operation.find("B") != -1):
            begin(operation)
    

# HELPER
def getNumber(operation):
    number = ""
    for char in operation:
        if char.isdigit():
            number += char

    number = int(number)
    return number

def getItem(operation, items):
    item = ""
    for char in items:
        if operation.find(char) != -1:
            item = char
    return item

def findTransaction(key) -> transaction:
    for t in transactionItems:
        if t.key == key:
            return t


# MAIN
with open(FILENAME, 'r') as f:
    print("=== starting ===")
    for operation in f:
        check(operation)

print("=== all finished ===")