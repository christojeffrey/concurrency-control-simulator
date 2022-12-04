# occ simulator with full rollback(not partial rollback)
import time
import threading

class Table():
    # it has a name and a value
    def __init__(self, name, value):
        self.name = name
        self.value = value
    
    def setValue(self, value):
        self.value = value
    
    def getValue(self):
        return self.value
    
    def __str__(self):
        return "Table name: " + self.name + " Table value: " + str(self.value)
    
    def copy(self):
        # return a copy of the table
        return Table(self.name, self.value)

class Database():
    # consist of a list of table
    def __init__(self):
        self.tables = []

    def addTable(self, table: Table):
        self.tables.append(table)

    def getTable(self, tableName) -> Table | None:
        for table in self.tables:
            if table.name == tableName:
                return table
        return None
    
    def __str__(self):
        print("Number of Tables: ", len(self.tables))
        for table in self.tables:
            print(table)
        return ""
    
    def copy(self):
        # return a copy of the database
        newDatabase = Database()
        for table in self.tables:
            newDatabase.addTable(table.copy())
        return newDatabase

class Operation():
    # represent a single operation
    def __init__(self, readOrWrite, subject, description):
        self.readOrWrite = readOrWrite
        self.subject = subject
        self.description = description
    
    def execute(self, database : Database):
        if(self.readOrWrite == "R"):
            # do nothing. just read
            pass   
        elif(self.readOrWrite == "W"):
            # write happens right after changes happen because we use temporary database
            pass
        else:
            # parse description. return the subject, and the new value. description formated as "A+1"
            object = self.description[0]
            operator = self.description[1]
            value = self.description[2]
            # read the value of object from database
            newSubject = database.getTable(object).getValue()
            # do the operation
            if operator == "+":
                newSubject += int(value)
            elif operator == "-":
                newSubject -= int(value)
            elif operator == "*":
                newSubject *= int(value)
            elif operator == "/":
                newSubject /= int(value)
            else:
                raise Exception("Invalid operator")
    
            # write the new value to database
            database.getTable(self.subject).setValue(newSubject)
        # return the new database
        return database
   
    def __str__(self) -> str:
        return f"readOrWrite: {self.readOrWrite} Subject: {self.subject} Description: {self.description}"

class Transaction():
    # consist of an ID with a list of operations
    def __init__(self, transactionID):
        self.transactionID = transactionID
        self.operations = []
        self.finishedTime = None

    def execute(self,  finishedTransactionsLock, databaseLock):
        isRollback = True
        while(isRollback):
            # kamus
            # take global database
            global database
            # make a copy of the database.
            temporaryDatabase = database.copy()

            global finishedTransactions
   
            # algoritma

            # BEGIN
            # start timestamp
            startTime = time.time()
            print(f"[T{self.transactionID}] BEGIN")


            # MODIFY
            for operation in self.operations:
                print(f"   [T{self.transactionID}] {operation}")
                temporaryDatabase = operation.execute(temporaryDatabase)


            # VALIDATE
            # check if there is any finished transaction during my execution that has modified the data that this transaction has read.
            # if there is, rollback
            # else, commit
            isRollback = False
            with finishedTransactionsLock and databaseLock:
                print(f"[T{self.transactionID}] VALIDATE")
                for finishedTransaction in finishedTransactions:
                    if finishedTransaction.finishedTime > startTime:
                        for finishedOperation in finishedTransaction.operations:
                            if finishedOperation.readOrWrite == "W":
                                # check if i have read the data that has been modified
                                for myOperation in self.operations:
                                    if myOperation.readOrWrite == "R" and myOperation.subject == finishedOperation.subject:
                                        # rollback
                                        isRollback = True
                                        break
                                if isRollback:
                                    break
                        if isRollback:
                            break
                print(f"[T{self.transactionID}] VALIDATE FINISHED")

                # COMMIT or ROLLBACK
                if isRollback:
                    # rollback
                    print(f"[T{self.transactionID}] ROLLBACK")
                else:
                    # commit
                    print(f"[T{self.transactionID}] COMMIT")
                    # add the transaction to the finished transactions
                    self.finishedTime = time.time()
                    finishedTransactions.append(self)
                    # update the database
                    database = temporaryDatabase.copy()
                    
    def addOperation(self, operation):
        self.operations.append(operation)

    def __str__(self) -> str:
        print(f"Transaction ID: {self.transactionID}")
        for operation in self.operations:
            print(operation)
        return ""


# main program
# config
FILENAME = "operations.tsv"
INITAL_VALUE_KEYWORD = 'initial value'
OPERATIONS_KEYWORD = 'operations'

# KAMUS
global database 
database = Database()
databaseLock = threading.Lock()

transactions = []
global finishedTransactions
finishedTransactions = []
finishedTransactionsLock = threading.Lock()

# ALGORITMA
# read and parse the file
with open(FILENAME) as f:
    # reading file has three state: start, initial value, operations.
    state = 'start'
    currentTransactionNumber = 0
    currentTransaction = None
    for line in f:
        line = line.strip()
        line = line.split("\t")
        
        # state detection
        if(line[0] == INITAL_VALUE_KEYWORD):
            state = 'initial value'
            continue
        if(line[0] == OPERATIONS_KEYWORD):
            state = 'operations'
            # skip the next line because it's just a header
            next(f)
            continue  
        
        # state execution
        if(state == 'initial value'):
            # create table
            table = Table(line[0], int(line[1]))
            database.addTable(table)

        if(state == 'operations'):
            # create operation
            transactionNumber = int(line[0])
            # create transaction if needed
            if(currentTransactionNumber != transactionNumber):
                currentTransactionNumber = transactionNumber
                currentTransaction = Transaction(currentTransactionNumber)
                transactions.append(currentTransaction)
            # create operation
            RorW = line[1]
            subject = line[2]
            # line[3] is description, can be nothing
            if(len(line) == 4):
                description = line[3]
            else:
                description = None
            # RorW can be nothing, read, or write
            if(RorW == ''):
                RorW = None
        
            operation = Operation(RorW, subject, description)
            # add operation to transaction
            currentTransaction.addOperation(operation)

# start executing the transactions
print("initial database:")
print(database)
print("=== starting threads ===")
# run the transactions with python threading
for transaction in transactions:
    thread = threading.Thread(target=transaction.execute, args=(finishedTransactionsLock, databaseLock))
    thread.start()

# wait for all the threads to finish
for thread in threading.enumerate():
    if thread is not threading.current_thread():
        thread.join()

print("=== all threads finished ===")
print("finish database:")
print(database)
