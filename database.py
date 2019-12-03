import sys, requests, time
from hash import Hash
from btree import BTree

class database:
    def __init__(self, mode):

        self.index = 0
        self.data = []
        self.time_record = []
        self.header = []

        if(mode == 'H'):
            self.structure_mode = "Hash"
            self.key_to_index = Hash()
        
        if(mode == 'T'):
            self.structure_mode = "B-Tree"
            self.key_to_index = BTree()
    
    def inputfromfile(self, input_path):
        start_time = time.time()
        try:
            with open(input_path, "r") as f:
                for line in f:
                    line = line.replace("\n", "")
                    line = line.split("|")
                    row = [data for data in line]
                    if len(self.header) == 0:
                        self.header = row
                    else: 
                        self.insert(int(row[0]), row)
        except FileNotFoundError:
            print("Please input valid input file path")
    
    def outputtofile(self, output_path):
        start_time = time.time()
        try:
            with open(output_path, "w+") as f:
                for h in self.header:
                    f.write(str(h) + " ")
                f.write("\n")
                for line in self.data:
                    for d in line:
                        f.write(str(d) + " ")
                    f.write("\n")
        except FileNotFoundError:
            print("Please input valid output file path")
    
    def insert(self, key, value):
        start_time = time.time()
        index = self.index
        self.data.append(value)
        self.key_to_index.insert(key, index)
        self.index += 1
        self.time_record.append(time.time() - start_time)

    def delete(self, key):
        start_time = time.time()
        index = self.key_to_index.delete(key)

        if(index == -1):
            self.time_record.append(time.time() - start_time)
            return -1
        
        if(self.data[index] == None):
            self.time_record.append(time.time() - start_time)
            return -1
        
        value = self.data[index]
        self.data[index] = "Deleted"
        self.time_record.append(time.time() - start_time)
        return value
    
    def search(self, key):
        start_time = time.time()
        if(len(self.data) < key):
            self.time_record.append(time.time() - start_time)
            return -1
        index = self.key_to_index.search(key)
        if(index == -1):
            self.time_record.append(time.time() - start_time)
            return -1
        value = self.data[index]
        self.time_record.append(time.time() - start_time)
        return value

    def select(self, condition):
        relops = ['=', '!=', '>', '>=', '<', '<=']
        arithops = ['+', '-', '*', '/']
        log_words = ['and','or']
        result = []
        start_time = time.time()
        for data in self.data:
            count = 0
            for log_word in log_words:
                if condition.find(log_word) != -1:
                    conditions = condition.split(log_word)
                    for c in conditions:
                        c = c.replace("(", "").strip()
                        c = c.replace(")", "").strip()
                        for relop in relops:
                            if c.find(relop) != -1:
                                exp_left = c.split(relop)[0].strip()
                                exp_right = c.split(relop)[1].strip()
                                if exp_left.isnumeric:
                                    col_index = self.header.index(exp_left)
                                    exp = exp_right
                                else:
                                    col_index = self.header.index(exp_right)
                                    exp = exp_left
                                if eval(str(data[col_index]) + relop + exp):
                                    count = count+1
                    if log_word.lower()=='or':
                        if count > 0:
                            result.append(data)
                    else:
                        if count == len(conditions):
                            result.append(data)
        self.time_record.append(time.time() - start_time)
        return result
    
    def sort(self, *conditions):
        start_time = time.time()
        selected_cols = conditions
        names = []
        for col in selected_cols:
            names.append(self.findByName(col))
        
        sorted_data = sorted(self.data, key = lambda x : self.keyfunction(x, names))
        return sorted_data
    
    def findByName(self, name):
        return self.header.index(name)

    def keyfunction(self, x, idxs):
        return tuple((x[i] for i in idxs))

def inputfromfile(input_path):
    db = database('H')
    db.inputfromfile(input_path)
    return db

def outputtofile(db, output_path):
    db.outputtofile(output_path)

def select(db, condition):
    table = db.select(condition)
    return table

def sort(db, *conditions):
    table = db.sort(*conditions)
    return table
    
if __name__ == "__main__":

    input_file = "sales1.txt"
    output_file = "test.txt"

    db = inputfromfile(input_file) 
    data = select(db, ' (time > 50) or (qty < 30)')
    table = sort(db, 'itemid', 'saleid')
    outputtofile(db, "R1.txt")



    

    
