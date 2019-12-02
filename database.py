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

if __name__ == "__main__":

    input_file = sys.argv[1]
    mode = sys.argv[2]
    # mode = 'H' 
    # mode = 'T'
    db = database(mode)
    db.inputfromfile(input_file)
    db.outputtofile("text.txt")





    

    
