import sys, requests, time
from hash import Hash
from btree import BTree

class database:
    def __init__(self, mode):

        self.data = []
        self.time_record = []
        self.index = 0

        if(mode == 'H'):
            self.structure_mode = "Hash"
            self.key_to_index = Hash()
        
        if(mode == 'T'):
            self.structure_mode = "B-Tree"
            self.key_to_index = BTree()
    
    def load_data(self, input_url):
        start_time = time.time()
        f = requests.get(input_url)
        text = f.text
        data = text.split("\n")
        data = data[1:]
        for line in data:
            line = line.split("|")
            if(len(line) != 2):
                break
            self.insert(int(line[0]), int(line[1]))
        return time.time() - start_time
    
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
    mode = 'H' 
    # mode = 'T'
    db = database(mode)





    

    
