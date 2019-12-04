import sys, requests, time, math
from hash import Hash
from btree import BTree

class table:

    def __init__(self):
        self.data = []
        self.header = []
        self.indices = []

    def loadData(self, input_path):
        try:
            with open(input_path, "r") as f:
                for line in f:
                    line = line.replace("\n", "")
                    line = line.split("|")
                    row = [data for data in line]
                    if len(self.header) == 0:
                        self.header = row
                    else:
                        self.data.append(row)
        except FileNotFoundError:
            print("Error: File Does Not Exist")
            exit(1)
    
    def setData(self, data, header, indices = None):
        self.data = data
        self.header = header
        self.indices = indices

    def creat_index(self,mode,key):
        if(mode == 'H'):
            count = 0
            index=Hash(key)
            col_index = self.header.index(key)
            for row in self.data:
                index.insert(count,row[col_index])
                count = count + 1
        self.index.append(index)
        if (mode == 'T'):
            count = 0
            index = BTree(key)
            col_index = self.header.index(key)
            for row in self.data:
                index.insert(count, row[col_index])
                count = count + 1
        self.indices.append(index)

    def findByName(self, name):
        try:
            return self.header.index(name)
        except ValueError:
            print("Value Error: Can't Find the Column: {}".format(name))
            exit(1)

    def findDistinct(self, index):
        distinct_val = set()
        try:
            for data in self.data:
                distinct_val.add(data[index])
            return distinct_val
        except ValueError:
            print("Value Error: Can't Find the Column Index: {}".format(index))
            exit(1)

    def keyfunction(self, x, idxs):
        return tuple((x[i] for i in idxs))