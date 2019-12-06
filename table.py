######################################################################
#                              table.py                              #
#                 A SIMPLE DATABASE IMPLEMENTATION                   #
######################################################################
#                                                                    #
######################################################################
# Header Comment:                                                    #
#  This file is implementation of table, the main structure          #
#   in the program                                                   #
#  The table class contains:                                         #
#   1. Constructor                                                   #
#   2. Load data from a file or set data from existing array table   #
#   3. Create index by Hash or Btree structures                      #
#   4. Helper function for database implementations                  #
######################################################################

import sys, requests, time, math
from hash import HashOnName
from btree import BTreeOnName

class table:
    def __init__(self):
        self.data = []
        self.header = []
        self.indices = []

    # Input paramaters: input path
    # This method load data from input path
    def loadData(self, input_path):
        try:
            with open(input_path, "r") as f:
                for line in f:
                    line = line.replace("\n", "")
                    line = line.split("|")
                    row = []
                    for data in line:
                        if data.isdigit():
                            data = int(data)
                        row.append(data)
                    if len(self.header) == 0:
                        self.header = row
                    else:
                        self.data.append(row)
        except FileNotFoundError:
            print("Error: File Does Not Exist")
            exit(1)
    
    # Input paramaters: data, header, index
    # This method set data, header and index(if have) to a existing table
    def setData(self, data, header, indices = None):
        self.data = data
        self.header = header
        self.indices = indices

    # Input paramaters: mode (Hash or Btree), key (column name of key index)
    # This method create an index key given a column and creating mode
    def creat_index(self,mode,key):
        if(mode == 'H'):
            count = 0
            index= HashOnName(key)
            col_index = self.header.index(key)
            for row in self.data:
                index.insert(row[col_index],count)
                count = count + 1
            self.indices.append(index)
        if (mode == 'T'):
            count = 0
            index = BTreeOnName(key)
            col_index = self.header.index(key)
            for row in self.data:
                index.insert(row[col_index],count)
                count = count + 1
            self.indices.append(index)

    # Input paramaters: name of column
    # This method find the index of column given the name of column (for index key)
    def findIndexByName(self,name):
        for index in self.indices:
            if index.name == name:
                return index
        return None

    # Input paramaters: name of column
    # This method find the index of column given the name of column
    def findByName(self, name):
        try:
            return self.header.index(name)
        except ValueError:
            print("Value Error: Can't Find the Column: {}".format(name))

    # Input paramaters: index of the column
    # This method find the distinct categories in the given column
    def findDistinct(self, index):
        distinct_val = set()
        try:
            for data in self.data:
                distinct_val.add(data[index])
            return distinct_val
        except ValueError:
            print("Value Error: Can't Find the Column Index: {}".format(index))
            exit(1)

    # Input paramaters: an entry of data, the selective indices
    # This is a helpful method for sorting
    def keyfunction(self, x, idxs):
        return tuple((x[i] for i in idxs))