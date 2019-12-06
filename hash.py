######################################################################
#                               hash.py                              #
#                 A SIMPLE DATABASE IMPLEMENTATION                   #
######################################################################
#                                                                    #
######################################################################
# Header Comment:                                                    #
#  This file is implementation of Hash structure                     #
#  The Hash class contains insert, search, delete ops                #
######################################################################

import time

class HashOnName:
    def __init__(self, name):
        self.key_to_index = dict()
        self.name = name

    # Input paramaters: key, value
    # Key: key of record
    # Value: index of record in data
    def insert(self, key, value):
        if key in self.key_to_index.keys():
            self.key_to_index[key] += [value]
        else:
            arr = []
            arr += [value]
            self.key_to_index[key] = arr

    # Input paramaters: key
    # Key: key of record
    # Return value: index of deleted record in data
    def delete(self, key):
        index = self.search(key)
        if(index != -1):
            del self.key_to_index[key]
            return index
        return -1

    # Input paramaters: key
    # Key: key of record
    # Return value: index of searched record in data. If not found, return -1
    def search(self, key):
        if self.key_to_index[int(key)] is not None:
            return self.key_to_index[int(key)]
        return []
