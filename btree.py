######################################################################
#                              btree.py                              #
#                 A SIMPLE DATABASE IMPLEMENTATION                   #
######################################################################
#                                                                    #
######################################################################
import time
from BTrees.OOBTree import OOBTree

class BTreeOnName:
    def __init__(self, name):
        self.key_to_index = OOBTree()
        self.name = name
    
    # Input paramaters: key, value
    # Key: key of record
    # Value: index of record in data
    def insert(self, key, value):
        if self.key_to_index.has_key(key):
            arr = self.key_to_index.__getitem__(key)
            arr += [value]
            self.key_to_index.update({key:arr})
        else:
            arr = []
            arr += [value]
            self.key_to_index.update({key: arr})
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
        if int(key) in self.key_to_index:
            return self.key_to_index[int(key)]
        return -1
