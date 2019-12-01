import time
from BTrees.OOBTree import OOBTree

class BTree:
    def __init__(self):
        self.key_to_index = OOBTree()
    
    # Input paramaters: key, value
    # Key: key of record
    # Value: index of record in data
    def insert(self, key, value):
        self.key_to_index.update({key : value})

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
        if key in self.key_to_index:
            return self.key_to_index[key]
        return -1
