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

def inputfromfile(input_path):
    start_time = time.time()
    result = table()
    result.loadData(input_path)
    print("inputfromfile:", (time.time() - start_time))
    return result

def outputtofile(_table, output_path):
    start_time = time.time()
    try:
        with open(output_path, "w+") as f:
            for h in _table.header:
                f.write(str(h) + " ")
            f.write("\n")
            for line in _table.data:
                for d in line:
                    f.write(str(d) + " ")
                f.write("\n")
    except FileNotFoundError:
        print("Error: Path is not valid")
        exit(1)
    print('outputtofile:',time.time() - start_time)

def select_single(_table,single_condition):
    relops = ['!=', '>', '>=', '<', '<=']
    arithops = ['+', '-', '*', '/']
    result = []
    single_condition = single_condition.replace('(','').strip()
    single_condition = single_condition.replace(')','').strip()
    for relop in relops:
        if single_condition.find(relop)!=-1:
            exp_left = single_condition.split(relop)[0].strip()
            exp_right = single_condition.split(relop)[1].strip()
            if exp_left.isnumeric():
                attribute = exp_right
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = _table.findByName(attribute)
                exp = exp_right
                constance = exp_left
            else:
                attribute = exp_left
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = _table.findByName(attribute)
                exp = exp_left
                constance = exp_right
            for row in _table.data:
                if eval(exp.replace(attribute,str(row[col_index]))+ relop + constance):
                    result.append(row)
    return result
    #TODO  equal
'''
    if single_condition.index('=') != -1:
        exp_left = single_condition.split('=')[0].strip()
        exp_right = single_condition.split('=')[1].strip()
        if exp_left.index('\'') != -1:
            col_index = _table.findByName(exp_right)
            exp = exp_left
        elif exp_right.index('\'') != -1:
            col_index = _table.findByName(exp_right)
            exp = exp_left
        for row in _table.data:
            if eval(str(row[col_index]) + relop + exp):
                result.append(row)
        return result
'''
def select(_table, conditions):
    relops = ['>=','<=','!=','>', '<','=']
    arithops = ['+', '-', '*', '/']
    log_words = ['and','or']
    result = []
    start_time = time.time()
    if conditions.find('and') != -1:
        result = _table.data
        condition_list = conditions.split('and')
        for c in condition_list:
            result = select_single(result,c)
    elif conditions.find('or') != -1:
        result = []
        condition_list = conditions.split('or')
        for c in condition_list:
            result.append(select_single(_table,c))
    else:
        select_single(_table,conditions)
    print('select:',time.time() - start_time)
    new_table = table()
    new_table.setData(result, _table.header, _table.indices)
    return new_table

def avg(_table, condition):
    start_time = time.time()
    data_size = float(len(_table.data))
    index = _table.findByName(condition)
    total = 0
    try:
        total = math.fsum(float(x[index]) for x in _table.data)
        header = ['Average {}'.format(condition)]
        data = [total/data_size]
        new_table = table()
        new_table.setData(data, header)
        print("avg:",time.time() - start_time)
        return new_table
    except ValueError:
        print("ValueError: Column Value is not Valid")
        exit(1)

def sum(_table, condition):
    start_time = time.time()
    index = _table.findByName(condition)
    total = 0  
    try:
        total = math.fsum(float(x[index]) for x in _table.data)
        header = ['Sum {}'.format(condition)]
        data = [total]
        new_table = table()
        new_table.setData(data, header)
        print("sum:",time.time() - start_time)
        return new_table
    except ValueError:
        print("ValueError: Column Value is not Valid")
        exit(1)

def count(_table):
    start_time = time.time()
    size = len(_table.data)
    header = ['Count']
    data = [size]
    new_table = table()
    new_table.setData(data, header)
    print("count:", time.time() - start_time)
    return new_table

def group(_table, condition):
    tables = []
    index = _table.findByName(condition)
    groups = _table.findDistinct(index)
    
    for g in groups:
        tables.append(select(_table, '{}={}'.format(_table.header[index], g)))
    
    return tables

def avggroup(_table, col, *conditions):
    start_time = time.time()
    key_index = _table.findByName(col)
    group_index = []
    for condition in conditions:
        group_index.append(_table.findByName(condition))

def sort(_table, *conditions):
    start_time = time.time()
    selected_cols = conditions
    names = []
    for col in selected_cols:
        names.append(_table.findByName(col))

    sorted_data = sorted(_table.data, key = lambda x : _table.keyfunction(x, names))
    print('sort:',time.time() - start_time)
    new_table = table()
    new_table.setData(sorted_data, _table.header, _table.indices)
    return new_table

def Hash(_table,key):
    start_time = time.time()
    _table.creat_index('H',key)
    print('hash:',time.time() - start_time)

def BTree(_table,key):
    start_time = time.time()
    _table.creat_index('T',key)
    print('btree:',time.time() - start_time)

if __name__ == "__main__":

    input_file = "sales1.txt"
    output_file = "test.txt"

    db = inputfromfile(input_file) 
    print("Count is:", count(db).data[0])
    selected_table = select(db, ' (time > 50) or (qty < 30)')
    print("Slected data is", selected_table.header)
    sorted_table = sort(db, 'itemid', 'saleid')
    print("Sorted data is:", sorted_table.header)
    print("Average is:", avg(db, 'qty').data[0])
    print("Sum is:", sum(db, 'qty').data[0])
    # group(db, 'time')
    outputtofile(db, "R1.txt")



    

    
