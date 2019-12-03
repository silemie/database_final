import sys, requests, time
from hash import Hash
from btree import BTree

class table:
    def __init__(self):
        self.data = []
        self.header = []
        self.indices = []

    def __init__(self,input_path):
        self.data = []
        self.header = []
        self.indices = []
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
            print("Please input valid input file path")

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
        return self.header.index(name)

    def keyfunction(self, x, idxs):
        return tuple((x[i] for i in idxs))


def inputfromfile(input_path):
    start_time = time.time()
    result = table(input_path)
    print("inputfromfile:", (time.time() - start_time))
    return result

def outputtofile(table, output_path):
    start_time = time.time()
    try:
        with open(output_path, "w+") as f:
            for h in table.header:
                f.write(str(h) + " ")
            f.write("\n")
            for line in table.data:
                for d in line:
                    f.write(str(d) + " ")
                f.write("\n")
    except FileNotFoundError:
        print("Please input valid output file path")
    print('outputtofile:',time.time() - start_time)

def select_single(table,single_condition):
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
                col_index = table.findByName(attribute)
                exp = exp_right
                constance = exp_left
            else:
                attribute = exp_left
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = table.findByName(attribute)
                exp = exp_left
                constance = exp_right
            for row in table.data:
                if eval(exp.replace(attribute,str(row[col_index]))+ relop + constance):
                    result.append(row)
    return result
    #TODO  equal
'''
    if single_condition.index('=') != -1:
        exp_left = single_condition.split('=')[0].strip()
        exp_right = single_condition.split('=')[1].strip()
        if exp_left.index('\'') != -1:
            col_index = table.findByName(exp_right)
            exp = exp_left
        elif exp_right.index('\'') != -1:
            col_index = table.findByName(exp_right)
            exp = exp_left
        for row in table.data:
            if eval(str(row[col_index]) + relop + exp):
                result.append(row)
        return result
'''
def select(table, conditions):
    relops = ['>=','<=','!=','>', '<','=']
    arithops = ['+', '-', '*', '/']
    log_words = ['and','or']
    result = []
    start_time = time.time()
    if conditions.find('and') != -1:
        result = table.data
        condition_list = conditions.split('and')
        for c in condition_list:
            result = select_single(result,c)
    elif conditions.find('or') != -1:
        result = []
        condition_list = conditions.split('or')
        for c in condition_list:
            result.append(select_single(table,c))
    else:
        select_single(table,conditions)
    print('select:',time.time() - start_time)
    return result

def sort(table, *conditions):
    start_time = time.time()
    selected_cols = conditions
    names = []
    for col in selected_cols:
        names.append(table.findByName(col))

    sorted_data = sorted(table.data, key = lambda x : table.keyfunction(x, names))
    print('sort:',time.time() - start_time)
    return sorted_data

def Hash(table,key):
    start_time = time.time()
    table.creat_index('H',key)
    print('hash:',time.time() - start_time)

def BTree(table,key):
    start_time = time.time()
    table.creat_index('T',key)
    print('btree:',time.time() - start_time)

if __name__ == "__main__":

    input_file = "sales1.txt"
    output_file = "test.txt"

    db = inputfromfile(input_file) 
    data = select(db, ' (time > 50) or (qty < 30)')
    table = sort(db, 'itemid', 'saleid')
    outputtofile(db, "R1.txt")



    

    
