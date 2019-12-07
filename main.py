######################################################################
#                              main.py                               #
#                 A SIMPLE DATABASE IMPLEMENTATION                   #
######################################################################
#                                                                    #
######################################################################
# Header Comment:                                                    #
#  This file is the main part of database implementation             #
#  Functions Including:                                              #
#   1. Basic Operations: Select, Project, Join                       #
#   2. Data Aggregation Operations: Count, Sum, Avg                  #
#   3. Group Operations: Group Sum, Group Avg, Group Count           #
#   4. Sort Operation                                                #
#   5. Moving Operations: Moving Sum, Moving Avg                     #
#   6. I/O Operations: Inputfromfile, Outputtofile                   #
#   7. Index Related Operations: Index by Hash, Index by BTree       #
######################################################################
import os
import sys, requests, time, math, re
from table import table

######################################################################
# Input / Output 
######################################################################

# Input paramaters: input path
# Return: A table loaded data from the input path
# This method loads data from input path
def inputfromfile(input_path):
    result = table()
    result.loadData(input_path)
    return result

# Input paramaters: output path
# This method outputs data given a table and output path
def outputtofile(_table, output_path):
    try:
        with open(output_path, "w+") as f:
            #for h in _table.header:
                #f.write(str(h) + "\t")
            #f.write("\n")
            for line in _table.data:
                for d in line:
                    f.write(str(d) + "\t")
                f.write("\n")
    except FileNotFoundError:
        print("Error: Path is not valid")
        exit(1)


######################################################################
# Hash / Btree Index
######################################################################

# Input paramaters: the name of column
# This method creates index with hash structure
def Hash(_table, key):
    _table.creat_index('H', key)

# Input paramaters: the name of column
# This method creates index with btree structure
def Btree(_table, key):
    _table.creat_index('T', key)


######################################################################
# Basic Operations
######################################################################

# ------------------------------------------------------------------
# Selection
# ------------------------------------------------------------------

# Input paramaters: table, select conditions
# Return: A table with selective data
# This method selects data given a set of conditions
def select(_table, conditions):
    relops = ['>=', '<=', '!=', '>', '<', '=']
    arithops = ['+', '-', '*', '/']
    log_words = ['and', 'or']
    result = table()
    if conditions.find('and') != -1:
        result = _table
        condition_list = conditions.split('and')
        for c in condition_list:
            result = select_single(result, c)
    elif conditions.find('or') != -1:
        condition_list = conditions.split('or')
        for c in condition_list:
            for d in select_single(_table, c).data:
                if d not in result.data:
                    result.data.append(d)
        result.header=_table.header
    else:
        result = select_single(_table, conditions)
    return result

# ------------------------------------------------------------------
# Projection
# ------------------------------------------------------------------

# Input paramaters: table, project conditions
# Return: A table with projected data
# This method projects data given a set of conditions
def project(_table, *cols):
    col_indices = []
    result = []
    header = []
    for col in cols:
        index = _table.findByName(col)
        col_indices.append(index)
        header.append(_table.header[index])
    for row in _table.data:
        row_data = []
        for col_index in col_indices:
            row_data.append(row[col_index])
        result.append(row_data)
    new_table = table()
    new_table.setData(result,header,_table.indices)
    return new_table

# ------------------------------------------------------------------
# Join
# ------------------------------------------------------------------
def join(_table1,_table2,_table1_name,_table2_name,conditions):
    relops = ['!=', '>', '>=', '<', '<=']
    data1 = _table1.data
    data2 = _table2.data
    header = [_table1_name +'_'+ s for s in _table1.header]
    header += [_table2_name +'_'+ s for s in _table2.header]
    result = table()
    indices1 = _table1.indices
    indices2 = _table2.indices
    new_data = []
    #new_data = [x+y for x,y in zip(data1,data2)] #inner join
    for data_a in data1:
        for data_b in data2:
            row = data_a+data_b
            # multi-conditions
            if conditions.find('and') != -1:
                condition_list = conditions.split('and')
                for c in condition_list:
                    c = c.replace('.', '_')
                    find = False
                    for relop in relops:
                        if c.find(relop) != -1:
                            find = True
                            exp_left = c.split(relop)[0].strip()
                            exp_right = c.split(relop)[1].strip()
                    if not find and c.find('=') != -1 :
                        exp_left = c.split('=')[0].strip()
                        exp_right = c.split('=')[1].strip()
                    col_index1 = header.index(exp_left)
                    col_index2 = header.index(exp_right)
                    if eval(str(row[col_index1]) + '==' + str(row[col_index2])):
                        new_data.append(row)
            # single condition
            else:
                c = conditions.replace('.', '_')
                find = False
                for relop in relops:
                    if c.find(relop) != -1:
                        find = True
                        exp_left = c.split(relop)[0].strip()
                        exp_right = c.split(relop)[1].strip()
                if not find and c.find('=') != -1:
                    exp_left = c.split('=')[0].strip()
                    exp_right = c.split('=')[1].strip()
                col_index1 = header.index(exp_left)
                col_index2 = header.index(exp_right)
                if eval(str(row[col_index1]) + '==' + str(row[col_index2])):
                    new_data.append(row)
                    print(exp_left,col_index1,exp_right,col_index2,row)
    result.setData(new_data,header)

    return result
# ------------------------------------------------------------------
# Concat
# ------------------------------------------------------------------

# Input paramaters: two tables
# Return: A table with combining two tables
# This method connects two tables together
def concat(_table1, _table2):
    header1 = _table1.header
    header2 = _table2.header

    if len(header1) != len(header2):
        print("Error: Data Schemas don't match")
        return None
    
    for i,j in zip(header1, header2):
        if i != j:
           print("Error: Data Schemas don't match")
           return None
    
    data1 = _table1.data
    data2 = _table2.data
    data = [*data1, *data2]
    new_table = table()
    new_table.setData(data, _table1.header)


    return new_table

# ------------------------------------------------------------------
# Data Aggregations: Average, Sum, Count
# ------------------------------------------------------------------

# Input paramaters: table, a name of column
# Return: A table with average value
# This method aggregates data given a table and a key column to calculate average value
def avg(_table, condition):
    data_size = float(len(_table.data))
    index = _table.findByName(condition)
    total = 0
    try:
        total = math.fsum(float(x[index]) for x in _table.data)
        header = ['Average {}'.format(condition)]
        data = [[total / data_size]]
        new_table = table()
        new_table.setData(data, header)
        return new_table
    except ValueError:
        print("ValueError: Column Value is not Valid")
        exit(1)

# Input paramaters: table, a name of column
# Return: A table with sum value
# This method aggregates data given a table and a key column to calculate sum value
def sum(_table, condition):
    index = _table.findByName(condition)
    total = 0
    try:
        total = math.fsum(float(x[index]) for x in _table.data)
        header = ['Sum {}'.format(condition)]
        data = [[total]]
        new_table = table()
        new_table.setData(data, header)
        return new_table
    except ValueError:
        print("ValueError: Column Value is not Valid")
        exit(1)

# Input paramaters: table
# Return: A table with count value
# This method aggregates data given a table to calculate count value
def count(_table):
    size = len(_table.data)
    header = ['Count']
    data = [[size]]
    new_table = table()
    new_table.setData(data, header)
    return new_table


# ------------------------------------------------------------------
# Group Average, Sum, Count
# ------------------------------------------------------------------

# Input paramaters: table, a name of column, conditions
# Return: A table with group average value
# This method aggregates data by group given a table and a key column to calculate average value
def avggroup(_table, col, *conditions):
    key_index = _table.findByName(col)
    tables = groupByMulti(_table, *conditions)
    data = []
    for t in tables:
        row = [avg(t, col).data[0][0]]
        for name in conditions:
            row.append(t.data[0][_table.findByName(name)])
        data.append(row)

    header = ['Average ' + col]
    for condition in conditions:
        header.append(condition)

    new_table = table()
    new_table.setData(data, header)
    new_table = sort(new_table, *conditions)
    return new_table

# Input paramaters: table, a name of column, conditions
# Return: A table with group sum value
# This method aggregates data by group given a table and a key column to calculate sum value
def sumgroup(_table, col, *conditions):
    key_index = _table.findByName(col)
    tables = groupByMulti(_table, *conditions)
    data = []
    for t in tables:
        row = [sum(t, col).data[0][0]]
        for name in conditions:
            row.append(t.data[0][_table.findByName(name)])
        data.append(row)
    header = ['Sum ' + col]
    for condition in conditions:
        header.append(condition)

    new_table = table()
    new_table.setData(data, header)
    new_table = sort(new_table, *conditions)
    return new_table

# Input paramaters: table, a name of column, conditions
# Return: A table with group count value
# This method aggregates data by group given a table and a key column to calculate count value
def countgroup(_table, *conditions):
    tables = groupByMulti(_table, *conditions)

    data = []
    for t in tables:
        row = [count(t).data[0][0]]
        for name in conditions:
            row.append(t.data[0][_table.findByName(name)])
        data.append(row)
    header = ['Count']
    for condition in conditions:
        header.append(condition)

    new_table = table()
    new_table.setData(data, header)
    new_table = sort(new_table, *conditions)
    return new_table


######################################################################
# Sort Operations
######################################################################

# ------------------------------------------------------------------
# Sort
# ------------------------------------------------------------------

# Input paramaters: table, conditions
# Return: A table with sorted data
# This method sorts data by given conditions
def sort(_table, *conditions):
    selected_cols = conditions
    names = []
    for col in selected_cols:
        names.append(_table.findByName(col))
    
    sorted_data = sorted(_table.data, key=lambda x: _table.keyfunction(x, names))
    new_table = table()
    new_table.setData(sorted_data, _table.header, _table.indices)
    return new_table


# ------------------------------------------------------------------
# Moving Average, Sum,
# ------------------------------------------------------------------

# Input paramaters: table, a name of column, counting interval
# Return: A table with moving average value
# This method aggregates data by interval value given a table and a key column to calculate average value
def movavg(_table, col, interval):
    new_table = table()
    data = []
    total_size = len(_table.data)
    col_index = _table.findByName(col)
    count = 0
    prev_count = 0
    interval_sum = 0

    while count < total_size:
        interval_sum += int(_table.data[count][col_index])
        if(count - prev_count < interval):
            val = interval_sum / float(count - prev_count + 1)
        else:
            if(count - prev_count >= interval):
                interval_sum -= int(_table.data[prev_count][col_index])
                prev_count += 1
            val = interval_sum / float(interval)
        row = []
        for d in _table.data[count]:
            row.append(d)
        row.append(val)
        data.append(row)
        count += 1

    header = []
    for h in _table.header:
        header.append(h)
    header.append("Moving Avg" + col)
    new_table.setData(data, header)
    return new_table

# Input paramaters: table, a name of column, counting interval
# Return: A table with moving sum value
# This method aggregates data by interval value given a table and a key column to calculate sum value
def movsum(_table, col, interval):
    new_table = table()
    data = []
    total_size = len(_table.data)
    col_index = _table.findByName(col)
    count = 0
    prev_count = 0
    interval_sum = 0

    while count < total_size:
        interval_sum += int(_table.data[count][col_index])
        if(count - prev_count < interval):
            val = interval_sum 
        else:
            if(count - prev_count >= interval):
                interval_sum -= int(_table.data[prev_count][col_index])
                prev_count += 1
            val = interval_sum 
        row = []
        for d in _table.data[count]:
            row.append(d)
        row.append(val)
        data.append(row)
        count += 1

    header = []
    for h in _table.header:
        header.append(h)
    header.append("Moving Sum" + col)
    new_table.setData(data, header)
    return new_table

######################################################################
# Helper Function
######################################################################
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def select_single(_table, single_condition):
    relops = ['!=', '>', '>=', '<', '<=']
    arithops = ['+', '-', '*', '/']
    result = []
    single_condition = single_condition.replace('(', '').replace(')', '')
    #single_condition = ''.join(single_condition.split())
    # inequality
    for relop in relops:
        if single_condition.find(relop) != -1:
            exp_left = single_condition.split(relop)[0].strip()
            exp_right = single_condition.split(relop)[1].strip()
            if is_number(exp_left): # 1 > qty-5
                attribute = exp_right
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = _table.findByName(attribute)
                exp = exp_right
                constance = exp_left
                for row in _table.data:
                    if eval(str(exp.replace(attribute, str(row[col_index])) + relop + constance)):
                        result.append(row)
            elif is_number(exp_right): # qty-5 > 1
                attribute = exp_left
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = _table.findByName(attribute)
                exp = exp_left
                constance = exp_right
                for row in _table.data:
                    if eval(str(exp.replace(attribute, str(row[col_index])) + relop + constance)):
                        result.append(row)
            else: # column comparision
                attribute1 = exp_left
                for arithop in arithops:
                    attribute1 = attribute1.split(arithop)[0].strip()
                col1_index = _table.findByName(attribute1)
                attribute2 = exp_right
                for arithop in arithops:
                    attribute2 = attribute2.split(arithop)[0].strip()
                col2_index = _table.findByName(attribute2)
                for row in _table.data:
                    if eval(str(exp_left.replace(attribute1, str(row[col1_index])) + relop + str(exp_right.replace(attribute2, str(row[col2_index]))))):
                        result.append(row)
            new_table = table()
            new_table.setData(result, _table.header, _table.indices)
            return new_table
    # equality
    if single_condition.index('=') != -1:
        exp_left = single_condition.split('=')[0].strip()
        exp_right = single_condition.split('=')[1].strip()
        if is_number(exp_left):
            # find by index
            if _table.findIndexByName(exp_right) is not None:
                for i in _table.findIndexByName(exp_right).search(exp_left):
                    result.append(_table.data[i])
                new_table = table()
                new_table.setData(result, _table.header, _table.indices)
                return new_table
            else:
                attribute = exp_right
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = _table.findByName(attribute)
                exp = exp_right
                constance = exp_left
                for row in _table.data:
                    if eval(exp.replace(attribute, str(row[col_index])) + '==' + constance):
                        result.append(row)
                new_table = table()
                new_table.setData(result, _table.header, _table.indices)
                return new_table
        elif is_number(exp_right):
            # find by index
            if _table.findIndexByName(exp_left) is not None:
                for i in _table.findIndexByName(exp_left).search(exp_right):
                    result.append(_table.data[i])
            else:
                attribute = exp_left
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = _table.findByName(attribute)
                exp = exp_left
                constance = exp_right
                for row in _table.data:
                    if eval(exp.replace(attribute, str(row[col_index])) + '==' + constance):
                        result.append(row)
        # string equality
        elif single_condition.find("'") > -1:
            if exp_left.find('\'') > -1:
                col_index = _table.findByName(exp_right)
                exp = exp_left
            else:
                col_index = _table.findByName(exp_left)
                exp = exp_right
            for row in _table.data:
                if str(row[col_index]) == exp.replace('\'', ''):
                    result.append(row)
        # column equality
        elif isinstance(exp_left, str) and isinstance(exp_right, str) :
            col_index1 = _table.findByName(exp_left)
            col_index2 = _table.findByName(exp_right)
            for row in _table.data:
                if eval(str(row[col_index1])+'=='+str(row[col_index2])):
                    result.append(row)
        new_table = table()
        new_table.setData(result, _table.header, _table.indices)
        return new_table


def group(_table, index):
    tables = []
    groups = _table.findDistinct(index)

    for g in groups:
        if isinstance(g, int):
            tables.append(select(_table, '{} = {}'.format(_table.header[index], g)))
        else:
            tables.append(select(_table, "{} = '{}'".format(_table.header[index], g)))

    return tables


def groupByMulti(_table, *conditions):
    indices = []
    for condition in conditions:
        index = _table.findByName(condition)
        indices.append(index)

    tables = [_table]
    for index in indices:
        new_tables = []
        for t in tables:
            grouped_tables = group(t, index)
            for gt in grouped_tables:
                new_tables.append(gt)
        tables = new_tables
    return tables

def paraseInput(line,table_name_dict):

    line = line.split('//')[0]
    line = ''.join(line.split())
    if line.find(':=')!=-1:
        table_name = line.split(":=")[0]
        function_call = line.split(":=")[1]
        function_name, parameters = function_call.split('(',1)
        parameters= parameters.replace('(','').replace(')','')
        # inputfromfile
        if function_name.find('inputfromfile') != -1:
            if parameters.find('.txt') != -1:
                table_name_dict[table_name] = inputfromfile(parameters)
            else:
                table_name_dict[table_name] = inputfromfile(parameters+'.txt')
        # select
        elif function_name.find('select') != -1:
            table_name_dict[table_name] = select(table_name_dict.get(parameters.split(',', 1)[0]),parameters.split(',', 1)[1])
        # project
        elif function_name.find('project') != -1:
            args = parameters.split(',')
            table_name_dict[table_name] = project(table_name_dict.get(args[0]), *args[1:])
        # avggroup
        elif function_name.find('avggroup') != -1:
            args = parameters.split(',')
            table_name_dict[table_name] = avggroup(table_name_dict.get(args[0]), args[1], *args[2:])
        # movavg
        elif function_name.find('movavg') != -1:
            args = parameters.split(',')
            table_name_dict[table_name] = movavg(table_name_dict.get(args[0]), args[1],int(args[2]))
        # movsum
        elif function_name.find('movsum') != -1:
            args = parameters.split(',')
            table_name_dict[table_name] = movsum(table_name_dict.get(args[0]),args[1],int(args[2]))
        # avg
        elif function_name.find('avg') != -1:
            table_name_dict[table_name] = avg(table_name_dict.get(parameters.split(',',1)[0]), parameters.split(',',1)[1])
        # sumgroup
        elif function_name.find('sumgroup') != -1:
            args = parameters.split(',')
            table_name_dict[table_name] = sumgroup(table_name_dict.get(args[0]), args[1], *args[2:])
        # join
        elif function_name.find('join') != -1:
            args = parameters.split(',')
            table_name_dict[table_name] = join(table_name_dict.get(args[0]),table_name_dict.get(args[1]),args[0],args[1],args[2])
        # sort
        elif function_name.find('sort') != -1:
            args = parameters.split(',')
            table_name_dict[table_name] = sort(table_name_dict.get(args[0]), *args[1:])
        # select
        elif function_name.find('select') != -1:
            table_name_dict[table_name] = select(table_name_dict.get(parameters.split(',',1)[0]), parameters.split(',',1)[1])
        # concat
        elif function_name.find('concat') != -1:
            table_name_dict[table_name] = concat(table_name_dict.get(parameters.split(',',1)[0]), table_name_dict.get(parameters.split(',',1)[1]))
        #print(table_name, table_name_dict[table_name].header,table_name_dict[table_name].data[:2])
    else:
        # Hash, Btree, outputtofile
        function_name, parameters = line.split('(', 1)
        parameters = parameters.replace('(', '').replace(')', '')
        if function_name.find('Hash') != -1:
            Hash(table_name_dict.get(parameters.split(',',1)[0]), parameters.split(',',1)[1])
        elif function_name.find('Btree') != -1:
            Btree(table_name_dict.get(parameters.split(',',1)[0]), parameters.split(',',1)[1])
        elif function_name.find('outputtofile') != -1:
            outputtofile(table_name_dict.get(parameters.split(',',1)[0]), parameters.split(',',1)[1])
        else:
            print(line)
            exec(line)

######################################################################
# Main Function
######################################################################

if __name__ == "__main__":
    try:
        with open("inputs.txt", "r") as input_file:
            opeartions = input_file.readlines()
    except FileNotFoundError:
        print("Error: Path is not valid")
        exit(1)
    table_name_dict = dict()
    for line in opeartions:
        if not line.startswith('//'):
            start_time = time.time()
            paraseInput(line,table_name_dict)
            print('Time of %s is:' % line, time.time() - start_time)
    for table in table_name_dict:
        directory = ".//output//"
        if not os.path.isdir(directory):
            os.mkdir(directory)
        file_path = os.path.join(directory, table+".txt")
        outputtofile(table_name_dict.get(table),file_path)
        print(table,len(table_name_dict.get(table).data))
    