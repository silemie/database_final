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

import sys, requests, time, math
from table import table

######################################################################
# Input / Output 
######################################################################

# Input paramaters: input path
# Return: A table loaded data from the input path
# This method loads data from input path
def inputfromfile(input_path):
    start_time = time.time()
    result = table()
    result.loadData(input_path)
    print("Time of inputfromfile is:", (time.time() - start_time))
    return result

# Input paramaters: output path
# This method outputs data given a table and output path
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
    print('Time of outputtofile is:', time.time() - start_time)

######################################################################
# Hash / Btree Index
######################################################################

# Input paramaters: the name of column
# This method creates index with hash structure
def Hash(_table, key):
    start_time = time.time()
    _table.creat_index('H', key)
    print('Time of hash is:', time.time() - start_time)

# Input paramaters: the name of column
# This method creates index with btree structure
def BTree(_table, key):
    start_time = time.time()
    _table.creat_index('T', key)
    print('Time of btree is:', time.time() - start_time)


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
    start_time = time.time()
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
    print('Time of select is:', time.time() - start_time)
    return result

# ------------------------------------------------------------------
# Projection
# ------------------------------------------------------------------

# Input paramaters: table, project conditions
# Return: A table with projected data
# This method projects data given a set of conditions
def project(_table, *cols):
    start_time = time.time()
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
    print('Time of projection is:', time.time() - start_time)
    return new_table

# ------------------------------------------------------------------
# Join
# ------------------------------------------------------------------

# TODO join

# ------------------------------------------------------------------
# Concat
# ------------------------------------------------------------------

# Input paramaters: two tables
# Return: A table with combining two tables
# This method connects two tables together
def concat(_table1, _table2):
    start_time = time.time()
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

    print("Time of concat is", time.time() - start_time)

    return new_table

# ------------------------------------------------------------------
# Data Aggregations: Average, Sum, Count
# ------------------------------------------------------------------

# Input paramaters: table, a name of column
# Return: A table with average value
# This method aggregates data given a table and a key column to calculate average value
def avg(_table, condition):
    start_time = time.time()
    data_size = float(len(_table.data))
    index = _table.findByName(condition)
    total = 0
    try:
        total = math.fsum(float(x[index]) for x in _table.data)
        header = ['Average {}'.format(condition)]
        data = [[total / data_size]]
        new_table = table()
        new_table.setData(data, header)
        print("Time of average is:", time.time() - start_time)
        return new_table
    except ValueError:
        print("ValueError: Column Value is not Valid")
        exit(1)

# Input paramaters: table, a name of column
# Return: A table with sum value
# This method aggregates data given a table and a key column to calculate sum value
def sum(_table, condition):
    start_time = time.time()
    index = _table.findByName(condition)
    total = 0
    try:
        total = math.fsum(float(x[index]) for x in _table.data)
        header = ['Sum {}'.format(condition)]
        data = [[total]]
        new_table = table()
        new_table.setData(data, header)
        print("Time of sum is:", time.time() - start_time)
        return new_table
    except ValueError:
        print("ValueError: Column Value is not Valid")
        exit(1)

# Input paramaters: table
# Return: A table with count value
# This method aggregates data given a table to calculate count value
def count(_table):
    start_time = time.time()
    size = len(_table.data)
    header = ['Count']
    data = [[size]]
    new_table = table()
    new_table.setData(data, header)
    print("Time of count is:", time.time() - start_time)
    return new_table


# ------------------------------------------------------------------
# Group Average, Sum, Count
# ------------------------------------------------------------------

# Input paramaters: table, a name of column, conditions
# Return: A table with group average value
# This method aggregates data by group given a table and a key column to calculate average value
def avggroup(_table, col, *conditions):
    start_time = time.time()
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
    print('Time of avggroup is:', time.time() - start_time)
    return new_table

# Input paramaters: table, a name of column, conditions
# Return: A table with group sum value
# This method aggregates data by group given a table and a key column to calculate sum value
def sumgroup(_table, col, *conditions):
    start_time = time.time()
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
    print('Time of sumgroup is:', time.time() - start_time)
    return new_table

# Input paramaters: table, a name of column, conditions
# Return: A table with group count value
# This method aggregates data by group given a table and a key column to calculate count value
def countgroup(_table, *conditions):
    start_time = time.time()
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
    print('Time of countgroup is:', time.time() - start_time)
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
    start_time = time.time()
    selected_cols = conditions
    names = []
    for col in selected_cols:
        names.append(_table.findByName(col))
    
    sorted_data = sorted(_table.data, key=lambda x: _table.keyfunction(x, names))
    print('Time of sort is:', time.time() - start_time)
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
    start_time = time.time()
    total_size = len(_table.data)
    col_index = _table.findByName(col)
    count = 0
    prev_count = 0
    avg_val = []
    interval_sum = 0
    while count < total_size:
        interval_sum += int(_table.data[count][col_index])
        if(count - prev_count >= interval):
            val = [interval_sum / float(interval)]
            avg_val.append(val)
            interval_sum -= int(_table.data[prev_count][col_index])
            prev_count += 1
        count += 1

    header = ['Moving Average ' + col]
    new_table = table()
    new_table.setData(avg_val, header)
    print("Time of movavg is:", time.time() - start_time)
    return new_table

# Input paramaters: table, a name of column, counting interval
# Return: A table with moving sum value
# This method aggregates data by interval value given a table and a key column to calculate sum value
def movsum(_table, col, interval):
    start_time = time.time()
    total_size = len(_table.data)
    col_index = _table.findByName(col)
    count = 0
    prev_count = 0
    interval_sum = 0
    sum_val = []
    while count < total_size:
        interval_sum += int(_table.data[count][col_index])
        if(count - prev_count >= interval):
            val = [interval_sum]
            sum_val.append(val)
            interval_sum -= int(_table.data[prev_count][col_index])
            prev_count += 1
        count += 1

    header = ['Moving Sum ' + col]
    new_table = table()
    new_table.setData(sum_val, header)
    print("Time of movsum is:", time.time() - start_time)
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
    single_condition = single_condition.replace('(', '').strip()
    single_condition = single_condition.replace(')', '').strip()
    for relop in relops:
        if single_condition.find(relop) != -1:
            exp_left = single_condition.split(relop)[0].strip()
            exp_right = single_condition.split(relop)[1].strip()
            if is_number(exp_left):
                attribute = exp_right
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = _table.findByName(attribute)
                exp = exp_right
                constance = exp_left
            elif is_number(exp_right):
                attribute = exp_left
                for arithop in arithops:
                    attribute = attribute.split(arithop)[0].strip()
                col_index = _table.findByName(attribute)
                exp = exp_left
                constance = exp_right
            for row in _table.data:
                if eval(str(exp.replace(attribute, str(row[col_index])) + relop + constance)):
                    result.append(row)
            new_table = table()
            new_table.setData(result, _table.header, _table.indices)
            return new_table

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
                new_table = table()
                new_table.setData(result, _table.header, _table.indices)
                return new_table
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
                new_table = table()
                new_table.setData(result, _table.header, _table.indices)
                return new_table
        # string equality
        if exp_left.find('\'') > -1:
            col_index = _table.findByName(exp_right)
            exp = exp_left
        elif exp_right.find('\'') > -1:
            col_index = _table.findByName(exp_left)
            exp = exp_right
        for row in _table.data:
            if str(row[col_index]) == exp.replace('\'', ''):
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


######################################################################
# Main Function
######################################################################

if __name__ == "__main__":
    input_file = "sales1.txt"
    output_file = "test.txt"

    R = inputfromfile(input_file)
    R1 = select(R, "(time > 50) or (qty < 30)")
    R2 = project(R1, 'saleid', 'qty', 'pricerange')
    R3 = avg(R1, 'qty')
    R4 = sumgroup(R1, 'time', 'qty')
    R5 = sumgroup(R1, 'qty', 'time', 'pricerange')
    R6 = avggroup(R1, 'qty', 'pricerange')
    
    outputtofile(R1, 'R1.txt')
    outputtofile(R2, 'R2.txt')
    outputtofile(R3, 'R3.txt')
    outputtofile(R4, 'R4.txt')
    outputtofile(R5, 'R5.txt')
    outputtofile(R6, 'R6.txt')

    # S = inputfromfile("sales2.txt")
    # T = join(R, S, 'R.customerid = S.C')
    # T1 = join(R1, S, '(R1.qty > S.Q) and (R1.saleid = S.saleid)')
    # T2 = sort(T1, S_C)
    # T2prime = sort(T1, R1_time, S_C)
    # T3 = movavg(T2prime, R1_qty, 3)
    # T4 = movsum(T2prime, P1_qty, 5)

    Q1 = select(R, 'qty = 5')
    BTree(R, 'qty')
    Q2 = select(R, 'qty = 5')
    Q3 = select(R, 'itemid = 7')
    Hash(R, 'itemid')
    Q4 = select(R, 'itemid = 7')
    Q5 = concat(Q4, Q2)

    outputtofile(Q1, 'Q1.txt')
    outputtofile(Q2, 'Q2.txt')
    outputtofile(Q3, 'Q3.txt')
    outputtofile(Q4, 'Q4.txt')
    outputtofile(Q5, 'Q5.txt')

    # outputtofile(T, T)
    # outputtofile(T1, 'T1.txt')
    # outputtofile(T2, 'T2.txt')
    # outputtofile(T3, 'T3.txt')
    # outputtofile(T4, 'T4.txt')

    