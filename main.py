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
import sys, requests, time, math, re
from table import table

######################################################################
# Input / Output 
######################################################################

def inputfromfile(input_path):

    result = table()
    result.loadData(input_path)
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



######################################################################
# Hash / Btree Index
######################################################################

def Hash(_table, key):
    start_time = time.time()
    _table.creat_index('H', key)


def Btree(_table, key):
    start_time = time.time()
    _table.creat_index('T', key)


######################################################################
# Basic Operations
######################################################################

# ------------------------------------------------------------------
# Selection
# ------------------------------------------------------------------

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
    return result

# ------------------------------------------------------------------
# Projection
# ------------------------------------------------------------------

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
    return new_table

# ------------------------------------------------------------------
# Join
# ------------------------------------------------------------------
def join(_table1,_table2,_table1_name,_table2_name,conditions):
    data1 = _table1.data
    data2 = _table2.data
    header = [_table1_name +'_'+ s for s in _table1.header]
    header += [_table2_name +'_'+ s for s in _table2.header]
    result = table()
    indices1 = _table1.indices
    indices2 = _table2.indices
    result.setData([x+y for x,y in zip(data1,data2)],header,[x+y for x,y in zip(indices1,indices2)])
    if conditions.find('and') != -1:
        condition_list = conditions.split('and')
        for c in condition_list:
            c=c.replace('.','_')
            result = select_single(result, c)
    else:
        conditions = conditions.replace('.','_')
        result = select_single(result, conditions)
    return result

# ------------------------------------------------------------------
# Concat
# ------------------------------------------------------------------

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


    return new_table

# ------------------------------------------------------------------
# Data Aggregations: Average, Sum, Count
# ------------------------------------------------------------------

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
        data = [[total]]
        new_table = table()
        new_table.setData(data, header)
        return new_table
    except ValueError:
        print("ValueError: Column Value is not Valid")
        exit(1)


def count(_table):
    start_time = time.time()
    size = len(_table.data)
    header = ['Count']
    data = [[size]]
    new_table = table()
    new_table.setData(data, header)
    return new_table


# ------------------------------------------------------------------
# Group Average, Sum, Count
# ------------------------------------------------------------------

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

    header = ['Average_' + col]
    for condition in conditions:
        header.append(condition)

    new_table = table()
    new_table.setData(data, header)
    new_table = sort(new_table, *conditions)
    return new_table


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
    header = ['Sum_' + col]
    for condition in conditions:
        header.append(condition)

    new_table = table()
    new_table.setData(data, header)
    new_table = sort(new_table, *conditions)
    return new_table


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
    return new_table


######################################################################
# Sort Operations
######################################################################

# ------------------------------------------------------------------
# Sort
# ------------------------------------------------------------------

def sort(_table, *conditions):
    start_time = time.time()
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

    header = ['MovingAverage_' + col]
    new_table = table()
    new_table.setData(avg_val, header)
    print("Time of movavg is:", time.time() - start_time)
    return new_table

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

    header = ['MovingSum_' + col]
    new_table = table()
    new_table.setData(sum_val, header)
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
                for row in _table.data:
                    if eval(str(exp.replace(attribute, str(row[col_index])) + relop + constance)):
                        result.append(row)
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
            else:
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
        if single_condition.find("'") > -1:
            if exp_left.find('\'') > -1:
                col_index = _table.findByName(exp_right)
                exp = exp_left
            else:
                col_index = _table.findByName(exp_left)
                exp = exp_right
            for row in _table.data:
                if str(row[col_index]) == exp.replace('\'', ''):
                    result.append(row)
            new_table = table()
            new_table.setData(result, _table.header, _table.indices)
            return new_table

        # column equality
        if isinstance(exp_left, str) and isinstance(exp_right, str) :
            col_index1 = _table.findByName(exp_left)
            col_index2 = _table.findByName(exp_right)
            for row in _table.data:
                if str(row[col_index1]) == str(row[col_index2]):
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
        print(table_name, table_name_dict[table_name].header,table_name_dict[table_name].data[:2])
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
            #print('Time of %s is:' % line, time.time() - start_time)
'''
    R := inputfromfile(sales1) // import vertical bar delimited foo, first line has column headers. Suppose they are saleid|itemid|customerid|storeid|time|qty|pricerange In general there can be more or fewer columns than this.
    R1 := select(R, (time > 50) or (qty < 30))  // select * from R where time > 50 or qty < 30
    R2 := project(R1, saleid, qty, pricerange) // select saleid, qty, pricerange from R1
    R3 := avg(R1, qty) // select avg(qty) from R1
    R4 := sumgroup(R1, time, qty) // select sum(time), qty from R1 group by qty
    R5 := sumgroup(R1, qty, time, pricerange) // select sum(qty), time, pricerange from R1 group by time, pricerange
    R6 := avggroup(R1, qty, pricerange) // select avg(qty), pricerange from R1 group by by pricerange
    S := inputfromfile(sales2) // suppose column headers are saleid|I|C|S|T|Q|P
    T := join(R, S, R.customerid = S.C) // select * from R, S where R.customerid = S.C
    T1 := join(R1, S, (R1.qty > S.Q) and (R1.saleid = S.saleid)) // select * from R1, S w
    T2 := sort(T1, S_C) // sort T1 by S_C
    T2prime := sort(T1, R1_time, S_C) // sort T1 by R1_time, S_C (in that order)
    T3 := movavg(T2prime, R1_qty, 3) // perform the three item moving average of T2prime on column R_qty. This will be as long as R_qty with the three way moving average of 4 8 9 7 being 4 6 7 8
    T4 := movsum(T2prime, R1_qty, 5) // perform the five item moving sum of T2prime on column R_qty
    Q1 := select(R, qty = 5) // select * from R where qty=5
    Btree(R, qty) // create an index on R based on column qty Equality selections and joins on R should use the index. All indexes will be on one column (both Btree and Hash)
    Q2 := select(R, qty = 5) // this should use the index
    Q3 := select(R, itemid = 7) // select * from R where itemid = 7
    Hash(R,itemid)
    Q4 := select(R, itemid = 7) // this should use the hash index
    Q5 := concat(Q4, Q2) // concatenate the two tables (must have the same schema) Duplicate rows may result (though not with this example).
    outputtofile(Q5, Q5) // This should output the table Q5 into a file with the same name and with vertical bar separators
    outputtofile(T, T) // This should output the table T
'''
    