import sys, requests, time, math
from table import table


######################################################################
# Input / Output 
######################################################################

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
    print('outputtofile:', time.time() - start_time)


######################################################################
# Hash / Btree Index
######################################################################

def Hash(_table, key):
    start_time = time.time()
    _table.creat_index('H', key)
    print('hash:', time.time() - start_time)


def BTree(_table, key):
    start_time = time.time()
    _table.creat_index('T', key)
    print('btree:', time.time() - start_time)


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
    print('select:', time.time() - start_time)
    return result


# ------------------------------------------------------------------
# Equal
# ------------------------------------------------------------------

# TODO  equal

# ------------------------------------------------------------------
# Projection
# ------------------------------------------------------------------

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

# TODO join

# ------------------------------------------------------------------
# Concat
# ------------------------------------------------------------------

# TODO concat

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
        data = [total / data_size]
        new_table = table()
        new_table.setData(data, header)
        print("avg:", time.time() - start_time)
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
        print("sum:", time.time() - start_time)
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


# ------------------------------------------------------------------
# Group Average, Sum, Count
# ------------------------------------------------------------------

def avggroup(_table, col, *conditions):
    start_time = time.time()
    key_index = _table.findByName(col)
    tables = groupByMulti(_table, *conditions)

    data = []
    for t in tables:
        row = [avg(t, col).data[0]]
        for name in conditions:
            row.append(t.data[_table.findByName(name)])
        data.append(row)
    header = [col]
    for condition in conditions:
        header.append(condition)

    new_table = table()
    new_table.setData(data, header)
    print('avggroup:', time.time() - start_time)
    return new_table


def sumgroup(_table, col, *conditions):
    start_time = time.time()
    key_index = _table.findByName(col)
    tables = groupByMulti(_table, *conditions)

    data = []
    for t in tables:
        row = [sum(t, col).data[0]]
        for name in conditions:
            row.append(t.data[_table.findByName(name)])
        data.append(row)
    header = [col]
    for condition in conditions:
        header.append(condition)

    new_table = table()
    new_table.setData(data, header)
    print('sumgroup:', time.time() - start_time)
    return new_table


def countgroup(_table, *conditions):
    start_time = time.time()
    tables = groupByMulti(_table, *conditions)

    data = []
    for t in tables:
        row = [count(t).data[0]]
        for name in conditions:
            row.append(t.data[_table.findByName(name)])
        data.append(row)
    header = ['Count']
    for condition in conditions:
        header.append(condition)

    new_table = table()
    new_table.setData(data, header)
    print('countgroup:', time.time() - start_time)
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
    print('sort:', time.time() - start_time)
    new_table = table()
    new_table.setData(sorted_data, _table.header, _table.indices)
    return new_table


# ------------------------------------------------------------------
# Moving Average, Sum, Count
# ------------------------------------------------------------------

# TODO moving operations

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
                if eval(str(exp.replace(attribute, row[col_index]) + relop + constance)):
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
        tables.append(select(_table, '{}={}'.format(_table.header[index], g)))

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

    db = inputfromfile(input_file)
    print("Count is:", count(db).data[0])
    selected_table = select(db, "(time > 50) or (qty < 30)")
    print("Slected data is", selected_table.header)
    BTree(db, 'time')
    selected_table = select(db, ' (time = 50) and (qty < 30)')
    print("Slected data is", selected_table.header)
    projected_table = project(db, 'saleid', 'qty', 'pricerange')
    print("Projected data is", projected_table.header)
    sorted_table = sort(db, 'itemid', 'saleid')
    print("Sorted data is:", sorted_table.header)
    print("Average is:", avg(db, 'qty').data[0])
    print("Sum is:", sum(db, 'qty').data[0])
    # print("GroupAvg is:", avggroup(db, 'qty', 'pricerange'))
    # print("GroupSum is:", sumgroup(db, 'qty', 'pricerange'))
    # print("GroupCount is:", countgroup(db, 'pricerange'))
    outputtofile(db, "R1.txt")
