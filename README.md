#Database Final Project

Students Name: Chenye Xu, Sile Yang

Students ID: cx650, sy2303

## Introductoin

This is a program that implements a miniature relational database with order. The database supports basic algebra operations, I/O operations, and query operations. 

The folllowing tree structure shows four files involving in this program: btree.py, hash.py, table.py, and main.py

├ README.md
├ btree.py
├ hash.py
├ table.py
├ main.py

**main.py**  is the main part of database implementation. It includes basic database operations, group operations, data aggregation operations, I/O operations, and index-related operations.

**table.py** is a data structure of database.table, including essential methods for database operations.

**hash.py** and **btree.py** are data structures related with index. We used hash and btree structures from homework 3, where we specify that both hash and btree are implemented by python package. (builtin library and OOBTree package)

**More details are commented in source codes**

## Input

`python3 main.py input.txt output.txt`

**input.txt**: a txt file containing operations for running database. **Please ensure the correct formats**. There is not an error checker in the program. 

**output.txt**: a txt file to record output from the program (i.e. table data, time records..)

## Output





