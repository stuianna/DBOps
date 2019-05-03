import sqlite3 as sq
import sys
import pandas as pd
import logging

def createTableIfNotExist(con,tableName,columns):

    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS {}({})".format(tableName,columns))
    con.commit()
    return getColumnNames(con,tableName)

def removeTable(con,tableName):

    cur = con.cursor()
    cur.execute("DROP TABLE {}".format(tableName))
    con.commit()

def getTableNames(con):

    cur = con.cursor()
    finalList = []
    tupleList = cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    for l in tupleList:
        finalList.append(l[0])
    return finalList

def getColumnNames(con,table):

    cur = con.cursor()
    cur.execute("PRAGMA table_info({})".format(table));
    fullList = cur.fetchall()
    nameList = []

    for item in fullList:
        nameList.append(item[1])

    return nameList

def printTable(con, table):

    cur = con.cursor()
    for row in cur.execute("SELECT * FROM {}".format(table)):
        print(row)

def table2Df(con,table):

    cur = con.cursor()
    cur.execute("SELECT * FROM {}".format(table))
    df = pd.DataFrame(cur.fetchall())
    return df

#This assumes that the table contains a column named "Timestamp"
def getLastTimeEntry(con,table):

    cur = con.cursor()
    entry = cur.execute("SELECT * FROM {} ORDER BY Timestamp DESC LIMIT 1".format(table))
    lastEntry = entry.fetchall()
    if len(lastEntry) > 0:
        return lastEntry
    else:
        lastEntry = 0
        return lastEntry

def getRowRange(con,table,column,minimum,maximum):

    cur = con.cursor()
    return pd.DataFrame(cur.execute("SELECT * FROM {} WHERE {} BETWEEN {} AND {}".format(table,column,minimum,maximum)).fetchall())

def getLastRows(con,table,maximum):

    cur = con.cursor()
    return pd.DataFrame(cur.execute("SELECT * FROM {} ORDER BY Timestamp DESC LIMIT {}".format(table,maximum)).fetchall())

def removeRowRange(con,table,column,minimum,maximum):

    cur = con.cursor()
    cur.execute("DELETE FROM {} WHERE {} BETWEEN {} AND {}".format(table,column,minimum,maximum))
    con.commit()

#This is implemented in a silly way.
#Definitely a better way to do this.
def append(con,table,values):

    cur = con.cursor()
    if type(values) is not list:
        cur.execute("INSERT INTO {} values((?))".format(table),(values,))
    elif len(values) is 2:
        cur.execute("INSERT INTO {} values((?),(?))".format(table),(values[0],values[1],))
    elif len(values) is 3:
        cur.execute("INSERT INTO {} values((?),(?),(?))".format(table),(values[0],values[1],values[2],))
    elif len(values) is 4:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],))
    elif len(values) is 5:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],))
    elif len(values) is 6:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],))
    elif len(values) is 7:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],))
    elif len(values) is 8:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],))
    elif len(values) is 9:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],))
    elif len(values) is 10:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],))
    elif len(values) is 11:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],))
    elif len(values) is 12:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],))
    elif len(values) is 13:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],))
    elif len(values) is 14:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],))
    elif len(values) is 15:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],))
    elif len(values) is 16:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],))
    elif len(values) is 17:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],))
    elif len(values) is 18:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],))
    elif len(values) is 19:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],))
    elif len(values) is 20:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],))
    elif len(values) is 21:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],))
    elif len(values) is 22:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],))
    elif len(values) is 23:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],))
    elif len(values) is 24:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],))
    elif len(values) is 25:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],))
    elif len(values) is 26:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],))
    elif len(values) is 27:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],))
    elif len(values) is 28:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],))
    elif len(values) is 29:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],))
    elif len(values) is 30:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],))
    elif len(values) is 31:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],))
    elif len(values) is 32:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],))
    elif len(values) is 33:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],))
    elif len(values) is 34:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],))
    elif len(values) is 35:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],))
    elif len(values) is 36:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],))
    elif len(values) is 37:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],))
    elif len(values) is 38:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],))
    elif len(values) is 39:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],))
    elif len(values) is 40:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],))
    elif len(values) is 41:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],))
    elif len(values) is 42:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],))
    elif len(values) is 43:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],values[42],))
    elif len(values) is 44:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],values[42],values[43],))
    elif len(values) is 45:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],values[42],values[43],values[44],))
    elif len(values) is 46:
        cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],values[42],values[43],values[44],values[45],))

    con.commit()

