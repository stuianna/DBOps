"""
DBOps: Single class module for working with sqlite3 databases.
Created April 2019
Author Stuart Ianna
"""

import sqlite3 as sq
import pandas as pd
import logging
import os

log = logging.getLogger(__name__)


class DBOps():
    """Class for working with a single database

    The logging module is used to log errors and warnings.

    Typical Usage:

    # Create a class instance for a single database
    database = DBOps('database.db')

    # Add a table to the database
    database.createTableIfNotExist('my_table','column_1 NUMERIC, \
        column_2 TEXT, column_3 TEXT')

    # Get all the tables in the database
    database.getTableNames()

    # Add an entry to the database
    database.append('my_table',[13, 'col_entry_2', 'col_entry_3'])

    # Return the table as a Pandas Dataframe
    database.table2Df('my_table')

    # Return a row based on a column query, returns entire matching row
    database.getRow('my_table','column_1','col_entry_1');

    Attributes:
        - con:sqlite3.Connection - Database class object

    Methods:
        - createTableIfNotExist(tableName,columns) - create a new table in the
            connected database, columns are a comma seperated string.
        - removeTable(tableName) - remove the passed table from the database.
        - getTableNames - Returns a list of all tables in the database.
        - getColumnNames(tableName) - Get a list of all column names contained
            in the passed table.
        - printTable(tableName) - print the passed table to stdout.
        - table2Df(tableName) - return the complete table as a dataframe.
        - getLastTimeEntry(table) - get the latest time entry in a table. Note:
            This assumes the table contains a column labeled 'timestamp'
        - getRowRange(table,column,minimum,maximum) Returns a dataframe of
            entries whose column matched the specific values.
        - getLastRows(table,maximum) - returns up to the maximum rows as a
            dataframe. This assumes there is a column named timestamp.
        - removeRowRange(table,column,minimum,maximum) - remove rows from the
            table where the column values match.
        - append(table,values) - add an entry to the database, value are passed
            as a list. Note: the number of items in the list must match the
            number of columns.
    """

    def __init__(self, db_name):

        self.__dbName = db_name
        self.con = None
        self.createDatabase()

    def createDatabase(self):
        try:
            self.con = sq.connect(self.__dbName)
        except Exception as e:
            log.error('Cannot connect to database {}, raised exception {}.'.format(self.__dbName, e))

    def exists(self):
        if os.path.exists(self.__dbName) and self.con is not None:
            return True
        return False

    def __checkDatabaseIsInitialised(self):
        if self.con is None:
            logging.error("Trying to operate on a database which doesn't exist \
                          or hasn't been initialised.")
            return False
        return True

    def createTableIfNotExist(self, tableName, columns):

        if self.__checkDatabaseIsInitialised() is False:
            return []

        cur = self.con.cursor()
        try:
            cur.execute("CREATE TABLE IF NOT EXISTS {}({})".format(tableName, columns))
        except sq.OperationalError:
            log.error("Trying to create table with illegle name/s table: {} column: {}".format(tableName, columns))
            return []
        self.con.commit()
        return self.getColumnNames(tableName)

    def removeTable(self, tableName):

        if self.__checkDatabaseIsInitialised() is False:
            return False

        cur = self.con.cursor()
        try:
            cur.execute("DROP TABLE {}".format(tableName))
        except Exception as e:
            log.error('Cannot remove table: {}. Exception {}.'.format(tableName, e))
            return False
        self.con.commit()

        return True

    def getTableNames(self):

        if self.__checkDatabaseIsInitialised() is False:
            return []

        cur = self.con.cursor()
        finalList = []
        tupleList = cur.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        for l in tupleList:
            finalList.append(l[0])
        return finalList

    def getColumnNames(self, table):

        if self.__checkDatabaseIsInitialised() is False:
            return []

        cur = self.con.cursor()
        try:
            cur.execute("PRAGMA table_info({})".format(table))
        except Exception as e:
            logging.error("Exception {} when trying to get column \
                            names for table {}".format(e, table))
            return []

        fullList = cur.fetchall()
        nameList = []

        for item in fullList:
            nameList.append(item[1])

        return nameList

    def printTable(self, table):

        cur = self.con.cursor()
        try:
            for row in cur.execute("SELECT * FROM {}".format(table)):
                print(row)
        except Exception as e:
            logging.error("Exception {} when trying to print table\
                            {}".format(e, table))
            return None

    def table2Df(self, table):

        cur = self.con.cursor()
        try:
            cur.execute("SELECT * FROM {}".format(table))
        except Exception as e:
            logging.error("Exception {} when trying to return table\
                            {} as a Dataframe".format(e, table))
            return None
        df = pd.DataFrame(cur.fetchall(), columns=self.getColumnNames(table))
        return df

    # This assumes that the table contains a column named "timestamp"
    def getLastTimeEntry(self, table):

        cur = self.con.cursor()
        try:
            entry = cur.execute(
                "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(
                    table))
        except Exception as e:
            log.error('Cannot get last time entry from {}.\
                      Does the table have a timestamp column? \
                      Exception {}'.format(table, e))
            return None

        lastEntry = entry.fetchall()
        if len(lastEntry) > 0:
            return lastEntry
        else:
            lastEntry = 0
            return lastEntry

    def getRowRange(self, table, column, minimum, maximum):

        cur = self.con.cursor()
        try:
            return pd.DataFrame(cur.execute(
                "SELECT * FROM {} WHERE {} BETWEEN {} AND {}".format(
                    table,
                    column,
                    minimum,
                    maximum)).fetchall(), columns=self.getColumnNames(table))
        except Exception as e:
            log.error('Cannot query rows from {}. \
                      Requested column: {}.\
                      Availabe: {}?, excpetion {}'.format(
                          table, column, self.getColumnNames, e))
            return None

    def getRow(self, table, column, query):

        cur = self.con.cursor()
        try:
            if type(query) is str:
                return pd.DataFrame(cur.execute(
                    "SELECT * FROM {} WHERE {} LIKE '{}'".format(
                        table, column, query)).fetchall(),
                                    columns=self.getColumnNames(table))
            else:
                return pd.DataFrame(cur.execute(
                    "SELECT * FROM {} WHERE {} = {}".format(
                        table, column, query)).fetchall(),
                                    columns=self.getColumnNames(table))
        except Exception as e:
            log.error('Cannot query rows from {}. \
                      Requested column: {}. Availabe: {}?. \
                      Exception {}'.format(
                          table, column, self.getColumnNames, e))

    def getLastRows(self, table, maximum):

        cur = self.con.cursor()
        try:
            return pd.DataFrame(cur.execute(
                "SELECT * FROM {} ORDER BY timestamp DESC LIMIT {}".format(
                    table, maximum)).fetchall(), columns=self.getColumnNames(table))
        except Exception as e:
            log.error('Could not get last rows from {}. \
                      Does the table have a timestamp column? \
                      Exception {}'.format(
                          table, e))
            return None

    def removeRowRange(self, table, column, minimum, maximum):

        cur = self.con.cursor()
        try:
            cur.execute(
                "DELETE FROM {} WHERE {} BETWEEN {} AND {}".format(
                    table, column, minimum, maximum))
        except Exception as e:
            log.error('Cannot remove rows from {}. \
                      Requested column: {}. \
                      Availabe: {}?. Exception {}'.format(
                          table, column, self.getColumnNames, e))
            return None
        self.con.commit()

    # This is implemented in a silly way.
    # Definitely a better way to do this.
    def append(self, table, values):

        cur = self.con.cursor()
        if type(values) is not list:
            cur.execute("INSERT INTO {} values((?))".format(table),(values,))
        elif len(values) == 2:
            cur.execute("INSERT INTO {} values((?),(?))".format(table),(values[0],values[1],))
        elif len(values) == 3:
            cur.execute("INSERT INTO {} values((?),(?),(?))".format(table),(values[0],values[1],values[2],))
        elif len(values) == 4:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],))
        elif len(values) == 5:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],))
        elif len(values) == 6:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],))
        elif len(values) == 7:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],))
        elif len(values) == 8:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],))
        elif len(values) == 9:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],))
        elif len(values) == 10:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],))
        elif len(values) == 11:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],))
        elif len(values) == 12:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],))
        elif len(values) == 13:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],))
        elif len(values) == 14:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],))
        elif len(values) == 15:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],))
        elif len(values) == 16:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],))
        elif len(values) == 17:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],))
        elif len(values) == 18:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],))
        elif len(values) == 19:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],))
        elif len(values) == 20:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],))
        elif len(values) == 21:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],))
        elif len(values) == 22:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],))
        elif len(values) == 23:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],))
        elif len(values) == 24:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],))
        elif len(values) == 25:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],))
        elif len(values) == 26:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],))
        elif len(values) == 27:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],))
        elif len(values) == 28:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],))
        elif len(values) == 29:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],))
        elif len(values) == 30:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],))
        elif len(values) == 31:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],))
        elif len(values) == 32:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],))
        elif len(values) == 33:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],))
        elif len(values) == 34:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],))
        elif len(values) == 35:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],))
        elif len(values) == 36:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],))
        elif len(values) == 37:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],))
        elif len(values) == 38:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],))
        elif len(values) == 39:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],))
        elif len(values) == 40:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],))
        elif len(values) == 41:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],))
        elif len(values) == 42:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],))
        elif len(values) == 43:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],values[42],))
        elif len(values) == 44:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],values[42],values[43],))
        elif len(values) == 45:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],values[42],values[43],values[44],))
        elif len(values) == 46:
            cur.execute("INSERT INTO {} values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?))".format(table),(values[0],values[1],values[2],values[3],values[4],values[5],values[6],values[7],values[8],values[9],values[10],values[11],values[12],values[13],values[14],values[15],values[16],values[17],values[18],values[19],values[20],values[21],values[22],values[23],values[24],values[25],values[26],values[27],values[28],values[29],values[30],values[31],values[32],values[33],values[34],values[35],values[36],values[37],values[38],values[39],values[40],values[41],values[42],values[43],values[44],values[45],))

        self.con.commit()

