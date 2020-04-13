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

    def append(self, table, values):

        if self.__checkDatabaseIsInitialised() is False:
            return False

        if type(values) is not list:
            log.error("Trying to insert data into table {} \
                        which is not of type list. Passed type {}".format(
                            table, type(values)))
            return False

        tupleList = tuple(values)
        placeHolder = ",".join(["(?)" for i in range(len(values))])
        cur = self.con.cursor()
        try:
            cur.execute("INSERT INTO {} values({})".format(table, placeHolder), tupleList)
        except sq.OperationalError as e:
            log.error("Exception {} when inserting data into table {}. \
                      Possible data length mismatch, invalid table".format(
                e, table))
            return False
        self.con.commit()
        return True
