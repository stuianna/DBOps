"""
SQHelper: Single class module for working with sqlite3 databases.
Author Stuart Ianna
"""

import sqlite3 as sq
import pandas as pd
import logging
import os

log = logging.getLogger(__name__)


class SQHelper():
    """Class for working with a single database

    The logging module is used to log errors and warnings.

    Typical Usage:

    # Create a class instance for a single database
    database = SQHelper('database.db')

    # Add a table to the database
    columns = {'timestamp': "NUMERIC", 'value': "REAL"}
    table_name = 'Temperature'
    database.create_table(table_name, columns)

    # Get all the tables in the database
    tables = database.get_table_namess()

    # Add an entry to the database
    temperature = {'timestamp': 1587222785, 'value': 23.2}
    database.insert(table_name, temperature)

    # Return the table as a Pandas Dataframe
    database.table_to_df(temperature)

    # Return a row based on a column query, returning all matching entries as Dataframe.
    column = 'value'
    temperature = 23.2
    df = database.get_row(table_name,column,temperature);

    Attributes:
        - con:sqlite3.Connection - Database class object
    """

    def __init__(self, db_name):
        """Create a class instance specifying the path to the database to work with.

        Success can be determined by calling object.exists()

        Parameters:
        db_name (str): Path the the sqlite3 database file.
        """
        self.__dbName = db_name
        self.con = None
        self.create_database()

    def create_database(self):
        """Create the sqlite3 database if it doesn't exist.

        The database name is that which was passed when the class object was initialised.

        Returns:
        True if the data base was created.
        False if an error occured.
        """

        try:
            self.con = sq.connect(self.__dbName)
            return True
        except Exception as e:
            log.error('Cannot connect to database {}, raised exception {}.'.format(self.__dbName, e))
            return False

    def exists(self):
        """Checks if the database exists and has been connected successfuly

        Returns:
        True if the database is correctly initialised.
        False if some error has occured and the database object is no useable.
        """
        if os.path.exists(self.__dbName) and self.con is not None:
            return True
        return False

    def __check_database_is_initialised(self):
        if self.con is None:
            logging.error("Trying to operate on a database which doesn't exist \
                          or hasn't been initialised.")
            return False
        return True

    def create_table(self, table_name, columns):
        """Add a new table to an initialised database

        If the table already exists, no action is taken.

        Parameters:
        table_name (str): The name of the table to create.
        columns (dict): PREFERED METHOD. A key value pair which specifies the name and datatype of the columns:
            columns = {"timestamp": "NUMERIC", 'value': "REAL"}
        columns (DataFrame): A Pandas dataframe containing named columns and (optionally) values:
            columns = pd.DataFrame({"timestamp": [1234567, 7654321],
                                   'value': [43.3, 53.3]})

        columns (str): A comma separated string of column names and types, e.g
            columns = "timestamp NUMERIC, value REAL"
            The columns are inserted in the order presented in the String. This form of entry
            is not recommmended to be used in conjuction with object.insert() with types of
            dataframe or dictionary, as this method sorts the columns alphabetically.

        Returns:
        On success (creation or already exists): A list of type str containing the names of each column in the database.
        On failure: An empty list.
        """

        if self.__check_database_is_initialised() is False:
            return []

        df = None
        if type(columns) is dict:
            columnString = ''
            for key in sorted(columns.keys()):
                columnString = columnString + key + ' ' + str(columns[key]) + ','
            columns = columnString.strip(',')
        elif type(columns) is pd.DataFrame:
            columnString = ', '.join(sorted(columns.columns))
            df = columns
            columns = columnString.strip(',')

        cur = self.con.cursor()
        try:
            cur.execute("CREATE TABLE IF NOT EXISTS {}({})".format(table_name, columns))
        except sq.OperationalError as e:
            log.error("Trying to create table with illegle name/s table: {} column: {}. Excpetion {}".format(
                table_name, columns, e))
            return []
        self.con.commit()

        if type(df) is pd.DataFrame:
            self.insert(table_name, df)

        return self.get_column_names(table_name)

    def remove_table(self, table_name):
        """Remove a table from an initialised database

        Parameters:
        table_name (str): The name of the table to remove.

        Returns:
        True: The table existed and was sucessfully removed.
        False: An issue occured and the nothing was done.
        """

        if self.__check_database_is_initialised() is False:
            return False

        cur = self.con.cursor()
        try:
            cur.execute("DROP TABLE {}".format(table_name))
        except Exception as e:
            log.error('Cannot remove table: {}. Exception {}.'.format(table_name, e))
            return False
        self.con.commit()

        return True

    def get_table_names(self):
        """Get a list containing all the tables which exist in the database.

        An empty list is returned if an error occured.

        Returns:
        A list of strings containing the name of each table.
        """

        if self.__check_database_is_initialised() is False:
            return []

        cur = self.con.cursor()
        finalList = []
        tupleList = cur.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        for l in tupleList:
            finalList.append(l[0])
        return finalList

    def get_column_names(self, table):
        """Get a list containing all columns which exist for a given table.

        An empty list is returned if an error occured, or the table doesn't exist.

        Returns:
        A list of strings containing the name of each column in the given table..
        """

        if self.__check_database_is_initialised() is False:
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

    def print_table(self, table):
        """Print the entire contents of a database table to stdout.

        Nothing is printed if the table doesn't exist or an error occured.

        Parameters:
        table (str): The name of the table to print.
        """

        if self.__check_database_is_initialised() is False:
            return None

        cur = self.con.cursor()
        try:
            print('\t\t'.join(self.get_column_names(table)))
            for row in cur.execute("SELECT * FROM {}".format(table)):
                print('\t\t'.join(str(x) for x in list(row)))
        except Exception as e:
            logging.error("Exception {} when trying to print table \
                            {}".format(e, table))
            return None

    def table_to_df(self, table):
        """Return the taget table as a Pandas Dataframe.

        Parameters:
        table (str): The name of the table to query.

        Returns:
        A dataframe object containing the complete table.
        None is returned if the table doesn't exist or an error occured.
        """

        if self.__check_database_is_initialised() is False:
            return None

        cur = self.con.cursor()
        try:
            cur.execute("SELECT * FROM {}".format(table))
        except Exception as e:
            logging.error("Exception {} when trying to return table\
                            {} as a Dataframe".format(e, table))
            return None
        df = pd.DataFrame(cur.fetchall(), columns=self.get_column_names(table))
        return df

    def get_last_time_entry(self, table):
        """Get the last timestamp entry from the databaase.

        ATTENTION: This function assumes the database contains a column
        named 'timestamp' of type NUMERIC, containing unix timestamps.

        Parameters:
        table (str): The name of the table to query.

        Returns:
        - The last time entry as a dictionary, containing column names as keys
        and they entry as values.
        - An empty dictionary if the table is empty.
        - None if an error occured (no table, no 'timestamp' column).
        """

        if self.__check_database_is_initialised() is False:
            return None

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
            columns = self.get_column_names(table)
            lastEntryDict = dict()
            for i, col in enumerate(columns):
                lastEntryDict[col] = lastEntry[0][i]
            return lastEntryDict
        else:
            return dict()

    def get_row_range(self, table, column, minimum, maximum):
        """Get a dataframe containing values of a table columns between two values (inclusive).

        Parameters:
        table (str): The name of the table to query.
        column (str): The name of the table's column to query.
        minimum (int,float): The minimum value (inclusive).
        maximum (int,float): The maximum value (inclusive).

        Returns:
        - A Pandas DataFrame containing the quried value on sucess.
        - An empty DataFrame if the table is empty.
        - None if an error occured (No table name, no column, invalid datatype).
        """

        if self.__check_database_is_initialised() is False:
            return None

        cur = self.con.cursor()
        try:
            return pd.DataFrame(cur.execute(
                "SELECT * FROM {} WHERE {} BETWEEN {} AND {}".format(
                    table,
                    column,
                    minimum,
                    maximum)).fetchall(), columns=self.get_column_names(table))
        except Exception as e:
            log.error('Cannot query rows from {}. \
                      Requested column: {}.\
                      Availabe: {}?, excpetion {}'.format(
                          table, column, self.get_column_names, e))
            return None

    def get_row(self, table, column, query):
        """ Get a dataframe containing entries which match a queried value

        Parameters:
        table (str): The name of the table to query.
        column (str): The name of the table's column to query.
        query (int, float, str): The value of the column to query.

        Returns:
        - A Pandas DataFrame containing all matching values on success:
        - An empty dataframe if the value wasn't found in the database.
        - None if an error occured.
        """

        if self.__check_database_is_initialised() is False:
            return None

        cur = self.con.cursor()
        try:
            if type(query) is str:
                return pd.DataFrame(cur.execute(
                    "SELECT * FROM {} WHERE {} LIKE '{}'".format(
                        table, column, query)).fetchall(),
                                    columns=self.get_column_names(table))
            else:
                return pd.DataFrame(cur.execute(
                    "SELECT * FROM {} WHERE {} = {}".format(
                        table, column, query)).fetchall(),
                                    columns=self.get_column_names(table))
        except Exception as e:
            log.error('Cannot query rows from {}. \
                      Requested column: {}. Availabe: {}?. \
                      Exception {}'.format(
                          table, column, self.get_column_names, e))
        return None

    def get_last_rows(self, table, maximum):
        """Get the last n number of entries in the database as a Pandas Dataframe.

        Parameters:
        table (str): The name of the table to query.
        maximum (int): The maximum number of entries to return. -1 can be passed
            to return all enties.

        Returns:
        - A Pandas DataFrame containing the up to the number of entries requested.
        - None if an error occured (no table, database not initialised, bad type for maximum.
        """

        if self.__check_database_is_initialised() is False:
            return None

        cur = self.con.cursor()
        try:
            return pd.DataFrame(cur.execute(
                "SELECT * FROM {} ORDER BY timestamp DESC LIMIT {}".format(
                    table, maximum)).fetchall(), columns=self.get_column_names(table))
        except Exception as e:
            log.error('Could not get last rows from {}. \
                      Does the table have a timestamp column? \
                      Exception {}'.format(
                          table, e))
        return None

    def remove_row_range(self, table, column, minimum, maximum):
        """Remove a number of rows from a database table.

        Parameters:
        table (str): The name of the table to remove rows from.
        column (str): The name of the table's column to match values to.
        minimum (int, float): The minimum value from which a matched value is removed (inclusive).
        maximum (int, float): The maximum value from which a matched value is removed (inclusive).

        Returns:
        True: No error occured. Items between the minimum and maximum values were removed.
        False: An error occured (bad table, column name. Bad type for minimum or maximum)
        """

        if self.__check_database_is_initialised() is False:
            return False

        if type(minimum) is str or type(maximum) is str:
            return False

        cur = self.con.cursor()
        try:
            cur.execute(
                "DELETE FROM {} WHERE {} BETWEEN {} AND {}".format(
                    table, column, minimum, maximum))
        except Exception as e:
            log.error('Cannot remove rows from {}. \
                      Requested column: {}. \
                      Availabe: {}?. Exception {}'.format(
                          table, column, self.get_column_names, e))
            return False
        self.con.commit()
        return True

    # For list the order must match the order when the table was created, dictionary creation is automatically sorted
    # For dictionary, the order doesn't matter, but keys must be same as columns
    # For datafram, the order doesn't matter, but column names must match database's
    def insert(self, table, values):
        """Insert values into a given table.

        IMPORTANT: If values is of type dict or dataframe, then the keys are sorted alphabetically
        before they are entered into the database. Therefore when working with these types, it
        is recommened that the table be created with the columns specified as a dictionary, as
        these are also sorted.

        Parameters:
        table (str): The name of the table to insert the values into.
        values (dict): A key value pair of columns:values to enter into the database table:
            values = {'timestamp': 12345667, 'value': 12.3}
        values (dataframe): A pandas dataframe containing the values to enter.
            values = pd.DataFrame({"timestamp": [1234567, 7654321],
                                   'value': [43.3, 53.3]})
        values (list): A list of columns values to enter. The order of the list should be the same
        order used when the table was created.
            values = [43.3, 53.3]]

        Returns:
        True: The items were sucessfully entered into the database table.
        False: An error occured
            - The keys or columns of the dataframe don't match the database table's columns.
            - The datatype of value is not a list, dict or dataframe.
            - The table doesn't exist.
            - The database is not initialised.
            - Some other error (if caught).
        """

        if self.__check_database_is_initialised() is False:
            return False

        if type(values) is list:
            insertedValues = tuple(values)
            placeHolder = ",".join(["(?)" for i in range(len(values))])
        elif type(values) is dict:
            if list(sorted(values.keys())) != self.get_column_names(table):
                return False
            insertedValues = tuple(values[x] for x in sorted(values.keys()))
            placeHolder = ",".join(["(?)" for i in range(len(values))])
        elif type(values) is pd.DataFrame:
            if sorted(values.columns) != self.get_column_names(table):
                return False
            dfDict = values.to_dict(orient='records')
            insertedValues = []
            for row in dfDict:
                insertedValues.append(tuple(row[x] for x in sorted(row.keys())))
            placeHolder = ",".join(["(?)" for i in range(len(values.columns))])
        else:
            log.error("Trying to insert data into table {} \
                        which is not of type list. Passed type {}".format(
                            table, type(values)))
            return False

        cur = self.con.cursor()
        try:
            if any(isinstance(i, tuple) for i in insertedValues):
                cur.executemany("INSERT INTO {} values({})".format(table, placeHolder), insertedValues)
            else:
                cur.execute("INSERT INTO {} values({})".format(table, placeHolder), insertedValues)
        except sq.OperationalError as e:
            log.error("Exception {} when inserting data into table {}. \
                      Possible data length mismatch, invalid table".format(
                e, table))
            return False
        self.con.commit()
        return True
