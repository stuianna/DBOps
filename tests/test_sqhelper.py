import unittest
import logging
import os
import sys
from io import StringIO
from dbops.sqhelper import SQHelper
import pandas as pd

test_db_name = "test_db.sql"

logging.disable(logging.CRITICAL)


class SQHelperTesting(unittest.TestCase):

    def setUp(self):
        self.db = SQHelper(test_db_name)
        pass

    def tearDown(self):
        try:
            os.remove(test_db_name)
        except Exception as e:
            print("No database file was removed, exception {}".format(e))

    def test_creating_database_and_test_if_exits_good_name(self):
        exists = self.db.exists()
        self.assertIs(exists, True)

    def test_creating_database_and_test_if_exits_bad_name_database_already_exists(self):
        db = SQHelper('tests')  # Same name as tests directory
        exists = db.exists()
        self.assertIs(exists, False)

    def test_creating_database_and_test_if_exits_bad_permissions(self):
        db = SQHelper('/usr/test_db.sql')
        exists = db.exists()
        self.assertIs(exists, False)

    def test_creating_database_and_test_if_exits_in_nested_directory(self):
        db = SQHelper('non/existant/directory/db_name.sql')
        exists = db.exists()
        self.assertIs(exists, False)

    def test_creating_a_new_table_with_columns_no_datatypes_string_column_input_good_format(self):

        tablename = 'test_table'
        columns = "column1, column2, column3"
        self.db.create_table(tablename, columns)

# Check for correct table name
        tableTuples = self.db.con.cursor().execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        self.assertEqual(len(tableTuples), 1)
        self.assertEqual(tableTuples[0][0], tablename)

        # Check for correct column names in table
        tableColumns = self.db.con.cursor().execute("PRAGMA table_info({})".format(tablename)).fetchall()
        expectedColumns = columns.split(', ')
        self.assertEqual(len(tableColumns), len(expectedColumns))
        for i in range(len(expectedColumns)):
            self.assertEqual(tableColumns[i][1], expectedColumns[i])

        return tablename, columns

    def test_creating_a_new_table_with_columns_with_dictionary_well_formed(self):

        tablename = 'test_table'
        columns = {'timestamp': 'INTEGER', 'value': 'REAL', 'comment': 'TEXT'}
        self.db.create_table(tablename, columns)

        # Check for correct column names in table
        tableColumns = self.db.con.cursor().execute("PRAGMA table_info({})".format(tablename)).fetchall()
        expectedColumns = [key for key in sorted(columns.keys())]
        self.assertEqual(len(tableColumns), len(expectedColumns))
        for i in range(len(expectedColumns)):
            self.assertEqual(tableColumns[i][1], expectedColumns[i])

    def test_creating_a_new_table_which_already_exists_doesnt_overwrite_it(self):

        # Create a table
        tablename, columns = self.test_creating_a_new_table_with_columns_no_datatypes_string_column_input_good_format()
        new_columns = "newColumn1"

        # Recreate same table with different names
        self.db.create_table(tablename, new_columns)

        # Check for correct table name
        tableTuples = self.db.con.cursor().execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        self.assertEqual(len(tableTuples), 1)
        self.assertEqual(tableTuples[0][0], tablename)

# Check for correct column names in table
        tableColumns = self.db.con.cursor().execute("PRAGMA table_info({})".format(tablename)).fetchall()
        expectedColumns = columns.split(', ')
        self.assertEqual(len(tableColumns), len(expectedColumns))
        for i in range(len(expectedColumns)):
            self.assertEqual(tableColumns[i][1], expectedColumns[i])

    def test_creating_a_new_table_with_columns_no_datatypes_string_column_input_bad_format(self):

        tablename = 'test_table'
        columns = "columns1, CREATE, column2"
        self.db.create_table(tablename, columns)
        tableColumns = self.db.con.cursor().execute("PRAGMA table_info({})".format(tablename)).fetchall()
        self.assertEqual(len(tableColumns), 0)  # No columns should be made

    def test_try_to_make_table_if_database_is_not_setup(self):

        db = SQHelper('/usr/test_db.sql')   # Bad db name
        tablename = 'test_table'
        columns = "column1, column2, column3"
        createdColumns = db.create_table(tablename, columns)
        self.assertEqual(len(createdColumns), 0)

    def test_create_table_with_bad_table_name(self):

        tablename = '123test.table'
        columns = "column1, column2, column3"
        createdColumns = self.db.create_table(tablename, columns)
        self.assertEqual(len(createdColumns), 0)

    def test_creating_a_new_table_from_a_dataframe(self):

        data = pd.DataFrame({"timestamp": [123456, 1234567, 1234568, 1234569],
                            'value': [43.3, 53.3, 63.3, 83.3]})

        table_name = 'new_table'
        columns = self.db.create_table(table_name, data)
        tables = self.db.get_table_names()
        contents = self.db.table_to_df(table_name)

        self.assertEqual(tables, [table_name])
        self.assertEqual(columns, ['timestamp', 'value'])

        pd.testing.assert_frame_equal(data, contents)

    def test_try_to_create_table_from_empty_dataframe(self):

        data = pd.DataFrame()
        table_name = 'new_table'
        columns = self.db.create_table(table_name, data)
        self.assertEqual(columns, [])

    def test_getting_table_names_if_one_table_exists(self):

        newtablename = 'test_table'
        columns = "columns1, columns2, column2"
        self.db.create_table(newtablename, columns)
        tableNames = self.db.get_table_names()
        self.assertEqual(tableNames[0], newtablename)

    def test_getting_table_names_if_more_than_one_table_exists(self):

        newtablename = 'test_table'
        newtablename_2 = 'test_table_2'
        tableList = [newtablename, newtablename_2]
        columns = "columns1, columns2, column2"
        self.db.create_table(newtablename, columns)
        self.db.create_table(newtablename_2, columns)
        tableNames = self.db.get_table_names()
        self.assertEqual(tableNames, tableList)

    def test_getting_table_names_if_no_tables_exist(self):

        tableNames = self.db.get_table_names()
        self.assertEqual(tableNames, [])

    def test_getting_table_names_if_no_database_exists(self):

        db = SQHelper('/usr/test_db.sql')   # Bad db name
        tableNames = db.get_table_names()
        self.assertEqual(tableNames, [])

    def test_getting_columns_names_with_one_column(self):

        newtablename = 'test_table'
        columns = "columns1"
        expectedColumns = columns.split(', ')
        self.db.create_table(newtablename, columns)
        columnNames = self.db.get_column_names(newtablename)
        self.assertEqual(columnNames, expectedColumns)

    def test_getting_columns_names_with_multiple_columns(self):

        newtablename = 'test_table'
        columns = "columns1, columns2, columns3"
        expectedColumns = columns.split(', ')
        self.db.create_table(newtablename, columns)
        columnNames = self.db.get_column_names(newtablename)
        self.assertEqual(columnNames, expectedColumns)

    def test_getting_columns_names_with_multiple_tables(self):

        newtablename = 'test_table'
        newtablename2 = 'test_table2'
        columns = "columns1, columns2, columns3"
        expectedColumns = columns.split(', ')
        self.db.create_table(newtablename, columns)
        self.db.create_table(newtablename2, columns)
        columnNames = self.db.get_column_names(newtablename2)
        self.assertEqual(columnNames, expectedColumns)

    def test_getting_columns_names_table_name_which_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "columns1"
        self.db.create_table(newtablename, columns)
        columnNames = self.db.get_column_names("not_a_table_name")
        self.assertEqual([], columnNames)

    def test_getting_columns_names_table_name_which_is_illeagle(self):

        newtablename = 'test_table'
        columns = "columns1"
        self.db.create_table(newtablename, columns)
        columnNames = self.db.get_column_names("re.sd-s")
        self.assertEqual([], columnNames)

    def test_getting_columns_names_with_no_database_created(self):

        db = SQHelper('/usr/test_db.sql')   # Bad db name
        columns = db.get_column_names('some_table')
        self.assertEqual(columns, [])

    def test_removing_a_table_which_was_created_actually_removes_it(self):

        newtablename = 'test_table'
        columns = "columns1, columns2"
        self.db.create_table(newtablename, columns)
        removed = self.db.remove_table(newtablename)
        self.assertIs(removed, True)
        self.assertEqual([], self.db.get_table_names())

    def test_removing_a_table_which_was_not_created_has_no_effect(self):

        newtablename = 'test_table'
        columns = "columns1, columns2"
        self.db.create_table(newtablename, columns)
        removed = self.db.remove_table("not_a_table_name")
        self.assertIs(removed, False)
        self.assertEqual([newtablename], self.db.get_table_names())

    def test_removing_a_table_with_no_database_created_does_nothing(self):

        db = SQHelper('/usr/test_db.sql')   # Bad db name
        removed = db.remove_table('some_name')
        self.assertIs(removed, False)

    def test_inserting_row_with_multiple_columns_passed_data_is_list(self):
        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        data = [123456, 43.3]

        inserted = self.db.insert(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, True)
        self.assertEqual(tuple(data), insertedData)

    def test_inserting_row_with_many_columns_passed_data_is_list(self):
        newtablename = 'test_table'
        columns = ["column_{}".format(i) for i in range(100)]
        columns.append("timestamp")
        columns = ",".join(columns)
        data = [i for i in range(101)]

        self.db.create_table(newtablename, columns)

        inserted = self.db.insert(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, True)
        self.assertEqual(tuple(data), insertedData)

    def test_inserting_row_with_one_columns_passed_data_is_list(self):
        newtablename = 'test_table'
        columns = "timestamp"
        self.db.create_table(newtablename, columns)
        data = [123456]

        inserted = self.db.insert(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, True)
        self.assertEqual(tuple(data), insertedData)

    def test_inserting_row_with_one_columns_passed_data_is_dict(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER"}
        self.db.create_table(newtablename, columns)
        data = {"timestamp": 123456}

        inserted = self.db.insert(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, True)
        self.assertEqual(data['timestamp'], insertedData[0])

    def test_inserting_row_with_multiple_columns_passed_data_is_dict(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER", 'value': 'REAL'}
        self.db.create_table(newtablename, columns)
        data = {"timestamp": 123456, 'value': 12.2}

        inserted = self.db.insert(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, True)
        self.assertEqual(data['timestamp'], insertedData[0])
        self.assertEqual(data['value'], insertedData[1])

    def test_inserting_row_with_multiple_columns_passed_data_is_dict_out_of_order(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER", 'value': 'REAL'}
        self.db.create_table(newtablename, columns)
        data = {"value": 12.2, "timestamp": 123456}

        inserted = self.db.insert(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, True)
        self.assertEqual(data['timestamp'], insertedData[0])
        self.assertEqual(data['value'], insertedData[1])

    def test_inserting_row_with_multiple_columns_passed_data_is_dict_bad_key_for_column(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER", 'value': 'REAL'}
        self.db.create_table(newtablename, columns)
        data = {"timestamp": 123456, 'badKey': 12.2}

        inserted = self.db.insert(newtablename, data)
        self.assertIs(inserted, False)

    def test_inserting_row_with_multiple_columns_passed_data_is_dict_of_wrong_length(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER"}
        self.db.create_table(newtablename, columns)
        data = {"timestamp": 123456, 'badKey': 12.2}

        inserted = self.db.insert(newtablename, data)
        self.assertIs(inserted, False)

    def test_inserting_row_with_multiple_columns_passed_data_is_dataframe(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER", 'value': "REAL"}
        self.db.create_table(newtablename, columns)

        data = pd.DataFrame({"timestamp": [123456, 1234567, 1234568, 1234569],
                            'value': [43.3, 53.3, 63.3, 83.3]})

        inserted = self.db.insert(newtablename, data)
        read = self.db.table_to_df(newtablename)

        self.assertIs(inserted, True)
        pd.testing.assert_frame_equal(data, read)

    def test_inserting_row_with_multiple_columns_passed_data_is_dataframe_out_of_order(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER", 'value': "REAL"}
        self.db.create_table(newtablename, columns)

        data = pd.DataFrame({"value": [123456, 1234567, 1234568, 1234569],
                            'real': [43.3, 53.3, 63.3, 83.3]})

        inserted = self.db.insert(newtablename, data)

        self.assertIs(inserted, False)

    def test_inserting_row_with_multiple_columns_passed_data_is_dataframe_with_one_columns(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER"}
        self.db.create_table(newtablename, columns)

        data = pd.DataFrame({"timestamp": [123456, 1234567, 1234568, 1234569]})

        inserted = self.db.insert(newtablename, data)
        read = self.db.table_to_df(newtablename)

        self.assertIs(inserted, True)
        pd.testing.assert_frame_equal(data, read)

    def test_inserting_row_with_multiple_columns_passed_data_is_dataframe_with_wrong_columns(self):
        newtablename = 'test_table'
        columns = {"timestamp": "INTEGER", 'value': "REAL"}
        self.db.create_table(newtablename, columns)

        data = pd.DataFrame({"timestamp": [123456, 1234567, 1234568, 1234569],
                            'wrong_column': [43.3, 53.3, 63.3, 83.3]})

        inserted = self.db.insert(newtablename, data)
        self.assertIs(inserted, False)

    def test_inserting_row_with_one_columns_passed_data_is_one_column_type_is_not_list(self):
        newtablename = 'test_table'
        columns = "timestamp"
        self.db.create_table(newtablename, columns)
        data = 123456

        inserted = self.db.insert(newtablename, data)

        self.assertIs(inserted, False)

    def test_inserting_data_of_length_which_doesnt_match_number_of_columns_in_table(self):
        newtablename = 'test_table'
        columns = "timestamp,value"
        self.db.create_table(newtablename, columns)
        data = [123456]

        inserted = self.db.insert(newtablename, data)
        self.assertIs(inserted, False)

    def test_inserting_data_into_table_which_hasnt_been_created(self):

        data = [123456]
        inserted = self.db.insert('invalid_table', data)
        self.assertIs(inserted, False)

    def test_appending_data_when_database_not_created(self):

        newtablename = 'notImportant'
        db = SQHelper('/usr/test_db.sql')   # Bad db name
        data = [123456]
        inserted = db.insert(newtablename, data)
        self.assertIs(inserted, False)

    def test_print_a_table_with_a_signle_entry(self):
        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        data = [123456, 43.3]
        self.db.insert(newtablename, data)

        old_stdout = sys.stdout
        sys.stdout = printOutput = StringIO()
        self.db.print_table(newtablename)
        sys.stdout = old_stdout

        expectedOutput = ('\t\t'.join(self.db.get_column_names(newtablename))) + '\n'
        expectedOutput = expectedOutput + ('\t\t'.join(str(x) for x in data))
        self.assertEqual(expectedOutput, printOutput.getvalue().strip('\n'))

    def test_print_a_table_with_multiple_entries(self):
        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        self.db.insert(newtablename, data)

        old_stdout = sys.stdout
        sys.stdout = printOutput = StringIO()
        self.db.print_table(newtablename)
        sys.stdout = old_stdout

        expectedOutput = ('\t\t'.join(self.db.get_column_names(newtablename))) + '\n'
        expectedOutput = expectedOutput + ('\t\t'.join(str(x) for x in data)) + '\n'
        expectedOutput = expectedOutput + ('\t\t'.join(str(x) for x in data))
        self.assertEqual(expectedOutput, printOutput.getvalue().strip('\n'))

    def test_print_table_with_no_entries(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        old_stdout = sys.stdout
        sys.stdout = printOutput = StringIO()
        self.db.print_table(newtablename)
        sys.stdout = old_stdout

        expectedOutput = ('\t\t'.join(self.db.get_column_names(newtablename)))
        self.assertEqual(expectedOutput, printOutput.getvalue().strip('\n'))

    def test_print_table_with_no_database(self):

        newtablename = 'notImportant'
        db = SQHelper('/usr/test_db.sql')   # Bad db name

        old_stdout = sys.stdout
        sys.stdout = printOutput = StringIO()
        db.print_table(newtablename)
        sys.stdout = old_stdout

        expectedOutput = ''
        self.assertEqual(expectedOutput, printOutput.getvalue().strip('\n'))

    def test_print_table_with_table_which_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        old_stdout = sys.stdout
        sys.stdout = printOutput = StringIO()
        self.db.print_table('wrong_table_name')
        sys.stdout = old_stdout

        expectedOutput = ''
        self.assertEqual(expectedOutput, printOutput.getvalue().strip('\n'))

    def test_print_table_with_illeagle_table_name(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        old_stdout = sys.stdout
        sys.stdout = printOutput = StringIO()
        self.db.print_table('bad.name-for?EXECUTE')
        sys.stdout = old_stdout

        expectedOutput = ''
        self.assertEqual(expectedOutput, printOutput.getvalue().strip('\n'))

    def test_getting_dataframe_from_table_single_entry(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        data = [123456, 43.3]
        self.db.insert(newtablename, data)

        expectedDF = pd.DataFrame({'timestamp': [123456], 'value': [43.3]})
        df = self.db.table_to_df(newtablename)

        pd.testing.assert_frame_equal(expectedDF, df)

    def test_getting_dataframe_from_table_multiple_entries(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        self.db.insert(newtablename, data)

        expectedDF = pd.DataFrame({'timestamp': [123456, 123456], 'value': [43.3, 43.3]})
        df = self.db.table_to_df(newtablename)

        pd.testing.assert_frame_equal(expectedDF, df)

    def test_getting_dataframe_from_table_no_entries(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        df = self.db.table_to_df(newtablename)

        self.assertEqual(len(df), 0)

    def test_getting_dataframe_from_table_bad_table_name(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        data = [123456, 43.3]
        self.db.insert(newtablename, data)

        df = self.db.table_to_df('bad_table_name')

        self.assertIs(df, None)

    def test_getting_dataframe_from_table_no_database_made(self):

        newtablename = 'notImportant'
        db = SQHelper('/usr/test_db.sql')   # Bad db name

        df = db.table_to_df(newtablename)

        self.assertIs(df, None)

    def test_getting_last_timestamp_entry_added_in_order(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        data = [1234567, 53.3]
        self.db.insert(newtablename, data)
        lastEntry = [1234568, 63.3]
        self.db.insert(newtablename, lastEntry)

        lastStoredEntry = self.db.get_last_time_entry(newtablename)
        expectedEntry = {'timestamp': lastEntry[0], 'value': lastEntry[1]}
        self.assertEqual(expectedEntry, lastStoredEntry)

    def test_getting_last_timestamp_entry_added_out_of_order(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        lastEntry = [1234568, 63.3]
        self.db.insert(newtablename, lastEntry)
        data = [1234567, 53.3]
        self.db.insert(newtablename, data)

        lastStoredEntry = self.db.get_last_time_entry(newtablename)
        expectedEntry = {'timestamp': lastEntry[0], 'value': lastEntry[1]}
        self.assertEqual(expectedEntry, lastStoredEntry)

    def test_getting_last_timestamp_entry_from_empty_table(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        lastStoredEntry = self.db.get_last_time_entry(newtablename)
        expectedEntry = dict()
        self.assertEqual(lastStoredEntry, expectedEntry)

    def test_getting_last_timestamp_entry_from_non_existant_table(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [123456, 43.3]
        self.db.insert(newtablename, data)

        lastStoredEntry = self.db.get_last_time_entry("bad_table_name")
        expectedEntry = None
        self.assertEqual(lastStoredEntry, expectedEntry)

    def test_getting_last_timestamp_entry_from_database_which_is_not_created(self):

        db = SQHelper('/usr/test_db.sql')   # Bad db name

        lastStoredEntry = db.get_last_time_entry("bad_table_name")
        expectedEntry = None
        self.assertEqual(lastStoredEntry, expectedEntry)

    def test_get_range_of_rows_well_formed_request_within_database_limits(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row_range(newtablename, 'value', 50, 70)
        expectedDF = pd.DataFrame({"timestamp": [1234567, 1234568], 'value': [53.3, 63.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_range_of_rows_well_formed_request_equal_to_boundries(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row_range(newtablename, 'value', 53.3, 63.3)
        expectedDF = pd.DataFrame({"timestamp": [1234567, 1234568], 'value': [53.3, 63.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_range_of_rows_well_formed_request_min_and_max_includes_all_database(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row_range(newtablename, 'value', 13.3, 163.3)
        expectedDF = pd.DataFrame({"timestamp": [123456, 1234567, 1234568, 1234568],
                                   'value': [43.3, 53.3, 63.3, 83.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_range_of_rows_well_formed_request_min_and_max_duplicate_values(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        data = [1234567, 53.3]
        self.db.insert(newtablename, data)
        data = [1234568, 63.3]
        self.db.insert(newtablename, data)
        data = [1234568, 63.3]
        self.db.insert(newtablename, data)

        df = self.db.get_row_range(newtablename, 'value', 50, 70)
        expectedDF = pd.DataFrame({"timestamp": [1234567, 1234568, 1234568],
                                   'value': [53.3, 63.3, 63.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_range_of_rows_well_formed_request_out_of_bounds(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row_range(newtablename, 'value', 13.3, 23.3)
        self.assertEqual(len(df), 0)

    def test_get_range_of_rows_well_formed_request_min_and_max_are_the_same(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row_range(newtablename, 'value', 63.3, 63.3)

        expectedDF = pd.DataFrame({"timestamp": [1234568],
                                   'value': [63.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_range_of_rows_matching_a_string(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [123456, "hello"]
        self.db.insert(newtablename, data)
        data = [1234567, "hellq"]
        self.db.insert(newtablename, data)
        data = [1234567, "hells"]
        self.db.insert(newtablename, data)

        df = self.db.get_row_range(newtablename, 'value', 'hellp', 'hellor')
        self.assertIs(df, None)

    def test_get_range_of_rows_with_bad_table_name(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row_range('bad_table_name', 'value', 12, 89)
        self.assertIs(df, None)

    def test_get_range_of_rows_with_bad_columns_name(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row_range(newtablename, 'not_a_column', 12, 89)
        self.assertIs(df, None)

    def test_get_range_of_rows_with_no_created_database(self):

        newtablename = 'notImportant'
        db = SQHelper('/usr/test_db.sql')   # Bad db name
        df = db.get_row_range(newtablename, 'value', 12, 89)
        self.assertIs(df, None)

    def test_get_row_matching_value_when_value_exists(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row(newtablename, 'value', 63.3)

        expectedDF = pd.DataFrame({"timestamp": [1234568],
                                   'value': [63.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_row_matching_value_when_value_is_duplicate(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        data = [1234567, 53.3]
        self.db.insert(newtablename, data)
        data = [1234568, 63.3]
        self.db.insert(newtablename, data)
        data = [1234568, 63.3]
        self.db.insert(newtablename, data)

        df = self.db.get_row(newtablename, 'value', 63.3)

        expectedDF = pd.DataFrame({"timestamp": [1234568, 1234568],
                                   'value': [63.3, 63.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_row_matching_value_when_value_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_row(newtablename, 'value', 3.3)

        self.assertEqual(len(df), 0)

    def test_get_row_matching_value_when_value_is_string(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [123456, '43.3']
        self.db.insert(newtablename, data)
        data = [1234567, '53.3']
        self.db.insert(newtablename, data)
        data = [1234568, '63.3']
        self.db.insert(newtablename, data)
        data = [1234568, '63.3']
        self.db.insert(newtablename, data)

        df = self.db.get_row(newtablename, 'value', '43.3')

        expectedDF = pd.DataFrame({"timestamp": [123456],
                                   'value': ['43.3']})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_row_matching_value_when_table_is_empty(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        df = self.db.get_row(newtablename, 'value', '43.3')
        self.assertEqual(len(df), 0)

    def test_get_row_matching_value_when_column_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        df = self.db.get_row(newtablename, 'doesnt_exist', '43.3')
        self.assertIs(df, None)

    def test_get_row_matching_value_when_table_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        df = self.db.get_row("doesnt_exist", 'value', '43.3')
        self.assertIs(df, None)

    def test_get_row_matching_value_when_table_doesnt_exist(self):

        db = SQHelper('/usr/test_db.sql')   # Bad db name

        df = db.get_row("doesnt_exist", 'value', '43.3')
        self.assertIs(df, None)

    def test_get_last_rows_single_request(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        data = [1234567, 53.3]
        self.db.insert(newtablename, data)
        data = [1234568, 63.3]
        self.db.insert(newtablename, data)
        data = [1234568, 83.3]
        self.db.insert(newtablename, data)

        df = self.db.get_last_rows(newtablename, 1)

        expectedDF = pd.DataFrame({"timestamp": [1234568],
                                   'value': [63.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_last_rows_multiple_request(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        data = [1234567, 53.3]
        self.db.insert(newtablename, data)
        data = [1234568, 63.3]
        self.db.insert(newtablename, data)
        data = [1234569, 83.3]
        self.db.insert(newtablename, data)

        df = self.db.get_last_rows(newtablename, 2)

        expectedDF = pd.DataFrame({"timestamp": [1234569, 1234568],
                                   'value': [83.3, 63.3]})
        pd.testing.assert_frame_equal(expectedDF, df)

    def test_get_last_rows_zero_request(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        data = [1234569, 83.3]
        self.db.insert(newtablename, data)

        df = self.db.get_last_rows(newtablename, 0)
        self.assertEqual(len(df), 0)

    def test_get_last_rows_negative_request(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_last_rows(newtablename, -1)
        self.assertEqual(len(df), 4)

    def test_get_last_rows_float_request(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_last_rows(newtablename, 1.1)
        self.assertEqual(df, None)

    def test_get_last_rows_no_timestamp_column(self):

        newtablename = 'test_table'
        columns = "not_timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_last_rows(newtablename, 1)
        self.assertEqual(df, None)

    def test_get_last_rows_table_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)

        self.insertFourUniqueEntries(newtablename)

        df = self.db.get_last_rows('not_a_table', 1)
        self.assertEqual(df, None)

    def test_get_last_rows_database_doesnt_exist(self):

        db = SQHelper('/usr/test_db.sql')   # Bad db name
        self.insertFourUniqueEntries('not_important')
        df = db.get_last_rows('not_important', 1)
        self.assertEqual(df, None)

    def test_remove_row_range_in_middle(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        self.insertFourUniqueEntries(newtablename)
        removed = self.db.remove_row_range(newtablename, 'value', 44.3, 73.3)

        expectedDF = pd.DataFrame({"timestamp": [123456, 1234569],
                                   'value': [43.3, 83.3]})
        df = self.db.table_to_df(newtablename)
        pd.testing.assert_frame_equal(expectedDF, df)
        self.assertEqual(removed, True)

    def test_remove_row_range_in_covering_all_entries(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        self.insertFourUniqueEntries(newtablename)
        removed = self.db.remove_row_range(newtablename, 'value', 14.3, 173.3)

        df = self.db.table_to_df(newtablename)

        self.assertEqual(len(df), 0)
        self.assertEqual(removed, True)

    def test_remove_row_range_covering_no_entries(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        self.insertFourUniqueEntries(newtablename)
        removed = self.db.remove_row_range(newtablename, 'value', 114.3, 173.3)

        df = self.db.table_to_df(newtablename)
        expectedDF = pd.DataFrame({"timestamp": [123456, 1234567, 1234568, 1234569],
                                   'value': [43.3, 53.3, 63.3, 83.3]})

        pd.testing.assert_frame_equal(expectedDF, df)
        self.assertEqual(removed, True)

    def test_remove_row_range_equal_to_one_entry(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        self.insertFourUniqueEntries(newtablename)
        removed = self.db.remove_row_range(newtablename, 'value', 63.3, 63.3)

        df = self.db.table_to_df(newtablename)
        expectedDF = pd.DataFrame({"timestamp": [123456, 1234567, 1234569],
                                   'value': [43.3, 53.3, 83.3]})

        pd.testing.assert_frame_equal(expectedDF, df)
        self.assertEqual(removed, True)

    def test_remove_row_range_equal_to_one_entry(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        data = [123456, '43.3']
        self.db.insert(newtablename, data)
        removed = self.db.remove_row_range(newtablename, 'value', '23.3', '63.3')

        df = self.db.table_to_df(newtablename)
        expectedDF = pd.DataFrame({"timestamp": [123456],
                                   'value': ['43.3']})

        pd.testing.assert_frame_equal(expectedDF, df)
        self.assertEqual(removed, False)

    def test_remove_row_range_table_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        self.insertFourUniqueEntries(newtablename)
        removed = self.db.remove_row_range("Bad_table", 'value', 63.3, 63.3)

        df = self.db.table_to_df(newtablename)
        expectedDF = pd.DataFrame({"timestamp": [123456, 1234567, 1234568, 1234569],
                                   'value': [43.3, 53.3, 63.3, 83.3]})

        pd.testing.assert_frame_equal(expectedDF, df)
        self.assertEqual(removed, False)

    def test_remove_row_range_column_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.create_table(newtablename, columns)
        self.insertFourUniqueEntries(newtablename)
        removed = self.db.remove_row_range(newtablename, 'value_not_a_column', 63.3, 63.3)

        df = self.db.table_to_df(newtablename)
        expectedDF = pd.DataFrame({"timestamp": [123456, 1234567, 1234568, 1234569],
                                   'value': [43.3, 53.3, 63.3, 83.3]})

        pd.testing.assert_frame_equal(expectedDF, df)
        self.assertEqual(removed, False)

    def test_remove_row_range_database_doesnt_exist(self):

        db = SQHelper('/usr/test_db.sql')   # Bad db name
        removed = db.remove_row_range('not_important', 'value', 63.3, 63.3)
        self.assertEqual(removed, False)

    def insertFourUniqueEntries(self, newtablename):
        data = [123456, 43.3]
        self.db.insert(newtablename, data)
        data = [1234567, 53.3]
        self.db.insert(newtablename, data)
        data = [1234568, 63.3]
        self.db.insert(newtablename, data)
        data = [1234569, 83.3]
        self.db.insert(newtablename, data)


if __name__ == '__main__':
    unittest.main()
