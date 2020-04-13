import unittest
import logging
import os
from dbops import DBOps

test_db_name = "test_db.sql"

logging.disable(logging.CRITICAL)


class DBOpsTesting(unittest.TestCase):

    def setUp(self):
        self.db = DBOps(test_db_name)
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
        db = DBOps('tests')  # Same name as tests directory
        exists = db.exists()
        self.assertIs(exists, False)

    def test_creating_database_and_test_if_exits_bad_permissions(self):
        db = DBOps('/usr/test_db.sql')
        exists = db.exists()
        self.assertIs(exists, False)

    def test_creating_database_and_test_if_exits_in_nested_directory(self):
        db = DBOps('non/existant/directory/db_name.sql')
        exists = db.exists()
        self.assertIs(exists, False)

    def test_creating_a_new_table_with_columns_no_datatypes_string_column_input_good_format(self):

        tablename = 'test_table'
        columns = "column1, column2, column3"
        self.db.createTableIfNotExist(tablename, columns)

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

    def test_creating_a_new_table_which_already_exists_doesnt_overwrite_it(self):

        # Create a table
        tablename, columns = self.test_creating_a_new_table_with_columns_no_datatypes_string_column_input_good_format()
        new_columns = "newColumn1"

        # Recreate same table with different names
        self.db.createTableIfNotExist(tablename, new_columns)

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
        self.db.createTableIfNotExist(tablename, columns)

        tableColumns = self.db.con.cursor().execute("PRAGMA table_info({})".format(tablename)).fetchall()
        self.assertEqual(len(tableColumns), 0)  # No columns should be made

    def test_try_to_make_table_if_database_is_not_setup(self):

        db = DBOps('/usr/test_db.sql')   # Bad db name
        tablename = 'test_table'
        columns = "column1, column2, column3"
        createdColumns = db.createTableIfNotExist(tablename, columns)
        self.assertEqual(len(createdColumns), 0)

    def test_create_table_with_bad_table_name(self):

        tablename = '123test.table'
        columns = "column1, column2, column3"
        createdColumns = self.db.createTableIfNotExist(tablename, columns)
        self.assertEqual(len(createdColumns), 0)

    def test_getting_table_names_if_one_table_exists(self):

        newtablename = 'test_table'
        columns = "columns1, columns2, column2"
        self.db.createTableIfNotExist(newtablename, columns)
        tableNames = self.db.getTableNames()
        self.assertEqual(tableNames[0], newtablename)

    def test_getting_table_names_if_more_than_one_table_exists(self):

        newtablename = 'test_table'
        newtablename_2 = 'test_table_2'
        tableList = [newtablename, newtablename_2]
        columns = "columns1, columns2, column2"
        self.db.createTableIfNotExist(newtablename, columns)
        self.db.createTableIfNotExist(newtablename_2, columns)
        tableNames = self.db.getTableNames()
        self.assertEqual(tableNames, tableList)

    def test_getting_table_names_if_no_tables_exist(self):

        tableNames = self.db.getTableNames()
        self.assertEqual(tableNames, [])

    def test_getting_table_names_if_no_database_exists(self):

        db = DBOps('/usr/test_db.sql')   # Bad db name
        tableNames = db.getTableNames()
        self.assertEqual(tableNames, [])

    def test_getting_columns_names_with_one_column(self):

        newtablename = 'test_table'
        columns = "columns1"
        expectedColumns = columns.split(', ')
        self.db.createTableIfNotExist(newtablename, columns)
        columnNames = self.db.getColumnNames(newtablename)
        self.assertEqual(columnNames, expectedColumns)

    def test_getting_columns_names_with_multiple_columns(self):

        newtablename = 'test_table'
        columns = "columns1, columns2, columns3"
        expectedColumns = columns.split(', ')
        self.db.createTableIfNotExist(newtablename, columns)
        columnNames = self.db.getColumnNames(newtablename)
        self.assertEqual(columnNames, expectedColumns)

    def test_getting_columns_names_with_multiple_tables(self):

        newtablename = 'test_table'
        newtablename2 = 'test_table2'
        columns = "columns1, columns2, columns3"
        expectedColumns = columns.split(', ')
        self.db.createTableIfNotExist(newtablename, columns)
        self.db.createTableIfNotExist(newtablename2, columns)
        columnNames = self.db.getColumnNames(newtablename2)
        self.assertEqual(columnNames, expectedColumns)

    def test_getting_columns_names_table_name_which_doesnt_exist(self):

        newtablename = 'test_table'
        columns = "columns1"
        self.db.createTableIfNotExist(newtablename, columns)
        columnNames = self.db.getColumnNames("not_a_table_name")
        self.assertEqual([], columnNames)

    def test_getting_columns_names_table_name_which_is_illeagle(self):

        newtablename = 'test_table'
        columns = "columns1"
        self.db.createTableIfNotExist(newtablename, columns)
        columnNames = self.db.getColumnNames("re.sd-s")
        self.assertEqual([], columnNames)

    def test_getting_columns_names_with_no_database_created(self):

        db = DBOps('/usr/test_db.sql')   # Bad db name
        columns = db.getColumnNames('some_table')
        self.assertEqual(columns, [])

    def test_removing_a_table_which_was_created_actually_removes_it(self):

        newtablename = 'test_table'
        columns = "columns1, columns2"
        self.db.createTableIfNotExist(newtablename, columns)
        removed = self.db.removeTable(newtablename)
        self.assertIs(removed, True)
        self.assertEqual([], self.db.getTableNames())

    def test_removing_a_table_which_was_not_created_has_no_effect(self):

        newtablename = 'test_table'
        columns = "columns1, columns2"
        self.db.createTableIfNotExist(newtablename, columns)
        removed = self.db.removeTable("not_a_table_name")
        self.assertIs(removed, False)
        self.assertEqual([newtablename], self.db.getTableNames())

    def test_removing_a_table_with_no_database_created_does_nothing(self):

        db = DBOps('/usr/test_db.sql')   # Bad db name
        removed = db.removeTable('some_name')
        self.assertIs(removed, False)

    def test_inserting_row_with_multiple_columns_passed_data_is_list(self):
        newtablename = 'test_table'
        columns = "timestamp, value"
        self.db.createTableIfNotExist(newtablename, columns)
        data = [123456, 43.3]

        inserted = self.db.append(newtablename, data)
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

        self.db.createTableIfNotExist(newtablename, columns)

        inserted = self.db.append(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, True)
        self.assertEqual(tuple(data), insertedData)

    def test_inserting_row_with_one_columns_passed_data_is_list(self):
        newtablename = 'test_table'
        columns = "timestamp"
        self.db.createTableIfNotExist(newtablename, columns)
        data = [123456]

        inserted = self.db.append(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, True)
        self.assertEqual(tuple(data), insertedData)

    def test_inserting_row_with_one_columns_passed_data_is_one_column_type_is_not_list(self):
        newtablename = 'test_table'
        columns = "timestamp"
        self.db.createTableIfNotExist(newtablename, columns)
        data = 123456

        inserted = self.db.append(newtablename, data)
        insertedData = self.db.con.cursor().execute(
            "SELECT * FROM {} ORDER BY timestamp DESC LIMIT 1".format(newtablename)).fetchone()

        self.assertIs(inserted, False)

    def test_inserting_data_of_length_which_doesnt_match_number_of_columns_in_table(self):
        newtablename = 'test_table'
        columns = "timestamp,value"
        self.db.createTableIfNotExist(newtablename, columns)
        data = [123456]

        inserted = self.db.append(newtablename, data)
        self.assertIs(inserted, False)

    def test_inserting_data_into_table_which_hasnt_been_created(self):

        data = [123456]
        inserted = self.db.append('invalid_table', data)
        self.assertIs(inserted, False)

    def test_appending_data_when_database_not_created(self):

        newtablename = 'notImportant'
        db = DBOps('/usr/test_db.sql')   # Bad db name
        data = [123456]
        inserted = db.append(newtablename, data)
        self.assertIs(inserted, False)

if __name__ == '__main__':
    unittest.main()
