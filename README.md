[![Build Status](https://travis-ci.org/stuianna/DBOps.svg?branch=master)](https://travis-ci.org/stuianna/DBOps)
![Codecov](https://img.shields.io/codecov/c/gh/stuianna/DBOps)
![GitHub](https://img.shields.io/github/license/stuianna/DBOps)

# DBOps

Python class helper for sqlite databases.

Example: Create a table, add an entry and return it as a Pandas dataframe.

```
# Create a class instance for a single database
database = DBOps('database.db')

# Add a table to the database
database.createTableIfNotExist('my_table','column_1 NUMERIC, column_2 TEXT, column_3 TEXT')

# Get all the tables in the database
database.getTableNames()

# Add an entry to the database
database.append('my_table',['col_entry_1, col_entry_2, col_entry_3'])

# Return the table as a Pandas Dataframe
database.table2Df('my_table')

# Return a row based on a column query, returns entire matching row
database.getRow('my_table','column_1','col_entry_1');
```

Use help(DBOps) for more detailed information.

