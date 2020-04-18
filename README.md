[![Build Status](https://travis-ci.org/stuianna/DBOps.svg?branch=master)](https://travis-ci.org/stuianna/DBOps)
![Codecov](https://img.shields.io/codecov/c/gh/stuianna/DBOps)
![GitHub](https://img.shields.io/github/license/stuianna/DBOps)

# DBOps

Python class helper for sqlite databases.

Example: Create a table, add an entry and return it as a Pandas dataframe.

```python
import dbops

table_name = 'temperature'
columns = {'timestamp': 'NUMERIC', 'celsius': 'REAL'}

# Create a class instance for a single database
database = DBOps(table_name)

# Add a table to the database
database.create_table(table_name,columns)

# Get all the tables in the database
all_tables = database.get_table_names()

# Add an entry to the database
new_entry = {'timestamp': 1587222785, 'celsius': 34.2}
database.insert(table_name, new_entry)

# Return the table as a Pandas Dataframe
df = database.table_to_df(table_name)

# Return all rows based on a column query, returns matching rows as dataframe
database.get_row(table_name, 'celsius', 34.2);
```

Use help(dbops) for more detailed information.

