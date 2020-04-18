[![Build Status](https://travis-ci.org/stuianna/DBOps.svg?branch=master)](https://travis-ci.org/stuianna/DBOps)
![Codecov](https://img.shields.io/codecov/c/gh/stuianna/DBOps)
![GitHub](https://img.shields.io/github/license/stuianna/DBOps)

Python class helper for sqlite3 databases.

Example: Create, read and remove a table working with just dataframes.

```python
from dbops.sqhelper import SQHelper
import pandas as pd

table_name = 'temperature'
df = pd.DataFrame({"timestamp": [1587222785, 1587222786], 'celsius': [23.3, 23.9]})

db = 'myDatabase.sql3'
database = SQHelper(db)

# The dataframe column names are used for the table's column names. 
# All dataframe entries are automatically inserted.
database.create_table(table_name,df)

# Add some more entries to the database, in this case duplicates of the above entry are made.
database.insert(table_name,df)

# Read the content back into a dataframe
new_df = database.table_to_df(table_name)

# Remove the table from the database
database.remove_table(table_name);

```

Example: Create a table, add an entry and return it as a Pandas dataframe.

```python
from dbops.sqhelper import SQHelper

db = 'myDatabase.sql3'
table_name = 'temperature'
columns = {'timestamp': 'NUMERIC', 'celsius': 'REAL'}

# Create a class instance for a single database
database = SQHelper(db)

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


Use help(SQHelper) for more detailed information.

