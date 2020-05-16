[![Build Status](https://travis-ci.org/stuianna/DBOps.svg?branch=master)](https://travis-ci.org/stuianna/DBOps)
![Codecov](https://img.shields.io/codecov/c/gh/stuianna/DBOps)
![GitHub](https://img.shields.io/github/license/stuianna/DBOps)

Python class helpers for sqlite3 and InfluxDB databases.

## SQHelper (SqLite3 Databases)

Example: Create, read and remove a table working with just dataframes.

```python
from dbops.sqhelper import SQHelper
import pandas as pd

>>> table_name = 'temperature'
>>> df = pd.DataFrame({"timestamp": [1587222785, 1587222786], 'celsius': [23.3, 23.9]})

>>> db = 'myDatabase.sql3'
>>> database = SQHelper(db)

# The dataframe column names are used for the table's column names. 
# All dataframe entries are automatically inserted.
>>> database.create_table(table_name,df)
['timestamp', 'celsius']

# Add some more entries to the database, in this case duplicates of the above entry are made.
>>> database.insert(table_name,df)
True

# Read the content back into a dataframe
>>> database.table_to_df(table_name)
   celsius   timestamp
0     34.2  1587222785
1     23.3  1587222785
2     23.9  1587222786
3     23.3  1587222785
4     23.9  1587222786

# Remove the table from the database
>>> database.remove_table(table_name);
True
```

Example: Create a table, add an entry and return it as a Pandas dataframe.

```python
from dbops.sqhelper import SQHelper

>> db = 'myDatabase.sql3'
>>> table_name = 'temperature'
>>> columns = {'timestamp': 'NUMERIC', 'celsius': 'REAL'}

# Create a class instance for a single database
>>> database = SQHelper(db)

# Add a table to the database
>>> database.create_table(table_name,columns)
['timestamp', 'celsius']

# Get all the tables in the database
>>> database.get_table_names()
['temperature']

# Add an entry to the database
>>> new_entry = {'timestamp': 1587222785, 'celsius': 34.2}
>>> database.insert(table_name, new_entry)
True

# Return the table as a Pandas Dataframe
df = database.table_to_df(table_name)

# Return all rows based on a column query, returns matching rows as dataframe
>>> database.get_row(table_name, 'celsius', 34.2);
   celsius   timestamp
0     34.2  1587222785
```

Use help(SQHelper) for more detailed information.

## InfluxDB
```python
from dbops.imfluxhelper import InfluxHelper

# Create a class instance for a single database
>>> database = InfluxHelper('database_name')

# Check the database is connected to OK
>>> database.exists()
True

# Add a new measurement to the database as a dictionary
>>> data = {'timestamp': 1585848415, 'temperature': 23.3, 'humidity': 12.2, 'room': 'kitchen', 'house': 'home'}
>>> measurement = 'Environment'
>>> fields = ['temperature', 'humidity']
>>> tags = ['room', 'house']
>>> database.insert(measurement, data, field_keys=fields, tag_keys=tags, use_timestamp=True)
True

# Add multiple measurements as a Pandas DataFrame
>>> data = [{
    'timestamp': 1585848415,
    'temperature': 23.3,
    'humidity': 12.2,
    'room': 'kitchen',
    'house': 'home'
}, {
    'timestamp': 1585848416,
    'temperature': 22.1,
    'humidity': 13.4,
    'room': 'bedroom',
    'house': 'home'
}]
>>> df = pd.DataFrame(data)
>>> measurement = 'Household'
>>> fields = ['temperature', 'humidity']
>>> tags = ['room', 'house']
>>> database.insert(measurement, df, field_keys=fields, tag_keys=tags, use_timestamp=use_time)
True

# Get all the measurements in the database
>>> database.get_measurement_names()
['Environment', 'Household']

# Get the last time based entry in a table
>>> last_time_entry = database.get_last_time_entry('Household', 'humidity', 'room', 'bedroom', as_unix=True)
>>> last_time_entry['last']
13.4
>>> last_time_entry['time']
1585848416
```

Use help(InfluxHelper) for more detailed information.

## Version History
**0.1.0**:
- Added interface for Influx Databases
- Added some timestamp conversion utilities
- Improved documentation
