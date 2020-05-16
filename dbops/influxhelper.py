"""
Influx: Single class module for working with influxdb databases.
Author Stuart Ianna
"""

from influxdb import InfluxDBClient
from dbops import timeconverter as timeconverter
import pandas as pd
import logging

log = logging.getLogger(__name__)


class InfluxHelper():
    """Class for working with a single influx database

    The logging module is used to log errors and warnings.

    NOTICE: The infuxdb must be enabled and running for this module to work.

    Typical Usage:

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

    # Get all the measurements in the database
    >>> database.get_measurement_names()
    ['Environment']

    # Get the last time based entry in a table
    >>> last_time_entry = database.get_last_time_entry('Environment', 'temperature', 'room', 'kitchen', as_unix=True)
    >>> last_time_entry['last']
    23.3
    >>> last_time_entry['time']
    1585848415

    Attributes:
        - client (influx.InfluxDBClient) - Influx DB client clas object
    """
    def __init__(self, db_name):
        """ Connect to the influxDB and create a database if it doesn't exist

        WARNING: The influxDB service must be installed and started for this module to function.
        See influxDB manual pages for installation.

        The module call 'exists' can be used to check the creation status

        Parameters:
        db_name (str): The name of the database
        """
        try:
            client = InfluxDBClient(database=db_name)
            client.create_database(db_name)
            client.switch_database(db_name)
            self.client = client
        except Exception as e:
            log.critical("Cannot create database {}, is influx service started?. Exception {}".format(db_name, e))
            self.client = None

    def __del__(self):
        if self.client is not None:
            self.client.close()

    def exists(self):
        """ Check if the database has been created and is connected to successfully.

        Returns:
        - True: Everythin is setup correctly
        - False: An error occured, the database isn't useable.
        """
        if self.client is None:
            return False
        return True

    def remove_database(self, db_name):
        """ Remove a database

        Returns:
        True: No error occured, the database was removed if it existed
        False: An error occured, possibly the database isn't setup correctly.
        """
        if self.client is not None:
            self.client.drop_database(db_name)
            return True
        return False

    def get_database_names(self):
        """ Get the names of all databases in the influxDB service

        Returns:
        databases (list): A list of all databases, this list is empty if an error occured.
        """
        if self.client is None:
            return []
        return [db['name'] for db in self.client.get_list_database()]

    def get_measurement_names(self):
        """ Get the names of all measurements in the connected database

        Returns:
        measurements (list): A list of all measurements, this list is empty if an error occured.
        """
        if self.client is None:
            return []
        return [m['name'] for m in self.client.get_list_measurements()]

    def get_last_time_entry(self, measurement, field, tag, tag_value, as_unix=False):
        """ Get the last time entry for a given query

        Parameters:
        measurement (str): The database measuement to use
        field (str): The field of measurement to query
        tag (str): The tag to filter by.
        tag_value: The tag value to use as a filter.
        as_unix: If true the time parameter is returned as a unix timestamp, if false a rfc3339 timestamp is returned.

        Returns:
        entry (dict):
            'time': The timestamp in the requested format
            'last': The value of the field at this timestamp
        """
        if self.client is None:
            return None
        lastTimeEntry = self.client.query("SELECT last({}) FROM {} WHERE {} ='{}'".format(
            field, measurement, tag, tag_value))
        dataPoints = lastTimeEntry.get_points()
        try:
            data = next(dataPoints)
            if as_unix:
                data['time'] = timeconverter.rfc3339_to_unix(data['time'])
            return data
        except StopIteration:
            return None

    def insert(self, measurement, data, field_keys, tag_keys, use_timestamp=False):
        """ Insert one or more entries into a measuremtn

        The data passed can be either a dictionary or dataframe

        Parameters:
        measurement (str): The measurement to add the data to
        data (dict, DataFrame): The data to add to the measurement
        field_keys (str): A list of key values to use for the field entries.
        These values match either the dictionry keys or the dataframe columns.
        tag_keys (str): A list of key values to use for the tag entries.
        These values match either the dictionry keys or the dataframe columns.
        use_timestamp (Boolean):
        -True: The dictionary's 'timestamp' key or dataframe's timestamp column is used in the influxDB timestamp value.
        -False: The current time is used in the influxDB timestamp value.

        Returns:
        True: No error occured, data was added to the database.
        False: Some error occured, check logs for info
        """
        if self.client is None:
            return False

        if type(data) is dict:
            return self.__insert_dict_entry(measurement, data, field_keys, tag_keys, use_timestamp)
        elif type(data) is pd.DataFrame:
            return self.__insert_dataframe_entry(measurement, data, field_keys, tag_keys, use_timestamp)
        else:
            return False

    def __insert_dataframe_entry(self, measurement, df, field_keys, tag_keys, use_timestamp):
        all_entries = []
        for index, row in df.iterrows():
            single_entry = self.__organise_single_entry(row.to_dict(), measurement, field_keys, tag_keys, use_timestamp)
            if single_entry is None:
                return False
            all_entries.append(single_entry)
        return self.client.write_points(all_entries)

    def __insert_dict_entry(self, measurement, data, field_keys, tag_keys, use_timestamp):

        new_entry = self.__organise_single_entry(data, measurement, field_keys, tag_keys, use_timestamp)
        if new_entry is None:
            return False
        try:
            return self.client.write_points([new_entry])
        except Exception as e:
            log.error("Cannot add entry for measurent {} with entry {}. Exception {}".format(measurement, new_entry, e))
            return False

    def __organise_single_entry(self, data, measurement, field_keys, tag_keys, use_timestamp):
        new_entry = dict()

        if type(field_keys) is list:
            new_entry['fields'] = {k: data[k] for k in data.keys() & field_keys}
        else:
            return None

        if type(tag_keys) is list:
            new_entry['tags'] = {k: data[k] for k in data.keys() & tag_keys}

        if use_timestamp:
            new_entry['time'] = timeconverter.unix_to_rfc3339(data['timestamp'])

        new_entry['measurement'] = measurement
        return new_entry
