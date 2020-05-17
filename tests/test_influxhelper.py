import unittest
import logging
from dbops.influxhelper import InfluxHelper
import pandas as pd

logging.disable(logging.CRITICAL)
test_db_name = 'test_db'
bad_db_name = None


class SQHelperTesting(unittest.TestCase):
    def setUp(self):
        self.db = InfluxHelper(test_db_name)

    def tearDown(self):
        self.db.remove_database(test_db_name)

    def create_bad_db(self):
        self.db = InfluxHelper(bad_db_name)

    def test_get_list_of_all_databases(self):
        db_list = self.db.get_database_names()
        self.assertIs(test_db_name in db_list, True)

    def test_get_list_of_all_databases_BADDB(self):
        self.create_bad_db()
        db_list = self.db.get_database_names()
        self.assertEqual(db_list, [])

    def test_check_database_exists_when_it_exists(self):
        self.assertEqual(self.db.exists(), True)

    def test_check_database_exists_when_it_DOESNT_exists(self):
        self.create_bad_db()
        self.assertEqual(self.db.exists(), False)

    def test_create_db_with_bad_name(self):
        self.create_bad_db()
        self.assertIs(self.db.client, None)

    def test_insert_data_which_is_single_dict_supply_tags_and_fields(self):
        success = self.insert_single_good_entry()
        last_time_entry = self.db.get_last_time_entry('Environment', 'temperature', 'room', 'kitchen')
        last_temperature = last_time_entry['last']
        self.assertEqual(last_temperature, 23.3)
        self.assertEqual(success, True)

    def test_insert_data_with_no_database_returns_false(self):
        self.create_bad_db()
        success = self.insert_single_good_entry()
        self.assertIs(success, False)

    def insert_single_good_entry(self, use_time=False):
        data = {'timestamp': 1585848415, 'temperature': 23.3, 'humidity': 12.2, 'room': 'kitchen', 'house': 'home'}
        measurement = 'Environment'
        fields = ['temperature', 'humidity']
        tags = ['room', 'house']
        return self.db.insert(measurement, data, field_keys=fields, tag_keys=tags, use_timestamp=use_time)

    def test_insert_bad_entry_bad_data_returns_false(self):
        data = {}
        measurement = 'Environment'
        fields = ['temperature', 'humidity']
        tags = ['room', 'house']
        success = self.db.insert(measurement, data, field_keys=fields, tag_keys=tags)
        self.assertIs(success, False)

    def test_insert_bad_entry_bad_fields_returns_false(self):
        data = {'timestamp': 1585848415, 'temperature': 23.3, 'humidity': 12.2, 'room': 'kitchen', 'house': 'home'}
        measurement = 'Environment'
        fields = []
        tags = ['room', 'house']
        success = self.db.insert(measurement, data, field_keys=fields, tag_keys=tags)
        self.assertIs(success, False)

    def test_insert_bad_entry_bad_field_type_returns_none(self):
        data = {'timestamp': 1585848415, 'temperature': 23.3, 'humidity': 12.2, 'room': 'kitchen', 'house': 'home'}
        measurement = 'Environment'
        fields = None
        tags = ['room', 'house']
        success = self.db.insert(measurement, data, field_keys=fields, tag_keys=tags)
        self.assertIs(success, False)

    def test_can_insert_entry_with_no_tags(self):
        data = {'timestamp': 1585848415, 'temperature': 23.3, 'humidity': 12.2, 'room': 'kitchen', 'house': 'home'}
        measurement = 'Environment'
        fields = ['temperature', 'humidity']
        tags = []
        success = self.db.insert(measurement, data, field_keys=fields, tag_keys=tags)
        self.assertIs(success, True)

    def test_insert_data_of_unsupported_type(self):
        data = 'A nice string'
        measurement = 'Environment'
        fields = ['temperature', 'humidity']
        tags = []
        success = self.db.insert(measurement, data, field_keys=fields, tag_keys=tags)
        self.assertIs(success, False)

    def test_get_last_time_entry_when_measurement_doesnt_exist_returns_none(self):
        self.insert_single_good_entry()
        last_time_entry = self.db.get_last_time_entry('Doesnt_exit', 'temperature', 'room', 'kitchen')
        self.assertEqual(last_time_entry, None)

    def test_get_last_time_entry_with_invalid_parameter_returns_none(self):
        self.insert_single_good_entry()
        last_time_entry = self.db.get_last_time_entry('Bad', 'temperature', 'room', 'kitchen')
        self.assertIs(last_time_entry, None)

    def test_get_last_time_entry_with_no_database_returns_none(self):
        self.create_bad_db()
        last_time_entry = self.db.get_last_time_entry('Environment', 'temperature', 'room', 'kitchen')
        self.assertIs(last_time_entry, None)

    def test_get_last_time_entry_with_empty_databasereturns_none(self):
        last_time_entry = self.db.get_last_time_entry('Environment', 'temperature', 'room', 'kitchen')
        self.assertIs(last_time_entry, None)

    def test_insert_data_single_dict_with_defined_timestamp_return_unix(self):
        success = self.insert_single_good_entry(use_time=True)
        last_time_entry = self.db.get_last_time_entry('Environment', 'temperature', 'room', 'kitchen', as_unix=True)
        last_time = last_time_entry['time']
        self.assertIs(success, True)
        self.assertEqual(last_time, 1585848415)

    def test_insert_data_single_dict_with_defined_timestamp_return_rfc3339(self):
        success = self.insert_single_good_entry(use_time=True)
        last_time_entry = self.db.get_last_time_entry('Environment', 'temperature', 'room', 'kitchen', as_unix=False)
        last_time = last_time_entry['time']
        self.assertIs(success, True)
        self.assertEqual(last_time, '2020-04-02T17:26:55Z')

    def test_insert_data_as_dataframe_no_timestamp(self):
        success = self.insert_dataframe_two_entries(use_time=True)
        last_entry = self.db.get_last_time_entry('Environment', 'humidity', 'room', 'bedroom', as_unix=True)
        last_time = last_entry['time']
        last_hum = last_entry['last']

        self.assertEqual(success, True)
        self.assertEqual(last_time, 1585848416)
        self.assertEqual(last_hum, 13.4)

    def insert_dataframe_two_entries(self, use_time=False):
        data = [{
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
        df = pd.DataFrame(data)
        measurement = 'Environment'
        fields = ['temperature', 'humidity']
        tags = ['room', 'house']
        return self.db.insert(measurement, df, field_keys=fields, tag_keys=tags, use_timestamp=use_time)

    def test_insert_data_bad_dataframe(self):
        data = [{'timestamp': 1585848415, 'temperature': 23.3, 'humidity': 12.2, 'room': 'kitchen', 'house': 'home'}]
        df = pd.DataFrame(data)
        measurement = 'Environment'
        fields = None
        tags = ['room', 'house']
        success = self.db.insert(measurement, df, field_keys=fields, tag_keys=tags, use_timestamp=False)
        self.assertEqual(success, False)

    def test_insert_empty_dataframe(self):
        df = pd.DataFrame()
        measurement = 'Environment'
        fields = ['temperature', 'humidity']
        tags = ['room', 'house']
        success = self.db.insert(measurement, df, field_keys=fields, tag_keys=tags, use_timestamp=False)
        self.assertEqual(success, True)

    def test_get_measurement_names(self):
        self.insert_dataframe_two_entries()
        measurements = self.db.get_measurement_names()
        self.assertEqual(measurements, ['Environment'])

    def test_get_measurement_names_no_measurement_added(self):
        measurements = self.db.get_measurement_names()
        self.assertEqual(measurements, [])

    def test_get_measurement_names_no_database(self):
        self.create_bad_db()
        measurements = self.db.get_measurement_names()
        self.assertEqual(measurements, [])

    def test_remove_measurement_no_database_object(self):
        self.insert_dataframe_two_entries()
        olddb = self.db
        self.create_bad_db()
        success = self.db.remove_measurement("Environment")
        self.assertEqual(success, False)
        self.db = olddb

    def test_remove_measurement_which_doesnt_exist(self):
        success = self.db.remove_measurement("Environment")
        self.assertEqual(success, False)

    def test_remove_measurement_which_does_exist(self):
        self.insert_dataframe_two_entries()
        success = self.db.remove_measurement("Environment")
        measurements = self.db.get_measurement_names()
        self.assertEqual("Environment" in measurements, False)
        self.assertEqual(success, True)

    def test_remove_measurement_when_passed_type_is_not_str(self):
        measurement = None
        self.insert_dataframe_two_entries()
        success = self.db.remove_measurement(measurement)
        self.assertEqual(success, False)
