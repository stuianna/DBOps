import unittest
import logging
from dbops import DBOps


class DBOpsTesting(unittest.TestCase):

    def setUp(self):
        pass

    def teadDown(self):
        pass

    def test_creating_database_and_test_if_exits_good_parameters(self):
        self.assertIs(True, True)


if __name__ == '__main__':
    logging.disable(logging.CRITICAL)
    unittest.main()
