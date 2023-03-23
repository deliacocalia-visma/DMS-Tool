import unittest

from dms_task import *

class TestConfigFileMethods(unittest.TestCase):

    def test_read_good_file(self):
        tables = get_tables('test/testfile1.yaml')
        
        self.assertEqual(tables, ['table1'])


if __name__ == '__main__':
    unittest.main()
