import unittest

from dms_task import *

class TestConfigFileMethods(unittest.TestCase):

    def test_read_good_file(self):
        tables = get_tables('test/testfile1.yaml')
        
        self.assertEqual(tables, ['table1'])


    def test_read_file_with_error(self):
        with self.assertRaises(Exception) as context:
            get_tables('test/testfile_with_error.yaml')

        self.assertTrue('unknown migration direction specified' in str(context.exception))


if __name__ == '__main__':
    unittest.main()
