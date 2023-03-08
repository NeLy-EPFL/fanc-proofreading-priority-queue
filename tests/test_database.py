import unittest
import tempfile
import sqlite3
import pandas as pd
from pathlib import Path

import ysp_bot
import ysp_bot.util


class DatabaseTest(unittest.TestCase):
    temp_db_path = Path(tempfile.gettempdir()) / 'ysp_bot_test.db'
    
    def test_db_op(self):
        """Note: this test should probably be split up into multiple tests.
        That is a bit complicated though, since there is a dependence in
        creating a test db, then adding stuff to it, then reading from it..."""
        
        if self.temp_db_path.is_file():
            self.temp_db_path.unlink()
        db = ysp_bot.ProofreadingDatabase(self.temp_db_path)

        # Test user skiplist: write
        db.add_to_user_skiplist('test_user', 987654321)
        # ...                 read
        user_skiplist = db.get_user_skiplist('test_user')
        self.assertEqual(len(user_skiplist), 1)
        
        # Test status table: write
        db.set_status(987654321, 'done', 'test_user')
        db.cur.execute('''
            SELECT * FROM status WHERE segid = 987654321;
        ''')
        status_record = db.cur.fetchall()
        self.assertEqual(len(status_record), 1)
        self.assertEqual(status_record[0][0], 987654321)
        
        # Test annotation table: write
        db.set_annotation(987654321, 'test annotation', 'test_user',
                          [100, 200, 300])
        db.cur.execute('''
            SELECT * FROM annotation WHERE segid = 987654321;
        ''')
        annotation_record = db.cur.fetchall()
        self.assertEqual(len(annotation_record), 1)
        self.assertEqual(annotation_record[0][0], 987654321)
        
        # Test get global segids to skip
        segids_to_skip = db.get_global_segids_to_skip()
        self.assertEqual(len(segids_to_skip), 1)
        self.assertTrue(987654321 in segids_to_skip)

        # Test read from pandas
        con = sqlite3.connect(self.temp_db_path)
        df = pd.read_sql_query('SELECT * FROM annotation', con)
        self.assertEqual(len(df), 1)


if __name__ == '__main__':
    unittest.main()