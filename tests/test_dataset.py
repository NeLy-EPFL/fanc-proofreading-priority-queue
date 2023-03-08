import unittest
from pathlib import Path

import fancpq
import fancpq.util


class FANCDatasetTest(unittest.TestCase):
    def _setup_ds(self):
        config = fancpq.util.load_config()
        data_dir = Path(config['local']['data']).expanduser()
        ds = fancpq.FANCDataset.from_path(data_dir / 'dump' /
                                          'bc_dump_1678035603')
        return ds
    
    def test_load_ds(self):
        self._setup_ds()
    
    def test_build_orphaned_soma_table(self):
        ds = self._setup_ds()
        orphaned_soma_table = ds.build_orphaned_soma_table()
        print('orphaned_soma_table', len(orphaned_soma_table))
    
    def test_build_multiple_soma_table(self):
        ds = self._setup_ds()
        multiple_soma_table = ds.build_multiple_soma_table()
        print('multiple_soma_table', len(multiple_soma_table))
    
    def test_build_problematic_an_table(self):
        ds = self._setup_ds()
        problematic_an_table = ds.build_problematic_an_table()
        print('problematic_an_table', len(problematic_an_table))
    
    def test_build_problematic_mn_table(self):
        ds = self._setup_ds()
        problematic_mn_table = ds.build_problematic_efferent_table()
        print('problematic_mn_table', len(problematic_mn_table))
    
    def test_build_unbalanced_in_table(self):
        ds = self._setup_ds()
        unbalanced_in_table = ds.build_unbalanced_interneuron_table()
        print('unbalanced_in_table', len(unbalanced_in_table))
    
    def test_build_all_tables(self):
        ds = self._setup_ds()
        orphaned_soma_table = ds.build_orphaned_soma_table()
        multiple_soma_table = ds.build_multiple_soma_table()
        problematic_an_table = ds.build_problematic_an_table()
        problematic_mn_table = ds.build_problematic_efferent_table()
        unbalanced_in_table = ds.build_unbalanced_interneuron_table()
        ...
        

if __name__ == '__main__':
    unittest.main()