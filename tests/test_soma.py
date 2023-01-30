import unittest

from fancpq._old_upstream_api.dump import load_latest_valid_dump
from fancpq._old_upstream_api.cave_live import get_live_synapse_count
from fancpq.prioritize.soma import find_orphaned_somas, \
                                   double_check_orphaned_somas


# class TestSoma(unittest.TestCase):
#     def test_soma(self):
#         mat_time, dump = load_latest_valid_dump()
#         orphaned_somas = find_orphaned_somas(dump['nodes'], dump['somas'])
#         self.assertGreater(len(orphaned_somas), 0)
#         sample = orphaned_somas.sample(10)
#         sample_filtered = double_check_orphaned_somas(sample)
        
        

# if __name__ == '__main__':
#     # unittest.main()
    
    
mat_time, dump = load_latest_valid_dump()
orphaned_somas = find_orphaned_somas(dump['nodes'], dump['somas'])
sample = orphaned_somas.sample(10)
sample_filtered = double_check_orphaned_somas(sample)
...