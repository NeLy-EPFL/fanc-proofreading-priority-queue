import pandas as pd

import fancpq.util
from fancpq.upstream_api.cave_live import get_live_synapse_count


config = fancpq.util.load_config()
orphaned_somas_synapse_thr = config['criteria'] \
                                   ['orphaned_soma']['max_synapse_count']


def find_orphaned_somas(nodes_df: pd.DataFrame,
                        soma_df: pd.DataFrame) -> pd.DataFrame:
    somas_that_are_nodes = nodes_df.index.intersection(soma_df['pt_root_id'])
    nodes_with_soma = nodes_df.loc[somas_that_are_nodes]
    total_synapses = nodes_with_soma['nr_pre'] + nodes_with_soma['nr_post']
    orphaned_somas = nodes_with_soma[
        total_synapses < orphaned_somas_synapse_thr
    ]
    return orphaned_somas


def double_check_orphaned_somas(potential_orphaned_somas: pd.DataFrame
                                ) -> pd.DataFrame:
    synapse_count = get_live_synapse_count(potential_orphaned_somas.index.values)
    orphaned_somas_filtered = potential_orphaned_somas.loc[synapse_count.index]
    orphaned_somas_filtered = orphaned_somas_filtered[
        (potential_orphaned_somas['nr_pre'] == synapse_count['nr_pre']) &
        (potential_orphaned_somas['nr_post'] == synapse_count['nr_post'])
    ]
    return potential_orphaned_somas