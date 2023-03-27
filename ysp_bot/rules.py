import logging
import abc
import numpy as np
import pandas as pd
from typing import Tuple, Dict

from ysp_bot.dataset import FANCDataset


class PrioritizationRule(abc.ABC):
    @abc.abstractmethod
    def get_table(self, dataset: FANCDataset, *args, **kwargs) -> pd.DataFrame:
        """Given a `FANCDataset` object and a set of user-defined
        parameters, return a Pandas dataframe of segments that are
        selectedd for proofreading by this rule. This method is run
        once an hour when a new FANC data dump is downloaded.

        Parameters
        ----------
        dataset : FANCDataset
            The most up-to-date FANC dataset object. `See dataset.py`.
        *args, **kwargs
            You can define any additional parameters for the rule.

        Returns
        -------
        pd.DataFrame
            A Pandas dataframe with any columns you want, as long as
            they are indexed by the segment ID (integer). The columns
            will be used to generate the feed entry (message given to
            the proofreader).
        """
        pass    # implement your selection logic here
            
    @abc.abstractmethod
    def entry_to_feed(self, etr: pd.Series) -> Dict:
        """Given a single row in the table returned by `get_table`,
        convert it to a message to be given to the proofreader.

        Parameters
        ----------
        etr : pd.Series
            A row from the table. `etr.name` would be the segment ID,
            `etr['col_name']` would be the value in any column you
            defined in `get_table`. 

        Returns
        -------
        Dict
            A dictionary of this format:
            ```
            {
                'segid': etr.name,
                'type': 'your text here',  # eg. 'Orphaned soma'
                'reason': 'your text here'  # why you're proposing this segment
            }
            ```
        """
        pass    # convert a single row in the table to a feed entry


class OrphanedSoma(PrioritizationRule):
    def get_table(self, dataset: FANCDataset,
                  synapse_count_thr: int = 10
                  ) -> pd.DataFrame:
        soma_segids = dataset.soma_table['remat_segment_id'].unique()
        soma_segids = dataset.node_table.index.intersection(soma_segids)
        sel = dataset.node_table.loc[soma_segids]
        sel['total_synapses'] = (sel['nr_post'] + sel['nr_pre']).astype(int)
        sel = sel[sel['total_synapses'] < synapse_count_thr]
        return sel
    
    def entry_to_feed(self, etr: pd.Series) -> Dict:
        return {
            'segid': etr.name,
            'type': 'Orphaned soma',
            'reason': (
                f'This soma is only attached to  {int(etr["total_synapses"])} '
                'synapses.'
            )
        }


class MultipleSomas(PrioritizationRule):
    def get_table(self, dataset: FANCDataset) -> pd.DataFrame:
        count = dataset.soma_table['remat_segment_id'].value_counts()
        res = count[count > 1].to_frame().reset_index(names='segment_id')
        res.rename(columns={'remat_segment_id': 'num_somas'}, inplace=True)
        return res.set_index('segment_id')
    
    def entry_to_feed(self, etr: pd.Series) -> Dict:
        return {
            'segid': etr.name,
            'type': 'Multiple somas',
            'reason': (
                f'This neuron appears to have {etr["num_somas"]} somas.'
            )
        }


class ProblematicEfferent(PrioritizationRule):
    def get_table(self, dataset: FANCDataset,
                  synapse_count_thr: int = 50,
                  mn_type: str = 'leg'
                  ) -> pd.DataFrame:
        if mn_type == 'all':
            mn_segids = np.intersect1d(
                dataset.nerve_bundle_table['remat_segment_id'],
                dataset.soma_table['remat_segment_id']
            )
            mn_segids = np.intersect1d(mn_segids, dataset.node_table.index)
        elif hasattr(dataset, f'{mn_type}_mn_table'):
            mn_segids = np.intersect1d(
                getattr(dataset, f'{mn_type}_mn_table')['remat_segment_id'],
                dataset.node_table.index
            )
        else:
            raise ValueError(f'Motor neuron type `{mn_type}` not recognized.')
        mn_nodes = dataset.node_table.loc[mn_segids]
        sel = mn_nodes[mn_nodes['nr_post'] < synapse_count_thr]
        sel = sel[['nr_post']].copy()
        sel['nr_post'] = sel['nr_post'].astype(int)
        return sel

    def entry_to_feed(self, etr: pd.Series) -> Dict:
        return {
            'segid': etr.name,
            'type': 'Motor neuron',
            'reason': (
                f'This motor neuron only has '
                f'{int(etr["nr_post"])} input synapses.'
            )
        }


class ProblematicAfferent(PrioritizationRule):
    def get_table(self, dataset: FANCDataset,
                  synapse_count_thr: int = 50,
                  an_type: str = 'leg'
                  ) -> pd.DataFrame:
        raise NotImplementedError
    
    def entry_to_feed(self, etr: pd.Series) -> Dict:
        raise NotImplementedError


class ProblematicDescending(PrioritizationRule):
    def get_table(self, dataset: FANCDataset,
                  synapse_count_thr: int = 50
                  ) -> pd.DataFrame:
        raise NotImplementedError
    
    def entry_to_feed(self, etr: pd.Series) -> Dict:
        raise NotImplementedError


class ProblematicAscending(PrioritizationRule):
    def get_table(self, dataset: FANCDataset,
                  synapse_count_thr: int = 50
                  ) -> pd.DataFrame:
        an_segids = np.intersect1d(
            dataset.neck_connective_table['remat_segment_id'],
            dataset.soma_table['remat_segment_id']
        )
        an_segids = np.intersect1d(an_segids, dataset.node_table.index)
        an_nodes = dataset.node_table.loc[an_segids]
        sel = an_nodes[an_nodes['nr_post'] < synapse_count_thr]
        sel = sel[['nr_post']].copy()
        sel['nr_post'] = sel[['nr_post']].astype(int)
        return sel
    
    def entry_to_feed(self, etr: pd.Series) -> Dict:
        return {
            'segid': etr.name,
            'type': 'Ascending neuron',
            'reason': (
                f'This ascending neuron only has '
                f'{int(etr["nr_post"])} input synapses.'
            )
        }


class UnbalancedInterneuron(PrioritizationRule):
    def get_table(self, dataset: FANCDataset,
                  min_total_synapses: int = 200,
                  io_ratio_range: Tuple[float, float] = (0.1, 5.0),
                  require_soma: bool = True
                  ) -> pd.DataFrame:
        inter_segids = np.setdiff1d(
            dataset.node_table.index,
            np.concatenate([dataset.nerve_bundle_table['remat_segment_id'],
                            dataset.neck_connective_table['remat_segment_id']])
        )
        if require_soma:
            inter_segids = np.intersect1d(
                inter_segids, dataset.soma_table['remat_segment_id']
            )
        inter_nodes = dataset.node_table.loc[inter_segids]
        inter_nodes['nr_total'] = inter_nodes['nr_post'] + inter_nodes['nr_pre']
        inter_nodes = inter_nodes[inter_nodes['nr_total'] >= min_total_synapses]
        inter_nodes['io_ratio'] = inter_nodes['nr_post'] / inter_nodes['nr_pre']
        inter_nodes = inter_nodes[
            (inter_nodes['io_ratio'] < io_ratio_range[0]) |
            (inter_nodes['io_ratio'] > io_ratio_range[1])
        ].copy()
        inter_nodes['nr_post'] = inter_nodes['nr_post'].astype(int)
        inter_nodes['nr_pre'] = inter_nodes['nr_pre'].astype(int)
        return inter_nodes
    
    def entry_to_feed(self, etr: pd.Series) -> Dict:
        return {
            'segid': etr.name,
            'type': 'VNC interneuron',
            'reason': (
                f'This VNC interneuron has {int(etr["nr_post"])} '
                f'input synapses and {int(etr["nr_pre"])} output '
                f'synapses. This is quite unbalanced.'
            )
        }