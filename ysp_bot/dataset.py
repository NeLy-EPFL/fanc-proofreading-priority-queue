import pandas as pd
import numpy as np
import logging
import requests
import json
from typing import Union, List, Iterable, Tuple
from nptyping import NDArray, Shape, Int
from datetime import datetime
from subprocess import run
from pathlib import Path
from fanc.lookup import segids_from_pts
# from fanc.rootID_lookup import segIDs_from_pts_service

import ysp_bot
import ysp_bot.util


config = ysp_bot.util.load_config()
cave_version = config['cave']['ref_version']
data_dir = Path(config['local']['data']).expanduser()
cave_table_lookup = config['cave']['tables']


def materialize_positions(pos: NDArray[Shape['NumPoints, 3'], Int],
                          timestamp: Union[int, datetime]
                          ) -> NDArray[Shape['NumPoints'], Int]:
    """Materialize a dataframe at a given timestamp."""
    if not isinstance(timestamp, datetime):
        timestamp = datetime.fromtimestamp(timestamp)
    # rootids = segIDs_from_pts_service(pos, return_roots=True,
    #                                   timestamp=timestamp)
    rootids = segids_from_pts(pos, return_roots=True,
                              timestamp=timestamp)
    assert rootids is not None and rootids.size == pos.shape[0]
    return rootids


def download_bc_connectivity_dump(node_url: str,
                                   edge_url: str,
                                   save_dir: Path,
                                   ) -> tuple[pd.DataFrame, pd.DataFrame]:
    for key, url in {'nodes': node_url, 'edges': edge_url}.items():
        logging.info(f'Downloading {key} file from '
                      'braincircuits.io connectivity table dump...')
        tgt_path = save_dir / f'bc_{key}.parquet'
        # if tgt_path.exists():
        #     continue
        logging.info(f'Downloading {key} dump from braincircuits...')
        run(['wget', '-nv', '-O', tgt_path, url])
    node_table = pd.read_parquet(save_dir / 'bc_nodes.parquet')
    edge_table = pd.read_parquet(save_dir / 'bc_edges.parquet')
    node_table.set_index('segment_id', inplace=True)
    node_table.fillna(0, inplace=True)
    node_table.to_parquet(save_dir / 'bc_nodes.parquet')
    assert node_table.index.is_unique, 'Node table has duplicate segment IDs'
    return node_table, edge_table


def materialize_cave_tables(mat_timestamp: int, cave_data_dir: Path = None,
                            save_dir: Path = None
                            ) -> dict[str, pd.DataFrame]:
    cave_data_dir = (Path(config['local']['data']).expanduser() /
                     'dump' / f'cave_{cave_version}')
    cave_tables = {}
    for cave_table_name in config['cave']['tables'].values():
        df = pd.read_parquet(cave_data_dir / f'{cave_table_name}.parquet')
        df[['x', 'y', 'z']] = df['pt_position'].to_list()
        logging.info(f'Materializing {cave_table_name} from CAVE...')
        df['remat_segment_id'] = materialize_positions(
            df[['x', 'y', 'z']].values, mat_timestamp
        )
        cave_tables[cave_table_name] = df
    
    if save_dir is not None:
        save_dir.mkdir(parents=True, exist_ok=True)
        for k, df in cave_tables.items():
            df.to_parquet(save_dir / f'cave_{k}.parquet')
            
    return cave_tables


def materialize_neck_connective(mat_timestamp: int, json_file: Path = None,
                                save_dir: Path = None) -> pd.DataFrame:
    if json_file is None:
        json_file = ysp_bot.ysp_dir / 'data/neck_seed_plane_75200.json'
    with open(json_file) as f:
        json_state = json.load(f)
    _dfs = []
    for side in ['LHS', 'RHS']:
        layer = [layer for layer in json_state['layers']
                 if layer['name'] == f'{side} 75200 Seeds'][0]
        points = [entry['point'] for entry in layer['annotations']]
        df = pd.DataFrame(points, columns=['x', 'y', 'z'])
        df['side'] = side[0]
        _dfs.append(df)
    df = pd.concat(_dfs)
    logging.info('Materializing neck connective table...')
    df['remat_segment_id'] = materialize_positions(
        df[['x', 'y', 'z']].values, mat_timestamp
    )
    
    if save_dir is not None:
        save_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(save_dir / 'neck_connective.parquet')
    return df

        
class FANCDataset:
    def __init__(self,
                 mat_timestamp: int,
                 version_data_dir: Path,
                 node_table: pd.DataFrame,
                 edge_table: pd.DataFrame,
                 soma_table: pd.DataFrame,
                 leg_mn_table: pd.DataFrame,
                 haltere_mn_table: pd.DataFrame,
                 wing_mn_table: pd.DataFrame,
                 neck_mn_table: pd.DataFrame,
                 nerve_bundle_table: pd.DataFrame,
                 neck_connective_table: pd.DataFrame) -> None:
        """The init function is not intended to be called directly.
        `FANCDataset.get_latest()` or `FANCDataset.from_path(data_dir)`
        should be used instead.`"""
        self.mat_timestamp = mat_timestamp
        self.version_data_dir = version_data_dir
        self.node_table = node_table
        self.edge_table = edge_table
        self.soma_table = soma_table
        self.leg_mn_table = leg_mn_table
        self.haltere_mn_table = haltere_mn_table
        self.wing_mn_table = wing_mn_table
        self.neck_mn_table = neck_mn_table
        self.nerve_bundle_table = nerve_bundle_table
        self.neck_connective_table = neck_connective_table
        self.priority_tables = {}
        if (p_tables_dir := version_data_dir / 'priority_tables').is_dir():
            for path in p_tables_dir.glob('*.parquet'):
                self.priority_tables[path.stem] = pd.read_parquet(path)
    
    
    def __str__(self) -> str:
        return (
            'FANCDataset(\n'
            f'    mat_timestep={self.mat_timestamp} '
            f'({datetime.fromtimestamp(self.mat_timestamp).isoformat()}),\n'
            f'    version_data_dir={self.version_data_dir},\n'
            f'    |V|={len(self.node_table)}, |E|={len(self.edge_table)}\n)'
        )
    
    
    def __repr__(self) -> str:
        return str(self)
    

    @classmethod
    def get_latest(cls):
        """Download the latest connectivity table dump from
        BrainCircuits and materialize the reference version of the
        CAVE tables to its materialization timestamp."""
        base_url = config['braincircuits']['base_url']
        credentials = ysp_bot.util.load_credentials()
        res = requests.get(
            f'{base_url}/circuit/graph/dump',
            headers={'Authorization': f'Bearer {credentials["braincircuits"]}',
                     'Authorization-Cave': f'Bearer {credentials["cave"]}'},
            params={'project': 'fruitfly_fanc_cave'}
        )
        if res.status_code != 200:
            raise RuntimeError(f'Failed to download connectivity table dump; '
                               f'{res.status_code}: {res.text}')
        links_dict = res.json()
        mat_timestamp = links_dict['last_materialized']
        node_url = links_dict['nodes_url']
        edge_url = links_dict['edges_url']
        
        mat_timestamp = mat_timestamp
        version_data_dir = data_dir / 'dump' / f'bc_dump_{mat_timestamp}'
        version_data_dir.mkdir(parents=True, exist_ok=True)
        
        # If everything is done, then just load the data
        if (version_data_dir / 'version_ready').is_file():
            return cls.from_path(version_data_dir)
        
        # Otherwise, download the data and materialize the cave tables
        node_table, edge_table = download_bc_connectivity_dump(
            node_url, edge_url, version_data_dir
        )
        cave_tables = materialize_cave_tables(
            mat_timestamp, save_dir=version_data_dir
        )
        connective_table = materialize_neck_connective(
            mat_timestamp, save_dir=version_data_dir
        )
        (version_data_dir / 'version_ready').touch()
        return cls(
            mat_timestamp=mat_timestamp,
            version_data_dir=version_data_dir,
            node_table=node_table,
            edge_table=edge_table,
            soma_table=cave_tables[cave_table_lookup['soma']],
            leg_mn_table=cave_tables[cave_table_lookup['leg_mn']],
            haltere_mn_table=cave_tables[cave_table_lookup['haltere_mn']],
            wing_mn_table=cave_tables[cave_table_lookup['wing_mn']],
            neck_mn_table=cave_tables[cave_table_lookup['neck_mn']],
            nerve_bundle_table=cave_tables[cave_table_lookup['nerve_bundle']],
            neck_connective_table=connective_table
        )
    

    @classmethod
    def from_path(cls, version_data_dir: Path, mat_timestamp: int = None):
        """Get a `FANCDataset` instance from a directory to which this
        version has already been downloaded and processed.

        Parameters
        ----------
        version_data_dir : Path
            Directory under which the data for this version is stored.
        mat_timestamp : int, optional
            Time of materialization. If not specified, it's inferred
            from the path.
        """
        if not (version_data_dir / 'version_ready').is_file():
            raise RuntimeError(f'FANC version under {version_data_dir} '
                               f'is not fully processed.')
        
        if mat_timestamp is None:
            mat_timestamp = int(version_data_dir.name.split('_')[-1])
        
        table_names = {
            'node_table': 'bc_nodes',
            'edge_table': 'bc_edges',
            'soma_table': f"cave_{cave_table_lookup['soma']}",
            'leg_mn_table': f"cave_{cave_table_lookup['leg_mn']}",
            'haltere_mn_table': f"cave_{cave_table_lookup['haltere_mn']}",
            'wing_mn_table': f"cave_{cave_table_lookup['wing_mn']}",
            'neck_mn_table': f"cave_{cave_table_lookup['neck_mn']}",
            'nerve_bundle_table': f"cave_{cave_table_lookup['nerve_bundle']}",
            'neck_connective_table': 'neck_connective'
        }
        tables = {k: pd.read_parquet(version_data_dir / f'{v}.parquet')
                  for k, v in table_names.items()}
        
        return cls(mat_timestamp, version_data_dir, **tables)
    
    
    def save(self, priority_tables_dir: Path = None) -> None:
        if priority_tables_dir is None:
            priority_tables_dir = self.version_data_dir / 'priority_tables'
        priority_tables_dir.mkdir(exist_ok=True)
        for k, v in self.priority_tables.items():
            v.to_parquet(priority_tables_dir / f'{k}.parquet')


    def build_orphaned_soma_table(self, synapse_count_thr: int = 10
                                  ) -> pd.DataFrame:
        logging.info('Finding orphaned somas...')
        soma_segids = self.soma_table['remat_segment_id'].unique()
        soma_segids = self.node_table.index.intersection(soma_segids)
        sel = self.node_table.loc[soma_segids]
        sel['total_synapses'] = (sel['nr_post'] + sel['nr_pre']).astype(int)
        sel = sel[sel['total_synapses'] < synapse_count_thr]
        return sel


    def build_multiple_soma_table(self) -> pd.DataFrame:
        logging.info('Finding segments with multiple somas...')
        count = self.soma_table['remat_segment_id'].value_counts()
        res = count[count > 1].to_frame().reset_index(names='segment_id')
        res.rename(columns={'remat_segment_id': 'num_somas'}, inplace=True)
        self.priority_tables['multiple_soma'] = res
        return res.set_index('segment_id')


    def build_problematic_efferent_table(self, synapse_count_thr: int = 50,
                                         mn_type: str = 'leg'
                                         ) -> pd.DataFrame:
        logging.info('Finding motor neurons with too few inputs...')
        if mn_type == 'all':
            mn_segids = np.intersect1d(
                self.nerve_bundle_table['remat_segment_id'],
                self.soma_table['remat_segment_id']
            )
            mn_segids = np.intersect1d(mn_segids, self.node_table.index)
        elif hasattr(self, f'{mn_type}_mn_table'):
            mn_segids = np.intersect1d(
                getattr(self, f'{mn_type}_mn_table')['remat_segment_id'],
                self.node_table.index
            )
        else:
            raise ValueError(f'Motor neuron type `{mn_type}` not recognized.')
        mn_nodes = self.node_table.loc[mn_segids]
        sel = mn_nodes[mn_nodes['nr_post'] < synapse_count_thr]
        sel = sel[['nr_post']].copy()
        sel['nr_post'] = sel['nr_post'].astype(int)
        return sel
    

    def build_problematic_afferent_table(self, synapse_count_thr: int = 50
                                         ) -> pd.DataFrame:
        raise NotImplementedError

    
    def build_problematic_dn_table(self, synapse_count_thr: int = 50
                                   ) -> pd.DataFrame:
        raise NotImplementedError
    

    def build_problematic_an_table(self, synapse_count_thr: int = 50
                                   ) -> pd.DataFrame:
        logging.info('Finding ascending neurons with too few inputs...')
        an_segids = np.intersect1d(
            self.neck_connective_table['remat_segment_id'],
            self.soma_table['remat_segment_id']
        )
        an_segids = np.intersect1d(an_segids, self.node_table.index)
        an_nodes = self.node_table.loc[an_segids]
        sel = an_nodes[an_nodes['nr_post'] < synapse_count_thr]
        sel = sel[['nr_post']].copy()
        sel['nr_post'] = sel[['nr_post']].astype(int)
        return sel
    
    
    def build_unbalanced_interneuron_table(
            self, min_total_synapses: int = 200,
            io_ratio_range: Tuple[float, float] = (0.1, 5.0),
            require_soma: bool = True
            ) -> pd.DataFrame:
        logging.info('Finding VNC interneurons with unbalanced '
                     'input-output ratios...')
        inter_segids = np.setdiff1d(
            self.node_table.index,
            np.concatenate([self.nerve_bundle_table['remat_segment_id'],
                            self.neck_connective_table['remat_segment_id']])
        )
        if require_soma:
            inter_segids = np.intersect1d(inter_segids,
                                          self.soma_table['remat_segment_id'])
        inter_nodes = self.node_table.loc[inter_segids]
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
