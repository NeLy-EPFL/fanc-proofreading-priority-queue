import requests
import tempfile
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path
from time import time, sleep
from subprocess import run
from caveclient import CAVEclient
from typing import Union

import fancpq.util


config = fancpq.util.load_config()
credentials = fancpq.util.load_credentials()
base_url = config['braincircuits']['base_url']
data_dir = Path(tempfile.gettempdir()) / f'fancpq'
data_dir.mkdir(parents=True, exist_ok=True)
download_interval = config['braincircuits']['dump_interval']


def download_connectivity_table_dump() -> int:
    """Download the latest connectivity table dump from
    BrainCircuits."""
    
    # Get links to the latest connectivity table dump
    res = requests.get(
        f'{base_url}/circuit/graph/dump',
        headers={'Authorization': f'Bearer {credentials["braincircuits"]}'},
        params={'project': 'fruitfly_fanc_cave'}
    )
    if res.status_code != 200:
        raise RuntimeError(f'Failed to download connectivity table dump; '
                           f'{res.status_code}: {res.text}')
    links_dict = res.json()
    mat_timestamp = links_dict['last_materialized']
    nodes_urls = links_dict['nodes_url']
    edges_urls = links_dict['edges_url']
    print(links_dict)
    
    # Download the dump
    dump_dir = data_dir / f'dump_{mat_timestamp}'
    if dump_dir.is_dir() and (dump_dir / 'connectivity_table_ok').is_file():
        return mat_timestamp
    dump_dir.mkdir(parents=True, exist_ok=True)
    for key, url in {'nodes': nodes_urls, 'edges': edges_urls}.items():
        print(f'downloading {key} file...')
        tgt_path = dump_dir / f'bc_{key}.parquet'
        run(['wget', '-O', tgt_path, url])
    (dump_dir / 'connectivity_table_ok').touch()
    
    return mat_timestamp


def download_soma_table_dump(mat_timestamp: int = None):
    """
    Download the soma table dump from CAVE.
    """
    dump_dir = data_dir / f'dump_{mat_timestamp}'
    if (dump_dir / 'soma_table_ok').is_file():
        return
    client = CAVEclient('fanc_production_mar2021')
    client.auth.token = credentials['cave']
    soma_table_name = client.info.get_datastack_info()['soma_table']
    df = _download_cave_table(soma_table_name)
    df.to_parquet(dump_dir / 'soma.parquet')
    (dump_dir / 'soma_table_ok').touch()


def _download_cave_table(table_name: str, mat_timestamp: int = None
                         ) -> pd.DataFrame:
    """Download a table from CAVE. Works for all except the synapse
    table."""
    if mat_timestamp:
        timestamp = datetime.fromtimestamp(mat_timestamp)
    else:
        timestamp = datetime.now()
    client = CAVEclient('fanc_production_mar2021')
    client.auth.token = credentials['cave']
    return client.materialize.live_query(table_name, timestamp)


def get_new_dump():
    # Check latest dump
    base_dir = Path(tempfile.gettempdir()) / 'fancpq'
    all_dump_dirs = sorted(base_dir.glob('dump_*'),
                           key=lambda x: int(x.name.replace('dump_', '')),
                           reverse=True)
    all_dump_dirs = {int(path.name.replace('dump_', '')): path
                     for path in all_dump_dirs}
    valid_dump_dirs = {k: v for k, v in all_dump_dirs.items()
                       if (v / 'connectivity_table_ok').is_file() and
                          (v / 'soma_table_ok').is_file()}
    if not valid_dump_dirs:
        latest_mat_time = None
    else:
        latest_mat_time = list(valid_dump_dirs.keys())[0]
    
    # Get a new dump if suited
    if not latest_mat_time or latest_mat_time + download_interval < time():
        print('Downloading new connectivity tables...')
        mat_time = download_connectivity_table_dump()
        if mat_time != latest_mat_time:
            print('Downloading new soma table...')
            download_soma_table_dump(mat_time)
        # Remove expired dumps
        for path in all_dump_dirs.values():
            if int(path.name.replace('dump_', '')) != mat_time:
                for f in ('connectivity_table_ok', 'soma_table_ok'):
                    try:
                        (path / f).unlink()
                    except FileNotFoundError:
                        pass
                shutil.rmtree(path)


def find_latest_valid_dump() -> tuple[int, Path]:
    base_dir = Path(tempfile.gettempdir()) / 'fancpq'
    if not base_dir.is_dir():
        return None, None
    all_dump_dirs = sorted(base_dir.glob('dump_*'),
                           key=lambda x: int(x.name.replace('dump_', '')),
                           reverse=True)
    all_dump_dirs = {int(path.name.replace('dump_', '')): path
                     for path in all_dump_dirs}
    valid_dump_dirs = {k: v for k, v in all_dump_dirs.items()
                       if (v / 'connectivity_table_ok').is_file() and
                          (v / 'soma_table_ok').is_file()}
    if not valid_dump_dirs:
       return None, None
    
    return list(valid_dump_dirs.keys())[0], list(valid_dump_dirs.values())[0]


def load_latest_valid_dump() -> tuple[datetime, dict[str, pd.DataFrame]]:
    dump_time, dump_dir = find_latest_valid_dump()
    if dump_time is None:
        return None, None
    dump = {
        'nodes': pd.read_parquet(dump_dir / 'bc_nodes.parquet'),
        'edges': pd.read_parquet(dump_dir / 'bc_edges.parquet'),
        'somas': pd.read_parquet(dump_dir / 'soma.parquet')
    }
    
    # nodes and edges df has some columns that should be int but are float
    dump['nodes']['segment_id'] = dump['nodes']['segment_id'].astype(int)
    for col in ('nr_pre', 'nr_post',
                'nr_downstream_partner', 'nr_upstream_partner'):
        dump['nodes'][col] = dump['nodes'][col].fillna(0).astype(int)
    dump['nodes'] = dump['nodes'].reset_index().set_index('segment_id')
    for col in ('src', 'dst', 'count'):
        dump['edges'][col] = dump['edges'][col].astype(int)
        
    return dump_time, dump