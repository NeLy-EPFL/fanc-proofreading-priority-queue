import pandas as pd
from pathlib import Path
from caveclient import CAVEclient

import ysp_bot.util

config = ysp_bot.util.load_config()
credentials = ysp_bot.util.load_credentials()
base_url = config['braincircuits']['base_url']
download_interval = config['braincircuits']['dump_interval']

cave_client = CAVEclient(datastack_name=config['cave']['dataset'],
                         auth_token_key=config['cave']['dataset'])
if (cave_version := config['cave']['ref_version']) is not None:
    cave_client.materialize.version = cave_version
cave_data_dir = (Path(config['local']['data']).expanduser() / 'dump' /
                 f'cave_{cave_client.materialize.version}')
cave_data_dir.mkdir(parents=True, exist_ok=True)
for cave_table_name in config['cave']['tables'].values():
    print(f'Downloading {cave_table_name} table...')
    table_df = cave_client.materialize.query_table(cave_table_name)
    table_df.to_parquet(cave_data_dir / f'{cave_table_name}.parquet')