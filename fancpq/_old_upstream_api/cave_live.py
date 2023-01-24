import requests
import tempfile
import shutil
import pandas as pd
from datetime import datetime
from pathlib import Path
from time import time, sleep
from subprocess import run
from caveclient import CAVEclient
from typing import Sequence

import fancpq.util


config = fancpq.util.load_config()
credentials = fancpq.util.load_credentials()


def get_live_synapse_count(segment_ids: Sequence[int]) -> pd.DataFrame:
    client = CAVEclient('fanc_production_mar2021')
    client.auth.token = credentials['cave']
    df0 = client.materialize.synapse_query(pre_ids=segment_ids)
    df1 = client.materialize.synapse_query(post_ids=segment_ids)
    df = pd.concat([df0, df1])
    count_as_pre = df.groupby('pre_pt_root_id').size()
    count_as_pre.name = 'nr_pre'
    count_as_post = df.groupby('post_pt_root_id').size()
    count_as_post.name = 'nr_post'
    count = pd.concat([count_as_pre, count_as_post], axis=1).fillna(0)
    count['nr_pre'] = count['nr_pre'].astype(int)
    count['nr_post'] = count['nr_post'].astype(int)
    return count.loc[count.index.intersection(segment_ids)]