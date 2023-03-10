import sqlite3
import pandas as pd
from typing import Tuple, Set
from datetime import datetime
from pathlib import Path


class ProofreadingDatabaseConnector:
    def __init__(self, db_path: Path) -> None:
        if not db_path.is_file():
            self.con = sqlite3.connect(db_path)
            self.cur = self.con.cursor()
            self.cur.execute('''
                CREATE TABLE user_skiplist (
                    user CHAR(64),
                    segid BIGINT,
                    timestamp DATETIME
                );
            ''')
            self.cur.execute('''
                CREATE TABLE status (
                    segid BIGINT,
                    status CHAR(16),
                    user CHAR(64),
                    timestamp DATETIME
                );
            ''')
            self.cur.execute('''
                CREATE TABLE annotation (
                    segid BIGINT,
                    annotation TEXT,
                    user CHAR(64),
                    timestamp DATETIME,
                    x_pos SMALLINT,
                    y_pos SMALLINT,
                    z_pos SMALLINT
                );
            ''')
        self.con = sqlite3.connect(db_path)
        self.cur = self.con.cursor()
    
    def get_user_skiplist(self, user: str) -> Set:
        # select all segids from "user_skiplist" table where user is "user"
        self.cur.execute('''
            SELECT segid FROM user_skiplist WHERE user = ?;
        ''', (user,))
        segids = {x[0] for x in self.cur.fetchall()}
        return segids
            
    def add_to_user_skiplist(self, user: str, segid: int) -> None:
        now = datetime.now()
        self.cur.execute('''
            INSERT INTO user_skiplist VALUES (?, ?, ?);
        ''', (user, segid, now))
        self.con.commit()
    
    def get_global_segids_to_skip(self) -> Set:
        self.cur.execute('''
            SELECT segid FROM status
                WHERE status IN ("expired", "fixed", "noaction");
        ''')
        segids = {x[0] for x in self.cur.fetchall()}
        return segids
    
    def set_status(self, segid: int, status: str, user: str) -> None:
        now = datetime.now()
        self.cur.execute('''
            INSERT INTO status VALUES (?, ?, ?, ?);
        ''', (segid, status, user, now))
        self.con.commit()
    
    def set_annotation(self, segid: int, annotation: str, user: str,
                       pt_pos: Tuple[int, int, int]) -> None:
        now = datetime.now()
        self.cur.execute('''
            INSERT INTO annotation VALUES (?, ?, ?, ?, ?, ?, ?);
        ''', (segid, annotation, user, now, *pt_pos))
        self.con.commit()
    
    def close(self):
        self.con.close()
