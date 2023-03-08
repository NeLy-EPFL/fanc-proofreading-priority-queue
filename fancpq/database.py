import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path


class ProofreadingDatabase:
    def __init__(self, db_path: Path) -> None:
        if not db_path.is_file():
            self.con = sqlite3.connect(db_path)
            self.cur = self.con.cursor()
            self.cur.execute('''
                CREATE TABLE user_skiplist (
                    user CHAR(16),
                    segid BIGINT,
                    timestamp DATETIME
                );
            ''')
            self.cur.execute('''
                CREATE TABLE status (
                    segid BIGINT,
                    status CHAR(16),
                    user CHAR(16),
                    timestamp DATETIME
                );
            ''')
            self.cur.execute('''
                CREATE TABLE annotation (
                    segid BIGINT,
                    annotation TEXT,
                    user CHAR(16),
                    timestamp DATETIME,
                    x_pos SMALLINT,
                    y_pos SMALLINT,
                    z_pos SMALLINT
                );
            ''')
        else:
            self.con = sqlite3.connect(db_path)
            self.cur = self.con.cursor()
    
    def get_user_skiplist(self, user):
        # select all segids from "user_skiplist" table where user is "user"
        self.cur.execute('''
            SELECT segid FROM user_skiplist WHERE user = ?;
        ''', (user,))
        segids = {x[0] for x in self.cur.fetchall()}
        return segids
            
    def add_to_user_skiplist(self, user, segid):
        now = datetime.now()
        self.cur.execute('''
            INSERT INTO user_skiplist VALUES (?, ?, ?);
        ''', (user, segid, now))
        self.con.commit()
    
    def get_global_segids_to_skip(self):
        # select all segids from "status" table where status is "skip" or "ok"
        self.cur.execute('''
            SELECT segid FROM status WHERE status IN ("skip", "done");
        ''')
        segids = {x[0] for x in self.cur.fetchall()}
        return segids
    
    def set_status(self, segid, status, user):
        now = datetime.now()
        self.cur.execute('''
            INSERT INTO status VALUES (?, ?, ?, ?);
        ''', (segid, status, user, now))
        self.con.commit()
    
    def set_annotation(self, segid, annotation, user, pt_pos):
        now = datetime.now()
        self.cur.execute('''
            INSERT INTO annotation VALUES (?, ?, ?, ?, ?, ?, ?);
        ''', (segid, annotation, user, now, *pt_pos))
        self.con.commit()