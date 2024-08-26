# import os, requests, re
# import time

# import pickle
# import datetime, dotenv

# import subprocess
import dotenv
import pathlib
import os
import pandas as pd
import numpy as np
# from pathlib import Path

# import urllib
import json
import mysql.connector
import uuid
# # DatabaseManager


def df_nan2none(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(df.notnull(), None)


class DatabaseManager:
    def __init__(
        self,
        info_db: dict | None = None,
        tableName: str | None = None,
        path_env: str | None = None,
        keys_info: dict | None = None,
    ) -> None:
        keys_info_default = {
            "DB_USERNAME": "DB_USERNAME",
            "DB_PASSWD": "DB_PASSWD",
            "DB_ACCESS": "DB_ACCESS",
        }
        keys_info = (keys_info or {}) | keys_info_default
        self.keys_info = keys_info
        self.load_env(path_env)
        self.obtain_db_info()

        if info_db is not None:
            self.info_db = info_db
        self.tableName = tableName
        self.connect()

    def load_env(self, path_env: str) -> None:
        if path_env is not None and pathlib.Path(path_env).exists():
            dotenv.load_dotenv(path_env, override=True)

    def obtain_db_info(self) -> dict:
        keys_info = self.keys_info    
        user = os.getenv(keys_info.get("DB_USERNAME"))
        passwd = os.getenv(keys_info.get("DB_PASSWD"))
        self.info_db = json.loads(os.getenv(keys_info.get("DB_ACCESS"))) | {
            "user": user,
            "password": passwd,
        }
        return self.info_db

    def connect(
        self, info_db: dict | None = None
    ) -> tuple[mysql.connector.MySQLConnection, mysql.connector.cursor.MySQLCursor]:
        if info_db is None:
            info_db = {}
        info_db_valid = {**self.info_db, **info_db}
        self.info_db = info_db_valid
        db = mysql.connector.connect(**info_db_valid)
        cursor = db.cursor()
        self.db = db
        self.cursor = cursor
        return db, cursor
    
    def close(self) -> None:
        self.cursor.close()
        self.db.close()

    def reconnect(self) -> mysql.connector.cursor.MySQLCursor:
        db = self.db
        cursor = db.cursor()
        self.cursor = cursor
        return cursor

    def obtain_colInfo(self, tableName) -> pd.DataFrame:
        self.connect()
        cursor = self.cursor
        columns_forDf = ["COLUMN_NAME", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH"]
        sql_cmd = (
            "SELECT {} FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = {}".format(
                ", ".join(columns_forDf), "%s"
            )
        )
        params = (tableName,)
        cursor.execute(sql_cmd, params)
        arrs_for_Df = [arr for arr in cursor]
        df_col = df_nan2none(pd.DataFrame(arrs_for_Df, columns=columns_forDf))
        self.df_col = df_col
        self.close()
        return df_col
    
    def obtain_tableContent(self,
                            sql_cmd: str | None = None,
                            params = tuple([]), 
                            tableName: str | None = None) -> pd.DataFrame:
        tableName = tableName or self.tableName
        if tableName is None:
            raise ValueError("tableName is None")
        df_col = self.obtain_colInfo(tableName)
        self.connect()
        cursor = self.cursor
        if sql_cmd is None:
            sql_cmd = "SELECT * FROM {}".format(tableName)
        cursor.execute(sql_cmd, params)
        res = [s for s in cursor]
        df_obtained = pd.DataFrame(res, columns=df_col["COLUMN_NAME"])
        return df_obtained

    def executeUpsertIntoDB(
        self,
        df_items: pd.DataFrame,
        colNames: list,
        flag_idIsExist: bool,
        tableName: str,
        key_id: str,
        flag_keyAutocompleted: bool = True,
    ) -> bool:
        self.connect()
        cursor = self.cursor

        if len(df_items) == 0:
            return False
        if key_id not in df_items.columns and flag_keyAutocompleted is True:
            df_items[key_id] = [str(uuid.uuid4()) for _ in df_items.index]
            flag_idIsExist = False
        cols_valid_set = set(
            list(colNames) + [key_id]
        ) & set(  # df_col["COLUMN_NAME"].tolist()
            df_items.columns
        )
        cols_valid = list(cols_valid_set)
        df_items = df_items.drop(columns=list(set(df_items.columns) - cols_valid_set))
        if flag_idIsExist is True:
            sql_cmd = "UPDATE {} SET {} WHERE {} = %s".format(
                tableName,
                ", ".join(f"{s} = %s" for s in cols_valid if s != key_id),
                key_id,
            )
            params_s = []
            for _, row in df_items.iterrows():
                arr_params = [
                    None if pd.isna(row.get(k)) else row.get(k)
                    for k in cols_valid
                    if k != key_id
                ] + [row[key_id]]
                params_s.append(tuple(arr_params))
        else:
            sql_cmd = "INSERT INTO {} ({}) VALUES ({})".format(
                tableName,
                ", ".join(f"{s}" for s in cols_valid),
                ", ".join("%s" for _ in cols_valid),
            )
            params_s = []
            for _, row in df_items.iterrows():
                arr_params = [
                    None if pd.isna(row.get(k)) else row.get(k) for k in cols_valid
                ]
                params_s.append(tuple(arr_params))
        cursor.executemany(sql_cmd, params_s)
        print(f"{len(params_s)} rows added")
        self.db.commit()
        self.close()
        return True

    def complement_uuid4(self, df_new: pd.DataFrame, df_old: pd.DataFrame, keys_match: list[str] ,key_uuid4: str = "uuid4") -> pd.DataFrame:
        rows_new = []
        for _, row in df_new.iterrows():
            rows_old = df_old[np.all(df_old[keys_match] == row[keys_match], axis=1)]
            if len(rows_old) > 0:
                row_old = rows_old.iloc[0]
                row[key_uuid4] = row_old[key_uuid4]
                rows_new.append(row)
                continue
            row[key_uuid4] = str(uuid.uuid4())
            rows_new.append(row)
        return pd.DataFrame(rows_new)

    def splitItems(
        self,
        df_items: pd.DataFrame,
        tableName: str,
        key_id: str,
        remakeFunc: object | None = None,
    ) -> list[pd.DataFrame, pd.DataFrame]:
        self.connect()
        cursor = self.cursor
        sql_cmd = "SELECT {} FROM {}".format(key_id, tableName)
        cursor.execute(sql_cmd)
        ids_exist = [s[0] for s in cursor]
        if len(ids_exist) == 0:
            df_exist = pd.DataFrame()
            df_new = df_items
        elif key_id not in df_items.columns:
            df_items[key_id] = [str(uuid.uuid4()) for _ in df_items.index]
            df_exist = pd.DataFrame()
            df_new = df_items
        else:
            cond = df_items[key_id].astype("str").isin(ids_exist)
            df_exist = df_items[cond]
            df_new = df_items[~cond]
        if callable(remakeFunc):
            df_exist = remakeFunc(df_exist, flag_idIsExist=True)
            df_new = remakeFunc(df_new, flag_idIsExist=False)
        self.close()
        return df_exist, df_new

    def executeUpsertIntoDB_all(self, df_made, colNames, key_id = "uuid4", tableName: str | None = None):
        tableName = tableName or self.tableName
        df_exist, df_new = self.splitItems(df_made, tableName, key_id)
        for df_tmp, flag_exist in zip([df_exist, df_new], [True, False]):
            self.executeUpsertIntoDB(df_tmp, colNames, flag_exist, tableName, key_id)
        # self.db.commit()
