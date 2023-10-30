from dataclasses import dataclass
from typing import Dict, Iterable, List, Union, Any
from google.cloud import bigquery
import json


@dataclass
class Player:
    year: int
    player: str
    team: str
    pos: str
    age: int
    # STANDARD STATS
    g: int
    mp: int
    fg: int
    fga: int
    fgperc: float
    twop: int
    twopa: int
    twopperc: float
    threep: int
    threepa: int
    threepperc: float
    ft: int
    fta: int
    ftperc: float
    orb: int
    drb: int
    trb: int
    ast: int
    stl: int
    blk: int
    tov: int
    pf: int
    pts: int
    # ADVANCED STATS
    per: float
    ts: float
    ws: float
    ows: float
    dws: float
    ws48: float
    obpm: float
    dbpm: float
    bpm: float
    vorp: float

    @staticmethod
    def bq_schema_dict() -> Iterable[Dict[str, Union[str, Dict]]]:
        return json.dumps([
            {"name": "Year", "type": "INTEGER"},
            {"name": "Player", "type": "STRING"},
            {"name": "Tm", "type": "STRING"},
            {"name": "Pos", "type": "STRING"},
            {"name": "Age", "type": "INTEGER"},
            {"name": "G", "type": "INTEGER"},
            {"name": "MP", "type": "INTEGER"},
            {"name": "FG", "type": "INTEGER"},
            {"name": "FGA", "type": "INTEGER"},
            {"name": "FG%", "type": "FLOAT"},
            {"name": "2P", "type": "INTEGER"},
            {"name": "2PA", "type": "INTEGER"},
            {"name": "2P%", "type": "FLOAT"},
            {"name": "3P", "type": "INTEGER"},
            {"name": "3PA", "type": "INTEGER"},
            {"name": "3P%", "type": "FLOAT"},
            {"name": "FT", "type": "INTEGER"},
            {"name": "FTA", "type": "INTEGER"},
            {"name": "FT%", "type": "FLOAT"},
            {"name": "ORB", "type": "INTEGER"},
            {"name": "DRB", "type": "INTEGER"},
            {"name": "TRB", "type": "INTEGER"},
            {"name": "AST", "type": "INTEGER"},
            {"name": "STL", "type": "INTEGER"},
            {"name": "BLK", "type": "INTEGER"},
            {"name": "TOV", "type": "INTEGER"},
            {"name": "PF", "type": "INTEGER"},
            {"name": "PTS", "type": "INTEGER"},
            {"name": "PER", "type": "FLOAT"},
            {"name": "TS%", "type": "FLOAT"},
            {"name": "WS", "type": "FLOAT"},
            {"name": "OWS", "type": "FLOAT"},
            {"name": "DWS", "type": "FLOAT"},
            {"name": "WS48", "type": "FLOAT"},
            {"name": "OBPM", "type": "FLOAT"},
            {"name": "DBPM", "type": "FLOAT"},
            {"name": "BPM", "type": "FLOAT"},
            {"name": "VORP", "type": "FLOAT"}
        ], sort_keys=True)

    @staticmethod
    def bq_schema_field() -> Iterable[List[bigquery.SchemaField]]:
        return [
            bigquery.SchemaField("Year", "INTEGER"),
            bigquery.SchemaField("Player", "STRING"),
            bigquery.SchemaField("Tm", "STRING"),
            bigquery.SchemaField("Pos", "STRING"),
            bigquery.SchemaField("Age", "INTEGER"),
            bigquery.SchemaField("G", "INTEGER"),
            bigquery.SchemaField("MP", "INTEGER"),
            bigquery.SchemaField("FG", "INTEGER"),
            bigquery.SchemaField("FGA", "INTEGER"),
            bigquery.SchemaField("FG%", "FLOAT"),
            bigquery.SchemaField("2P", "INTEGER"),
            bigquery.SchemaField("2PA", "INTEGER"),
            bigquery.SchemaField("2P%", "FLOAT"),
            bigquery.SchemaField("3P", "INTEGER"),
            bigquery.SchemaField("3PA", "INTEGER"),
            bigquery.SchemaField("3P%", "FLOAT"),
            bigquery.SchemaField("FT", "INTEGER"),
            bigquery.SchemaField("FTA", "INTEGER"),
            bigquery.SchemaField("FT%", "FLOAT"),
            bigquery.SchemaField("ORB", "INTEGER"),
            bigquery.SchemaField("DRB", "INTEGER"),
            bigquery.SchemaField("TRB", "INTEGER"),
            bigquery.SchemaField("AST", "INTEGER"),
            bigquery.SchemaField("STL", "INTEGER"),
            bigquery.SchemaField("BLK", "INTEGER"),
            bigquery.SchemaField("TOV", "INTEGER"),
            bigquery.SchemaField("PF", "INTEGER"),
            bigquery.SchemaField("PTS", "INTEGER"),
            bigquery.SchemaField("PER", "FLOAT"),
            bigquery.SchemaField("TS%", "FLOAT"),
            bigquery.SchemaField("WS", "FLOAT"),
            bigquery.SchemaField("OWS", "FLOAT"),
            bigquery.SchemaField("DWS", "FLOAT"),
            bigquery.SchemaField("WS48", "FLOAT"),
            bigquery.SchemaField("OBPM", "FLOAT"),
            bigquery.SchemaField("DBPM", "FLOAT"),
            bigquery.SchemaField("BPM", "FLOAT"),
            bigquery.SchemaField("VORP", "FLOAT"),
        ]

    @staticmethod
    def df_schema() -> dict:
        return {
            "Year": "Int32",
            "Player": str,
            "Tm": str,
            "Pos": str,
            "Age": "Int32",
            "G": "Int32",
            "MP": "Int32",
            "FG": "Int32",
            "FGA": "Int32",
            "FG%": "Float64",
            "2P": "Int32",
            "2PA": "Int32",
            "2P%": "Float64",
            "3P": "Int32",
            "3PA": "Int32",
            "3P%": "Float64",
            "FT": "Int32",
            "FTA": "Int32",
            "FT%": "Float64",
            "ORB": "Int32",
            "DRB": "Int32",
            "TRB": "Int32",
            "AST": "Int32",
            "STL": "Int32",
            "BLK": "Int32",
            "TOV": "Int32",
            "PF": "Int32",
            "PTS": "Int32",
            "PER": "Float64",
            "TS%": "Float64",
            "WS": "Float64",
            "OWS": "Float64",
            "DWS": "Float64",
            "WS48": "Float64",
            "OBPM": "Float64",
            "DBPM": "Float64",
            "BPM": "Float64",
            "VORP": "Float64"
            }
