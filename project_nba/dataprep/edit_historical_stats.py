import pandas as pd

df = pd.read_csv('/home/admin/src/project_nba/sample_data/historical_stats_bk.csv')

df.drop(["Unnamed: 0"], axis=1, inplace=True)

stat_types = {"Year": "Int32", "Player": str, "Pos": str, "Age": "Int32", "Tm": str, "G": "Int32", "GS": "Int32", "MP": "Int32", "PER": "Float64", "TS%": "Float64", "3PAr": "Float64", "FTr": "Float64", "ORB%": "Float64", "DRB%": "Float64", "TRB%": "Float64", "AST%": "Float64", "STL%": "Float64", "BLK%": "Float64", "TOV%": "Float64", "USG%": "Float64", "OWS": "Float64", "DWS": "Float64", "WS": "Float64", "WS/48": "Float64", "OBPM": "Float64", "DBPM": "Float64", "BPM": "Float64", "VORP": "Float64", "FG": "Int32", "FGA": "Int32", "FG%": "Float64", "3P": "Int32", "3PA": "Int32", "3P%": "Float64", "2P": "Int32", "2PA": "Int32", "2P%": "Float64", "eFG%": "Float64", "FT": "Int32", "FTA": "Int32", "FT%": "Float64", "ORB": "Int32", "DRB": "Int32", "TRB": "Int32", "AST": "Int32", "STL": "Int32", "BLK": "Int32", "TOV": "Int32", "PF": "Int32", "PTS": "Int32"}
df = df.astype(stat_types)
df['WS48'] = df['WS/48']

cols = [
    'Year',
    'Player',
    'Tm',
    'Pos',
    'Age',
    'G',
    'MP',
    'FG',
    'FGA',
    'FG%',
    '2P',
    '2PA',
    '2P%',
    '3P',
    '3PA',
    '3P%',
    'FT',
    'FTA',
    'FT%',
    'ORB',
    'DRB',
    'TRB',
    'AST',
    'STL',
    'BLK',
    'TOV',
    'PF',
    'PTS',
    'PER',
    'TS%',
    'WS',
    'OWS',
    'DWS',
    'WS48',
    'OBPM',
    'DBPM',
    'BPM',
    'VORP'
]

df = df[cols]


df.to_csv('/home/admin/src/project_nba/sample_data/historical_stats.csv', index=False)
