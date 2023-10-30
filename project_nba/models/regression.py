import numpy as np
import pandas as pd
from pyhocon import ConfigFactory
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from math import sqrt


class Regression:
    def __init__(self, config: ConfigFactory):
        self.historical_stats = "project:dataset.table"
        self.new_stats = "project:dataset.table"

    def read_data():

        stats = pd.read_csv('/home/admin/src/project_nba/sample_data/historical_stats.csv')
        stats.drop(["Unnamed: 0"], axis=1, inplace=True)

        new_stats = pd.read_csv('/home/admin/src/project_nba/sample_data/new_stats_2024.csv')
        new_stats.drop(["Unnamed: 0"], axis=1, inplace=True)

        winners = pd.read_csv('/home/admin/src/project_nba/sample_data/winners.csv')
        winners.drop(["Unnamed: 0"], axis=1, inplace=True)

        stats = pd.concat([stats, new_stats])
        stat_types = {"Year": "Int32", "Player": str, "Pos": str, "Age": "Int32", "Tm": str, "G": "Int32", "GS": "Int32", "MP": "Int32", "PER": "Float64", "TS%": "Float64", "3PAr": "Float64", "FTr": "Float64", "ORB%": "Float64", "DRB%": "Float64", "TRB%": "Float64", "AST%": "Float64", "STL%": "Float64", "BLK%": "Float64", "TOV%": "Float64", "USG%": "Float64", "OWS": "Float64", "DWS": "Float64", "WS": "Float64", "WS/48": "Float64", "OBPM": "Float64", "DBPM": "Float64", "BPM": "Float64", "VORP": "Float64", "FG": "Int32", "FGA": "Int32", "FG%": "Float64", "3P": "Int32", "3PA": "Int32", "3P%": "Float64", "2P": "Int32", "2PA": "Int32", "2P%": "Float64", "eFG%": "Float64", "FT": "Int32", "FTA": "Int32", "FT%": "Float64", "ORB": "Int32", "DRB": "Int32", "TRB": "Int32", "AST": "Int32", "STL": "Int32", "BLK": "Int32", "TOV": "Int32", "PF": "Int32", "PTS": "Int32"}
        stats = stats.astype(stat_types)


        winner_types = {"Year": "Int32", "MVP": str, "CHAMP": str}
        winners = winners.astype(winner_types)

    # MVP award started in 1956
    stats = stats[stats['Year'] >= 1956]


stats.drop(['GS','3PAr','FTr','ORB%','DRB%','TRB%','AST%','STL%','BLK%','TOV%','USG%','FG%','FT%','3P%','2P%','eFG%','WS/48'],axis=1,inplace=True)
