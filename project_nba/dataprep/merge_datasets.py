import pandas as pd
import numpy as np

whole_types = {"Rk": "Int32", "Year": "Int32", "Player": str, "Pos": str, "Age": "Int32", "Tm": str, "G": "Int32", "GS": "Int32", "MP": "Int32", "PER": "Float64", "TS%": "Float64", "3PAr": "Float64", "FTr": "Float64", "ORB%": "Float64", "DRB%": "Float64", "TRB%": "Float64", "AST%": "Float64", "STL%": "Float64", "BLK%": "Float64", "TOV%": "Float64", "USG%": "Float64", "OWS": "Float64", "DWS": "Float64", "WS": "Float64", "WS/48": "Float64", "OBPM": "Float64", "DBPM": "Float64", "BPM": "Float64", "VORP": "Float64", "FG": "Int32", "FGA": "Int32", "FG%": "Float64", "3P": "Int32", "3PA": "Int32", "3P%": "Float64", "2P": "Int32", "2PA": "Int32", "2P%": "Float64", "eFG%": "Float64", "FT": "Int32", "FTA": "Int32", "FT%": "Float64", "ORB": "Int32", "DRB": "Int32", "TRB": "Int32", "AST": "Int32", "STL": "Int32", "BLK": "Int32", "TOV": "Int32", "PF": "Int32", "PTS": "Int32"}

stat_types = {"Rk": "Int32", "Player": str, "Pos": str, "Age": "Int32", "Tm": str, "G": "Int32", "GS": "Int32", "MP": "Int32", "FG": "Int32", "FGA": "Int32", "FG%": "Float64", "3P": "Int32", "3PA": "Int32", "3P%": "Float64", "2P": "Int32", "2PA": "Int32", "2P%": "Float64", "eFG%": "Float64", "FT": "Int32", "FTA": "Int32", "FT%": "Float64", "ORB": "Int32", "DRB": "Int32", "TRB": "Int32", "AST": "Int32", "STL": "Int32", "BLK": "Int32", "TOV": "Int32", "PF": "Int32", "PTS": "Int32"}
adv_stat_types = {"Rk": "Int32", "Player": str, "Pos": str, "Age": "Int32", "Tm": str, "G": "Int32", "MP": "Int32", "PER": "Float64", "TS%": "Float64", "3PAr": "Float64", "FTr": "Float64", "ORB%": "Float64", "DRB%": "Float64", "TRB%": "Float64", "AST%": "Float64", "STL%": "Float64", "BLK%": "Float64", "TOV%": "Float64", "USG%": "Float64", "OWS": "Float64", "DWS": "Float64", "WS": "Float64", "WS/48": "Float64", "OBPM": "Float64", "DBPM": "Float64", "BPM": "Float64", "VORP": "Float64"}

season_stats = pd.read_csv("/home/admin/src/project_nba/sample_data/Seasons_Stats.csv")

season_stats.dropna(how="all", axis="columns", inplace=True)
season_stats = season_stats.astype(whole_types)

season_stats.drop(["Rk"], axis=1, inplace=True)
cols = season_stats.columns

# for i in range(2018, 2024):
#     adv_stats = pd.read_csv(f"/home/admin/src/project_nba/sample_data/adv_stats_{i}.csv")
#     stats = pd.read_csv(f"/home/admin/src/project_nba/sample_data/stats_{i}.csv")

#     adv_stats = adv_stats.astype(adv_stat_types)
#     stats = stats.astype(stat_types)

#     adv_stats["Year"] = i
#     stats["Year"] = i

#     adv_stats["Year"] = adv_stats["Year"].astype("Int32")
#     stats["Year"] = stats["Year"].astype("Int32")

#     adv_stats.dropna(how="all", axis="columns", inplace=True)
#     stats.dropna(how="all", axis="columns", inplace=True)

#     adv_stats.drop(["Rk", "Pos", "Age", "G", "MP"], axis=1, inplace=True)
#     stats.drop(["Rk"], axis=1, inplace=True)

#     year_stats = stats.merge(adv_stats, how="outer", on=["Player", "Tm", "Year"])
#     # pr"Int32"(year_stats.columns)
#     year_stats = year_stats[cols]
#     season_stats = pd.concat([season_stats, year_stats], ignore_index=True)

# season_stats.to_csv("/home/admin/src/project_nba/sample_data/historical_stats.csv")

for i in range(2024,2025):
    adv_stats = pd.read_csv(f"/home/admin/src/project_nba/sample_data/adv_stats_{i}.csv")
    stats = pd.read_csv(f"/home/admin/src/project_nba/sample_data/stats_{i}.csv")

    adv_stats = adv_stats.astype(adv_stat_types)
    stats = stats.astype(stat_types)

    adv_stats["Year"] = i
    stats["Year"] = i

    adv_stats["Year"] = adv_stats["Year"].astype("Int32")
    stats["Year"] = stats["Year"].astype("Int32")

    adv_stats.dropna(how="all", axis="columns", inplace=True)
    stats.dropna(how="all", axis="columns", inplace=True)

    adv_stats.drop(["Rk", "Pos", "Age", "G", "MP"], axis=1, inplace=True)
    stats.drop(["Rk"], axis=1, inplace=True)

    year_stats = stats.merge(adv_stats, how="outer", on=["Player", "Tm", "Year"])
    # pr"Int32"(year_stats.columns)
    year_stats = year_stats[cols]

    year_stats.to_csv(f"/home/admin/src/project_nba/sample_data/new_stats_{i}.csv")
