import logging
import numpy as np
import pandas as pd
import math
from datetime import datetime
from pyhocon import ConfigFactory
from sklearn.linear_model import LinearRegression
from google.cloud import bigquery
from project_nba.objects.player import Player

LOGGER = logging.getLogger(__name__)


class Regression:
    def __init__(self, config: ConfigFactory):
        self.config = config
        self.dt = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        self.project_id = self.config.get_string("gcp.project")
        self.historic_dataset = self.config.get_string("gcp.bigquery.input.dataset")
        self.historic_table = self.config.get_string("gcp.bigquery.input.historic_stats")
        self.winners_table = self.config.get_string("gcp.bigquery.input.winners")
        self.new_dataset = self.config.get_string("gcp.bigquery.output.dataset")
        self.new_table = self.config.get_string("gcp.bigquery.output.new_stats")
        self.current_year = self.config.get_string("bball.current_year")
        self.train_split = self.config.get_string("models.regr.train_split")
        self.features = ["Age", "PPG", "ORPG", "DRPG", "APG", "SPG", "BPG", "TPG", "MPG", "G", "TS%", "PER", "OWS", "DWS", "OBPM", "DBPM", "VORP"]
        self.target = ["MVP"]
        self.prediction_dataset = self.config.get_string("gcp.bigquery.predictions.dataset")
        self.prediction_table = self.config.get_string("gcp.bigquery.predictions.regression.table")

    def read_data(self, dataset_id, table_id) -> pd.DataFrame:
        logging.info("READ")
        client = bigquery.Client()

        dataset_ref = bigquery.DatasetReference(self.project_id, dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = client.get_table(table_ref)

        data = client.list_rows(table).to_dataframe()

        return data

    def write_data(self, data, dataset_id, table_id) -> None:
        logging.info("WRITE")
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(schema=[
            bigquery.SchemaField("Year", "INTEGER"),
            bigquery.SchemaField("Player", "STRING"),
            bigquery.SchemaField("Tm", "STRING"),
            bigquery.SchemaField("Prediction", "FLOAT"),
        ])

        job = client.load_table_from_dataframe(
            data, f"{dataset_id}.{table_id}", job_config=job_config
        )

        job.result()

    def gather_data(self) -> pd.DataFrame:
        logging.info("COALESCE")
        stat_types = Player.df_schema()

        winners = self.read_data(self.historic_dataset, self.winners_table)
        winner_types = {"Year": int, "MVP": str, "CHAMP": str}
        winners = winners.astype(winner_types)

        historical_stats = self.read_data(self.historic_dataset, self.historic_table)
        historical_stats = historical_stats.astype(stat_types)
        historical_stats = self.clean_data(historical_stats)
        historical_stats = pd.merge(historical_stats, winners, how="left", on="Year")
        historical_stats = self.create_features(historical_stats)

        current_year = pd.DataFrame([[self.current_year, "", ""]], columns=["Year", "MVP", "CHAMP"])
        current_year = current_year.astype(winner_types)
        new_stats = self.read_data(self.new_dataset, self.new_table)
        new_stats = new_stats.astype(stat_types)
        new_stats.fillna(0, inplace=True)
        new_stats = self.clean_data(new_stats)
        new_stats = pd.merge(new_stats, current_year, how="left", on="Year")
        new_stats = self.create_features(new_stats)

        stats = pd.concat([historical_stats, new_stats])

        # MVP award started in 1956
        stats = stats[stats["Year"] >= 1956]

        return stats

    def clean_data(self, stats) -> pd.DataFrame:
        logging.info("CLEAN")
        stats.drop(["FG%", "2P%", "3P%", "FT%", "WS48"], axis=1, inplace=True)
        stats["Player"] = stats["Player"].str.replace("*", "")

        stats["3P"].fillna(0, inplace=True)
        stats["3PA"].fillna(0, inplace=True)
        stats.dropna(inplace=True)

        return stats

    def create_features(self, stats) -> pd.DataFrame:
        # Feature Engineering
        stats["MPG"] = stats["MP"] / stats["G"]
        stats["RPG"] = stats["TRB"] / stats["G"]
        stats["DRPG"] = stats["DRB"] / stats["G"]
        stats["ORPG"] = stats["ORB"] / stats["G"]
        stats["APG"] = stats["AST"] / stats["G"]
        stats["SPG"] = stats["STL"] / stats["G"]
        stats["BPG"] = stats["BLK"] / stats["G"]
        stats["TPG"] = stats["TOV"] / stats["G"]
        stats["PPG"] = stats["PTS"] / stats["G"]

        mvp_cond = [stats["Player"] == stats["MVP"]]
        stats["MVP"] = np.select(mvp_cond, [1], default=0)

        champ_cond = [stats["Tm"] == stats["CHAMP"]]
        stats["CHAMP"] = np.select(champ_cond, [1], default=0)

        return stats

    def run(self):
        logging.info("RUN")
        stats = self.gather_data()

        cutoff = min(math.floor(stats["Year"].nunique()*float(self.train_split)) + int(stats["Year"].min()), int(self.current_year) - 1)

        logging.info("REGRESSION")
        train = stats[stats["Year"] < int(cutoff)]
        test = stats[stats["Year"] >= int(cutoff)]
        X_train = train[self.features]
        y_train = train[self.target]
        X_test = test[self.features]
        y_test = test[self.target]

        regressor = LinearRegression()
        regressor.fit(X_train, y_train)

        y_pred = regressor.predict(X_test)
        y_pred = pd.DataFrame(y_pred, columns=["Prediction"])

        test.reset_index(inplace=True, drop=True)
        test.drop(["Age", "G", "MPG", "Pos", "PTS", "ORB", "DRB", "AST", "STL", "BLK", "TOV", "PF", "FT", "FTA", "MP", "WS", "TRB", "FG", "FGA", "2P", "2PA", "3P", "3PA", "OBPM", "DBPM", "TS%", "PER", "OWS", "DWS", "BPM", "VORP", "RPG", "DRPG", "ORPG", "APG", "SPG", "BPG", "TPG", "PPG", "MVP", "CHAMP"], axis=1)

        y_pred.reset_index(inplace=True, drop=True)

        pred_df = pd.concat([test, y_pred], axis=1)

        pred_df = pred_df.sort_values(by=["Prediction"], ascending=False)
        pred_df.reset_index(inplace=True, drop=True)

        result = pred_df[pred_df["Year"] == int(self.current_year)]
        result = result[["Year", "Player", "Tm", "Prediction"]]
        result = result.astype({"Year": "Int64", "Player": str, "Tm": str, "Prediction": "Float64"})
        print(result.head(5))
        self.write_data(result, self.prediction_dataset, self.prediction_table + "_" + self.dt)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("INIT")
    # parser = argparse.ArgumentParser()
    conf = ConfigFactory.parse_file("/home/admin/src/project_nba/config/application.conf")
    regr = Regression(conf)
    regr.run()
