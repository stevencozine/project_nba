import logging
# import argparse
import numpy as np
import pandas as pd
from pyhocon import ConfigFactory
from urllib.request import urlopen
from bs4 import BeautifulSoup
from project_nba.objects.player import Player
from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)


class GenerateNewStats:
    def __init__(self, config: ConfigFactory):
        self.config = config
        self.PROJECT_ID = self.config.get_string("gcp.project")
        self.DATASET = self.config.get_string("gcp.bigquery.output.dataset")
        self.TABLE = self.config.get_string("gcp.bigquery.output.new_stats")
        self.year = self.config.get_string("bball.current_year")
        self.total_stats = self.config.get_string("bball.total_stats")
        self.adv_stats = self.config.get_string("bball.adv_stats")

    def generate_data(
            self
    ) -> None:
        logging.info("GENERATE")
        data = self.scrape([self.total_stats, self.adv_stats])
        self.write(data)

    def scrape(self, urls):
        logging.info("SCRAPE")
        stat_url = urls[0]
        adv_url = urls[1]

        stat_header = ["Player", "Pos", "Age", "Tm", "G", "GS", "MP", "FG", "FGA", "FG%", "3P", "3PA", "3P%", "2P", "2PA", "2P%", "eFG%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS"]
        adv_header = ["Player", "Pos", "Age", "Tm", "G", "MP", "PER", "TS%", "3PAr", "FTr", "ORB%", "DRB%", "TRB%", "AST%", "STL%", "BLK%", "TOV%", "USG%", "blank1", "OWS", "DWS", "WS", "WS/48", "blank2", "OBPM", "DBPM", "BPM", "VORP"]

        cols = ["Year", "Player", "Tm", "Pos", "Age", "G", "MP", "FG", "FGA", "FG%", "2P", "2PA", "2P%", "3P", "3PA", "3P%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS", "PER", "TS%", "WS", "OWS", "DWS", "WS48", "OBPM", "DBPM", "BPM", "VORP"]

        collections = {"stats": [stat_url, stat_header], "adv": [adv_url, adv_header]}

        dfs = []

        for v in collections.values():
            html = urlopen(v[0])
            soup = BeautifulSoup(html, features="lxml")

            rows = soup.findAll("tr")[1:]

            generic_stats = [[td.getText() for td in rows[i].findAll("td")] for i in range(len(rows))]
            generic_stats = [e for e in generic_stats if e != []]

            df = pd.DataFrame(generic_stats, columns=v[1])
            df = df.replace(r'^\s*$', np.nan, regex=True)
            df.dropna(axis=1)
            dfs.append(df)

        final_df = pd.merge(dfs[0], dfs[1].drop(['Pos', 'Age', 'G', 'MP'], axis=1), how="outer", on=["Player", "Tm"])

        final_df["Year"] = self.year
        final_df["WS48"] = final_df["WS/48"]
        final_df = final_df[cols]
        final_df = final_df.astype(Player.df_schema())
        logging.info(df.columns)
        return final_df

    def write(self, data):
        logging.info(f"WRITE to {self.PROJECT_ID}:{self.DATASET}.{self.TABLE}")
        table = f"{self.DATASET}.{self.TABLE}"
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(schema=Player.bq_schema_field())
        job = client.load_table_from_dataframe(
            data, table, job_config=job_config
        )

        job.result()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("ENTER")
    # parser = argparse.ArgumentParser()
    conf = ConfigFactory.parse_file('/home/admin/src/project_nba/config/application.conf')
    generator = GenerateNewStats(conf)
    generator.generate_data()
