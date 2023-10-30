import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions
from pyhocon import ConfigFactory
from datetime import datetime
from base_pipeline import BaseBeamJob
from fetch_bq_stats import FetchBQStats
from project_nba.dataprep.generate_new_stats import FetchNewStats

LOGGER = logging.getLogger(__name__)


class DataPrep(BaseBeamJob):
    def __init__(self, config: ConfigFactory):
        super().__init__(config)

    @staticmethod
    def debug(row):
        return row

    def dataprep(
        self,
        job_name,
        dataset,
        table,
        beam_options: list[str] = None,
    ) -> beam.PCollection:
        dt = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        # dataflow_options = PipelineOptions(beam_options, save_main_session=True, job_name=f"{job_name}_{dt}")
        dataflow_opts = self.construct_dataflow_args()
        dataflow_options = PipelineOptions(dataflow_opts, save_main_session=True, job_name=f"{job_name}_{dt}")
        worker_options = dataflow_options.view_as(WorkerOptions)
        worker_options.default_sdk_harness_log_level = "INFO"
        worker_options.sdk_harness_log_level_overrides = {"apache_beam.runners.dataflow": "INFO"}

        with beam.Pipeline(options=beam_options) as pipeline:
            historic_stat_fetch = FetchBQStats(self.config)
            historic_stats = historic_stat_fetch.fetch_pipeline("fetch_historic_stats", "historic_dataset", "historic_table", beam_options=beam_options)
            new_stat_fetch = FetchNewStats(self.config)
            new_stats = new_stat_fetch.fetch_pipeline("fetch_new_stats", "new_dataset", "new_table", beam_options=beam_options)

            dataprep = (
                (historic_stats, new_stats)
                | "Merge Data" >> beam.Flatten()
                | "Transform Data" >> beam.Map(DataPrep.debug)
            )

        return dataprep
