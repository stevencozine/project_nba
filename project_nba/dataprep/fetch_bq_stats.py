import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions
from base_pipeline import BaseBeamJob
from pyhocon import ConfigFactory
from datetime import datetime

LOGGER = logging.getLogger(__name__)


class FetchBQStats(BaseBeamJob):
    def __init__(self, config: ConfigFactory):
        super().__init__(config)
        self.table = config.get_string("gcp.bigquery.historic_stats")

    # def fetch_pipeline(
    #     self,
    #     job_name: str,
    #     dataset: str,
    #     table: str,
    #     beam_options: list[str] = None,
    # ) -> beam.PCollection:
    #     dt = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    #     # dataflow_options = PipelineOptions(beam_options, save_main_session=True, job_name=f"{job_name}_{dt}")
    #     dataflow_opts = self.construct_dataflow_args()
    #     dataflow_options = PipelineOptions(dataflow_opts, save_main_session=True, job_name=f"{job_name}_{dt}")
    #     worker_options = dataflow_options.view_as(WorkerOptions)
    #     worker_options.default_sdk_harness_log_level = "INFO"
    #     worker_options.sdk_harness_log_level_overrides = {"apache_beam.runners.dataflow": "INFO"}

    #     with beam.Pipeline(options=beam_options) as pipeline:
    #         historic_stats = (
    #             pipeline
    #             | "Read Tables" >> beam.io.ReadFromBigQuery(table=f"{self.PROJECT_ID}:{dataset}.{table}")
    #         )

    #     return historic_stats
