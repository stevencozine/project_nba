import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions
from pyhocon import ConfigFactory
from datetime import datetime

LOGGER = logging.getLogger(__name__)


class BaseBeamJob:
    def __init__(self, config: ConfigFactory):
        self.PROJECT_ID = config.get_string("gcp.project")
        self.REGION = config.get_string("gcp.dataflow.region")
        self.TMP_BUCKET = config.get_string("gcp.dataflow.tmp_bucket")
        self.MAX_WORKERS = config.get_string("gcp.dataflow.max_workers")
        self.MACHINE_TYPE = config.get_string("gcp.dataflow.machine_type")

    def construct_dataflow_args(self) -> list[str]:
        self.DATAFLOW_ARGS = [
            "--runner=DirectRunner",
            # "--runner=DataflowRunner",
            f"--project={self.PROJECT_ID}",
            f"--region={self.REGION}",
            f"--max_num_workers={self.MAX_WORKERS}",
            f"--worker_machine_type={self.MACHINE_TYPE}",
            f"--temp_location=gs://{self.TMP_BUCKET}/dftmp",
            f"--staging_location=gs://{self.TMP_BUCKET}/dfstg",
            # f"--service_account={self.SERVICE_ACCOUNT}",
            "--experiments=shuffle_mode=service",
            "--experiment=upload_graph",
            "--save_main_session",
        ]

    def fetch_pipeline(
        self,
        job_name,
        dataset,
        table,
        beam_options: list[str] = None,
    ):
        dt = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        # dataflow_options = PipelineOptions(beam_options, save_main_session=True, job_name=f"{job_name}_{dt}")
        dataflow_opts = self.construct_dataflow_args()
        dataflow_options = PipelineOptions(dataflow_opts, save_main_session=True, job_name=f"{job_name}_{dt}")
        worker_options = dataflow_options.view_as(WorkerOptions)
        worker_options.default_sdk_harness_log_level = "INFO"
        worker_options.sdk_harness_log_level_overrides = {"apache_beam.runners.dataflow": "INFO"}

        with beam.Pipeline(options=beam_options) as pipeline:
            elements = (
                pipeline
                | "Read Tables" >> beam.io.ReadFromBigQuery(table=f"{self.PROJECT_ID}:{dataset}.{table}")
            )

        return elements
