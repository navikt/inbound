import datetime
import json
import os
import tempfile
import tracemalloc
from enum import Enum
from pathlib import Path
from typing import Union

import yaml
from jinja2 import Template

from inbound.core import connection_factory, connection_loader
from inbound.core.environment import get_env
from inbound.core.job_factory import JobFactory
from inbound.core.jobs_result import JobsResult
from inbound.core.logging import LOGGER
from inbound.core.models import *
from inbound.core.utils import generate_id


class Mode(Enum):
    SEQUENTIAL = 1
    PARALELL = 2


def run_job(
    source: Union[str, dict], output_dir: Path = None, callback: str = None
) -> JobsResult:
    LOGGER.info(f"Running job {str(source)}")
    jobs_spec = _get_json_config(source)

    # Replace 'env_var's in template
    temp = Template(json.dumps(jobs_spec)).render(env_var=get_env)
    jobs_config = json.loads(temp, strict=False)

    # Get list of jobs to run
    try:
        jobs = JobsModel(**jobs_config).jobs
        return _run_jobs_in_list(jobs=jobs, output_dir=output_dir, callback=callback)
    except Exception as e:
        LOGGER.info(f"Invalid jobs configuration: {str(e)}")
        return JobsResult()


def run_jobs(path: str = "./jobs", output_dir: Path = None):
    try:
        job_definition_files = [
            os.path.join(d, x)
            for d, dirs, files in os.walk(path)
            for x in files
            if x.endswith(".yml")
        ]
    except Exception as e:
        LOGGER.info(f"Error in searching for job .yml files i path: {path}. {str(e)}")
        return JobsResult()

    try:
        for job_definition_file in job_definition_files:
            LOGGER.info(f"Running jobs in {job_definition_file}")
            res = run_job(job_definition_file, output_dir)
            if res.result != "DONE":
                LOGGER.info(
                    f"Error running job: {job_definition_file}. Result: {str(res)}"
                )
        return JobsResult(result="DONE")
    except:
        LOGGER.error(f"Error running jobs in {job_definition_file}")
        return JobsResult()


def _get_json_config(source: Union[str, dict]):
    if type(source) == dict:
        return source

    try:
        jobs_spec = json.loads(source)
        LOGGER.info(f"Loaded jobs configuration from json {source}")
        return jobs_spec
    except:
        pass

    if not Path(source).is_file():
        source = str(Path.cwd() / source)

    try:
        with open(source, "r") as f:
            # default format is yaml
            try:
                jobs_spec = yaml.safe_load(f)
                return jobs_spec
            except:
                # try alternative format: json
                try:
                    jobs_spec = json.load(f)
                    return jobs_spec
                except:
                    pass

        LOGGER.info(f"Loaded jobs configuration from {source}")
    except Exception as e:
        LOGGER.info(f"Error loading jobs configuration from {source}. {e}")


def _run_jobs_in_list(
    jobs: JobsModel,
    mode: Mode = Mode.SEQUENTIAL,
    output_dir: Path = None,
    callback: str = None,
) -> JobsResult:
    # Load plugins for source og target
    source_types = [job.source.type for job in jobs]
    sink_types = [job.target.type for job in jobs]
    types = list(set(source_types + sink_types))
    connection_loader.load_plugins(types)

    # Run E(T)L jobs
    jobs_result = JobsResult(start_date_time=datetime.datetime.now())
    jobs_result.job_name = "Run jobs"
    LOGGER.info(f"Starting {len(jobs)} jobs:")
    [LOGGER.info(job.name) for job in jobs]
    tracemalloc.start()
    for job in jobs:
        source_connector = connection_factory.create(job.source)
        sink_connector = connection_factory.create(job.target)
        job_instance = JobFactory(source_connector, sink_connector, job, output_dir)()
        LOGGER.info(
            f"Starting job: {job.name} ({job.job_id}). Source: {job.source.name or job.source.type}. Target: {job.target.name or job.target.type}"
        )
        try:
            res = job_instance.run()
            jobs_result.end_date_time = datetime.datetime.now()
            jobs_result.memory = tracemalloc.get_traced_memory()
            jobs_result.append(res)
            jobs_result.result = "DONE"
            jobs_result.log(output_dir)
        except Exception as e:
            LOGGER.error(
                f"Error running job: {job.name} ({job.job_id}). Source: {job.source.name or job.source.type}. Target: {job.target.name or job.target.type}. {e}"
            )
            jobs_result.end_date_time = datetime.datetime.now()
            jobs_result.memory = tracemalloc.get_traced_memory()
            jobs_result.result = "FAILED"
            jobs_result.log(output_dir)
        finally:
            LOGGER.info(
                f"Finished job: {job.name} ({job.job_id}). Source: {job.source.name or job.source.type}. Target: {job.target.name or job.target.type}"
            )

            if callback is not None:
                LOGGER.info(f"callback end job: {callback}")

    LOGGER.info(f"Finished {len(jobs)}")
    tracemalloc.stop()
    return jobs_result
