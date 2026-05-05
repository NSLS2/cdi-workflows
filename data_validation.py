import os
import time as ttime

from prefect import flow, task
from tiled.client import from_uri
from dotenv import load_dotenv

BEAMLINE_OR_ENDSTATION = "cdi"


def get_api_key_from_env(api_key=None):
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None, beamline_acronym=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    return cl[beamline_acronym]["raw"][uid]


@task(retries=2, retry_delay_seconds=10)
def read_stream(run, stream):
    return run[stream].read()


@flow
def data_validation(uid, api_key=None, beamline_acronym=BEAMLINE_OR_ENDSTATION):
    run = get_run(uid, api_key=api_key, beamline_acronym=beamline_acronym)
    print(f"Validating uid {run.start['uid']}")
    start_time = ttime.monotonic()
    for stream in run['streams']:
        print(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = read_stream(run, stream)
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        print(f"{stream} elapsed_time = {stream_elapsed_time}")
        print(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    print(f"{elapsed_time = }")
