import os
import time as ttime

from prefect import flow, task
from prefect.blocks.system import Secret
from tiled.client import from_profile

BEAMLINE_OR_ENDSTATION = "cdi"


@task(retries=2, retry_delay_seconds=10)
def read_all_streams(uid, beamline_acronym=BEAMLINE_OR_ENDSTATION):
    api_key = Secret.load(f"tiled-{beamline_acronym}-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)
    run = tiled_client[beamline_acronym]["raw"][uid]
    print(f"Validating uid {run.start['uid']}")
    start_time = ttime.monotonic()
    for stream in run['streams']:
        print(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        print(f"{stream} elapsed_time = {stream_elapsed_time}")
        print(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    print(f"{elapsed_time = }")


@flow(log_prints=True)
def data_validation(uid):
    read_all_streams(uid)
