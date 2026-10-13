"""Azure Function `gios_poller`: every hour one poll of the GIOŚ API → the blob container of the live landing zone.

On Databricks the container is the external volume raw.landing_live (Terraform module live_ingest); a new batch
file triggers the live Job, whose declarative pipeline picks the file up with Auto Loader. Polling here instead
of on a cluster: ~20 s of HTTP calls no longer pay for a ~7 min cluster start every hour.

All the logic is smogcast.ingest.live_poll (tested in the repo); this file only adapts a blob container to its
file store and runs it on a timer. The function signs in with its user-assigned managed identity — no keys.

App settings (set by Terraform): SMOGCAST_ENV, LIVE_STORAGE_ACCOUNT_URL, LIVE_CONTAINER, LIVE_PREFIX,
AZURE_CLIENT_ID (the identity), POLL_SCHEDULE (NCRONTAB, UTC).
"""

import logging
import os

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

from smogcast.core.config import load_config
from smogcast.ingest.live_poll import poll_once

app = func.FunctionApp()


class BlobStore:
    """A blob container (under a path prefix) as the file store of live_poll."""

    def __init__(self, container: ContainerClient, prefix: str):
        self.container = container
        self.prefix = prefix

    def names(self, prefix: str) -> list[str]:
        start = self.prefix + prefix
        return sorted(b.name[len(self.prefix):] for b in self.container.list_blobs(name_starts_with=start))

    def read_text(self, path: str) -> str:
        return self.container.download_blob(self.prefix + path).readall().decode("utf-8")

    def write_text(self, path: str, text: str) -> None:
        # One upload = one blob appearing whole: Auto Loader never sees a half-written batch.
        self.container.upload_blob(self.prefix + path, text.encode("utf-8"), overwrite=True)


def _store() -> BlobStore:
    credential = DefaultAzureCredential(managed_identity_client_id=os.environ["AZURE_CLIENT_ID"])
    container = ContainerClient(account_url=os.environ["LIVE_STORAGE_ACCOUNT_URL"],
                                container_name=os.environ["LIVE_CONTAINER"], credential=credential)
    return BlobStore(container, os.environ.get("LIVE_PREFIX", ""))


@app.timer_trigger(schedule="%POLL_SCHEDULE%", arg_name="timer", run_on_startup=False, use_monitor=True)
def poll_gios(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.warning("gios_poller: the timer is past due — polling now")
    summary = poll_once(load_config(os.environ["SMOGCAST_ENV"]), _store())
    logging.info("gios_poller: %s", summary)
