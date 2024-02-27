from dataclasses import dataclass, field
from cloudquery.sdk.scheduler import Client as ClientABC

from plugin.lseg.client import LSEGClient

DEFAULT_CONCURRENCY = 10
DEFAULT_QUEUE_SIZE = 10000
DEFAULT_RETRY_LIMIT = 3


@dataclass
class Spec:
    username: str
    password: str
    base_url: str = field(default="https://dmd.lseg.com/dmd/")
    concurrency: int = field(default=DEFAULT_CONCURRENCY)
    queue_size: int = field(default=DEFAULT_QUEUE_SIZE)

    def validate(self):
        if self.username is None:
            raise Exception("username must be provided")
        if self.password is None:
            raise Exception("password must be provided")


class Client(ClientABC):
    def __init__(self, spec: Spec) -> None:
        self._spec = spec
        self._client = LSEGClient(spec.username, spec.password, spec.base_url)

    def id(self):
        return "lseg"

    @property
    def client(self) -> LSEGClient:
        return self._client
