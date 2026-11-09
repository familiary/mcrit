import os
from dataclasses import dataclass

from mcrit.config.ConfigInterface import ConfigInterface


@dataclass
class GunicornConfig(ConfigInterface):
    # introduce a switch for gunicorn
    USE_GUNICORN: bool = False
    # specified as "<host>:<port>" to be able to pass it easily into gunicorn config; waitress listens on it as well.
    # Loopback unless MCRIT_SERVER_BIND says otherwise: the API has no authentication unless MCRIT_AUTH_TOKEN
    # is set, so serving other hosts (e.g. "0.0.0.0:8000" in a container) is opt-in
    BIND: str = os.getenv("MCRIT_SERVER_BIND", "").strip() or "127.0.0.1:8000"
    # The number of workers gunicorn will spin up
    WORKERS: int = 4
    # The number of threads in each worker
    THREADS: int = 8
    # timeout before silent workers are killed
    TIMEOUT: int = 120
