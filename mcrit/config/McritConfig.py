import logging
import os

from .MinHashConfig import MinHashConfig
from .QueueConfig import QueueConfig
from .ShinglerConfig import ShinglerConfig
from .StorageConfig import StorageConfig


class McritConfig:
    # served by /version; has to equal [project].version in pyproject.toml, which the release workflow checks
    VERSION = "1.16.0"
    # basic pathing info
    CONFIG_FILE_PATH = str(os.path.abspath(__file__))
    PROJECT_ROOT = str(os.path.abspath(os.sep.join([CONFIG_FILE_PATH, "..", ".."])))

    # Authentication token, which can be optionally used to lock down communication with the API.
    # It can be supplied via the environment, so that it does not have to be stored in a config file.
    AUTH_TOKEN = os.getenv("MCRIT_AUTH_TOKEN", "")

    # ApiScout WinAPI databases handed to SMDA, which resolves the API references of memory dumps
    # with them (files get theirs from the import table either way). Off when empty, the default.
    # "os_name=path" entries separated by ";" or ",", or just a path; see mcrit/config/SmdaConfigFactory.py
    # and the README. Enabling it can change the code SMDA recovers from a dump, so dumps indexed
    # before and after are not disassembled alike.
    API_COLLECTION_FILES = os.getenv("MCRIT_API_COLLECTION_FILES", "")

    ### global logging-config setup
    # Only do basicConfig if no handlers have been configured
    LOG_PATH = "./"
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = "%(asctime)-15s: %(name)-32s - %(message)s"

    # initialize context-based configurations
    MINHASH_CONFIG = MinHashConfig()
    SHINGLER_CONFIG = ShinglerConfig()
    STORAGE_CONFIG = StorageConfig()
    QUEUE_CONFIG = QueueConfig()

    def __init__(self, log_level=logging.INFO):
        if not logging.root.handlers:
            logging.basicConfig(level=log_level, format=self.LOG_FORMAT)
