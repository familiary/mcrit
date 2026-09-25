import functools
import logging
from timeit import default_timer as timer

from bson import json_util

LOGGER = logging.getLogger(__name__)


def get_username(req):
    """The user a request was made for, as MCRITweb and McritClient send it, or None."""
    return req.get_header("username", default=None)


def db_log_msg(index, req, message, level=None):
    username = get_username(req) or "anonymous"
    if level is None:
        LOGGER.info(f"{username} - {message}")
    index._storage.dbLogEvent(message, username=username)
    return


def getMatchingParams(req_params):
    parameters = {}
    for key, value in req_params.items():
        try:
            if key == "pichash_size":
                pichash_size = int(value)
                pichash_size = max(0, pichash_size)
                parameters["pichash_size"] = pichash_size
                # self.index.updatePicHashSize(pichash_size)
            if key == "minhash_score":
                minhash_score = int(value)
                minhash_score = max(0, min(100, minhash_score))
                parameters["minhash_threshold"] = minhash_score
            if key == "force_recalculation":
                if value.lower() == "true":
                    parameters["force_recalculation"] = True
            if key == "sample_group_only":
                if value.lower() == "true":
                    parameters["sample_group_only"] = True
            if key == "band_matches_required":
                band_matches_required = int(value)
                band_matches_required = max(0, band_matches_required)
                parameters["band_matches_required"] = band_matches_required
        except (AttributeError, TypeError, ValueError):
            LOGGER.warning(f"Failed to handle request parameter: {key}: {value}")
    return parameters


def getUniqueBlocksParams(req_params):
    parameters = {}
    for key, value in req_params.items():
        try:
            if key == "covers_required":
                # k of the k-of-n cover: every sample must be reached by this many selected blocks
                parameters["covers_required"] = max(1, int(value))
            if key == "min_instructions":
                # blocks shorter than this are dropped before the cover is chosen
                parameters["min_instructions"] = max(0, int(value))
        except (AttributeError, TypeError, ValueError):
            LOGGER.warning(f"Failed to handle request parameter: {key}: {value}")
    return parameters


def jsonify(content, debug_print=False):
    if debug_print:
        print(content)
        print(json_util.dumps(content).encode("utf-8"))
    return json_util.dumps(content).encode("utf-8")


def timing(func):
    # wraps keeps the responder's name and docstring, which the API reference is generated from
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = timer()
        func(*args, **kwargs)
        end = timer()
        LOGGER.info("  *** this took: %s sec" % (end - start))

    return wrapper
