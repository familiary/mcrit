import logging
from timeit import default_timer as timer

import falcon
from bson import json_util

from mcrit.index.MatchingParameters import MatchingParameterError, applyMatchingPreset, resolveMatchingParams

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


# band_df_cutoff ends up in a MongoDB query ({"df": {"$lte": cutoff}}), whose integers end here;
# shortlist_size shares the bound so the two knobs accept the same range
_MATCHING_KNOB_MAX = 2**63 - 1


def _parseJobKnob(key, value):
    """shortlist_size or band_df_cutoff as an int, refusing what no job could apply (#217).

    Refused rather than ignored, unlike the older options: an ignored value is replaced by the
    configured one, so the caller would get a result computed under a setting they did not ask for,
    with nothing in the response to say so.
    """
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise MatchingParameterError(f"{key} must be an integer, not {value!r}.") from None
    if number < 0 or number > _MATCHING_KNOB_MAX:
        raise MatchingParameterError(f"{key} must be an integer from 0 (off) to {_MATCHING_KNOB_MAX}.")
    return number


def getMatchingParams(req_params, config=None, with_shortlist=True):
    """The matching options of a request, as keyword arguments for the matching jobs.

    Given the server's config, every matching knob the request leaves out is filled in with the
    value the job will run with (see mcrit.index.MatchingParameters, which MinHashIndex applies to
    direct callers as well). `with_shortlist=False` is for matches restricted to the samples they
    name, which take no shortlist. `preset` names a bundle of knobs (MATCHING_PRESETS) that fills in
    the ones the request leaves out.

    Raises MatchingParameterError for an unusable shortlist_size or band_df_cutoff, for a
    shortlist_size on a match that takes none, and for an unknown preset.
    """
    parameters = {}
    preset = None
    for key, value in req_params.items():
        if key in ("shortlist_size", "band_df_cutoff"):
            parameters[key] = _parseJobKnob(key, value)
            continue
        if key == "preset":
            # refused when unknown, like the two knobs above, rather than ignored
            preset = value
            continue
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
    with_shortlist = with_shortlist and not parameters.get("sample_group_only")
    if not with_shortlist and "shortlist_size" in parameters:
        # refused, not dropped, for the reason an unusable value is: the caller asked for something
        # this match does not do, and a silently different answer would not say so
        raise MatchingParameterError("shortlist_size does not apply to a match restricted to the samples it names (one against another, a group or a cross compare).")
    if preset is not None:
        # expanded into knob values here, so the job is keyed on the values it runs with and a preset
        # request shares its job with the equivalent explicit one
        parameters = applyMatchingPreset(parameters, preset, with_shortlist=with_shortlist, config=config)
    if config is not None:
        parameters = resolveMatchingParams(parameters, config, with_shortlist=with_shortlist)
    return parameters


def readMatchingParams(index, req, resp, handler, with_shortlist=True):
    """getMatchingParams for a resource: the parameters, or None after answering a 400 for them."""
    try:
        return getMatchingParams(req.params, index.config, with_shortlist=with_shortlist)
    except MatchingParameterError as error:
        resp.status = falcon.HTTP_400
        resp.data = jsonify({"status": "failed", "data": {"message": str(error)}})
        db_log_msg(index, req, f"{handler} - failed - {error}")
        return None


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
    def wrapper(*args, **kwargs):
        start = timer()
        func(*args, **kwargs)
        end = timer()
        LOGGER.info("  *** this took: %s sec" % (end - start))

    return wrapper
