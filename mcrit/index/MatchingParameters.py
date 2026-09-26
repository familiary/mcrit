"""The matching knobs a job runs with, resolved before it is submitted (#217).

A matching job is reused for any later request with the same arguments - its descriptor is its
cache key - so the arguments have to hold the values the job runs with. A knob left out used to
be filled in by the worker from its own configuration, which keyed the job on the knob's absence:
after a configuration change, every request relying on the default was served the result
computed under the old value. Resolving here, for the server and for direct callers of
MinHashIndex alike, puts the value into the key.
"""

from typing import Any, Dict

from mcrit.matchers.MatcherInterface import shortlistUnavailableReason

# the order the knobs take in a job's arguments, so a job listing shows them in the same
# positions whichever of them a request named (the descriptor itself sorts its keys)
MATCHING_KNOBS = ("minhash_threshold", "pichash_size", "band_matches_required", "shortlist_size", "band_df_cutoff", "shortlist_unavailable")


# Named bundles of knobs, picked per request (#217). A preset sets the knobs it names unless the
# request sets them itself; every other knob keeps its configured value. The values come from the
# measurements in #217, band_matches_required crossed with the shortlist: turning the shortlist on
# never moved top-10 or top-25 recall at any band_matches_required, while every value >= 2 did.
# So both presets use k=1 (the default is 2). Identifying a sample turns the shortlist on - a
# deployment's configured size, else 100, the size measured on #195 - and hunting, which wants the
# tail a shortlist cuts off, turns it off.
MATCHING_PRESETS = {
    "hunt": {"band_matches_required": 1, "shortlist_size": 0},
    "identification": {"band_matches_required": 1, "shortlist_size": 100},
}


def applyMatchingPreset(parameters: Dict[str, Any], preset: str, with_shortlist: bool = True, config=None) -> Dict[str, Any]:
    """`parameters` with the knobs of `preset` filled in wherever they are left out (None or absent).

    A preset that turns the shortlist on keeps the size `config` sets, if it sets one. A match
    restricted to the samples it names takes no shortlist (`with_shortlist=False`), so a preset
    applies the rest of its knobs there. The name is matched case-insensitively. Raises
    MatchingParameterError for anything else, a list (a repeated query parameter) included.
    """
    name = preset.strip().lower() if isinstance(preset, str) else None
    if name not in MATCHING_PRESETS:
        raise MatchingParameterError(f"preset must be one of {', '.join(sorted(MATCHING_PRESETS))}, not {preset!r}.")
    applied = dict(parameters)
    for key, value in MATCHING_PRESETS[name].items():
        if key == "shortlist_size":
            if not with_shortlist:
                continue
            configured = getattr(getattr(config, "MINHASH_CONFIG", None), "MINHASH_MATCHING_SHORTLIST_SIZE", 0) or 0
            if value > 0 and configured > 0:
                value = configured
        if applied.get(key) is None:
            applied[key] = value
    return applied


def _canonical(value):
    """One representation per value, so equal knobs make equal cache keys: True and 1, 50.0 and 50."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


class MatchingParameterError(ValueError):
    """A matching knob set to a value no job can run with; the server answers it with a 400."""


def checkBandDfCutoff(band_df_cutoff: int, config) -> None:
    """Refuse a df cutoff above STORAGE_BAND_BUCKET_SIZE, as MongoDbStorage refuses such a configured one.

    Under bucketing only bucket 0 carries a hash's df, so a spilled hash's df has to be rejectable
    by the cutoff (#196).
    """
    bucket_size = getattr(getattr(config, "STORAGE_CONFIG", None), "STORAGE_BAND_BUCKET_SIZE", 0) or 0
    if bucket_size and band_df_cutoff > bucket_size:
        raise MatchingParameterError(f"band_df_cutoff must not exceed STORAGE_BAND_BUCKET_SIZE ({bucket_size}).")


def resolveMatchingParams(parameters: Dict[str, Any], config, storage=None, with_shortlist: bool = True) -> Dict[str, Any]:
    """The job arguments for `parameters`, with every matching knob set to the value the job runs with.

    Knobs the caller named are kept, the others take the configured value. `with_shortlist=False`
    is for matches restricted to the samples they name, which take no shortlist and carry none in
    their arguments. Given the storage, a shortlist it cannot apply right now is marked
    (`shortlist_unavailable`), so the job that falls back is keyed apart from a shortlisted one.
    Other arguments pass through unchanged, after the knobs.

    Raises MatchingParameterError for a band_df_cutoff the storage could not apply.
    """
    knobs: Dict[str, Any] = {key: value for key, value in parameters.items() if key in MATCHING_KNOBS and value is not None}
    others = {key: value for key, value in parameters.items() if key not in MATCHING_KNOBS}
    minhash_config = config.MINHASH_CONFIG
    knobs.setdefault("minhash_threshold", minhash_config.MINHASH_MATCHING_THRESHOLD)
    knobs.setdefault("pichash_size", minhash_config.PICHASH_SIZE)
    knobs.setdefault("band_matches_required", minhash_config.BAND_MATCHES_REQUIRED)
    knobs.setdefault("band_df_cutoff", getattr(config.STORAGE_CONFIG, "STORAGE_BAND_DF_CUTOFF", 0))
    checkBandDfCutoff(knobs["band_df_cutoff"], config)
    if with_shortlist:
        knobs.setdefault("shortlist_size", getattr(minhash_config, "MINHASH_MATCHING_SHORTLIST_SIZE", 0))
        if storage is not None and knobs["shortlist_size"] > 0 and "shortlist_unavailable" not in knobs:
            reason = shortlistUnavailableReason(storage)
            if reason is not None:
                knobs["shortlist_unavailable"] = reason
        if knobs["shortlist_size"] <= 0:
            # nothing to fall back from
            knobs.pop("shortlist_unavailable", None)
    else:
        knobs.pop("shortlist_size", None)
        knobs.pop("shortlist_unavailable", None)
    resolved: Dict[str, Any] = {key: _canonical(knobs[key]) for key in MATCHING_KNOBS if key in knobs}
    resolved.update(others)
    return resolved
