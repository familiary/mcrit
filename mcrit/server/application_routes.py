import json
import logging
import os
import secrets

import falcon

from mcrit.config.McritConfig import McritConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.FamilyResource import FamilyResource

from .BlocksResource import BlocksResource
from .FunctionResource import FunctionResource
from .JobResource import JobResource
from .MatchResource import MatchResource
from .QueryResource import QueryResource
from .SampleResource import SampleResource
from .StatusResource import StatusResource

# Only do basicConfig if no handlers have been configured
if not logging.root.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
LOGGER = logging.getLogger(__name__)


# https://falcon.readthedocs.io/en/stable/user/quickstart.html#a-more-complex-example
class AuthMiddleware:
    def __init__(self):
        # warn here rather than in McritConfig, which is also constructed by every worker
        # process - an unprotected API is only a fact where the API is actually served
        if McritConfig.AUTH_TOKEN in [None, ""]:
            LOGGER.warning("No AUTH_TOKEN configured (MCRIT_AUTH_TOKEN is unset) - the API is not protected against unauthenticated access.")

    def process_request(self, req, resp):
        if McritConfig.AUTH_TOKEN in [None, ""]:
            return
        token = req.get_header("apitoken")

        if token is None:
            description = "Please provide an auth token as part of the request."

            raise falcon.HTTPUnauthorized(
                title="API token required",
                description=description,
            )

        if not self._token_is_valid(token):
            description = "The provided API token is not valid. Please request a new token and try again."

            raise falcon.HTTPUnauthorized(
                title="Authentication required",
                description=description,
            )

    def _token_is_valid(self, token):
        if McritConfig.AUTH_TOKEN in [None, ""]:
            return True
        # compare_digest refuses str inputs that are not pure ASCII, and WSGI hands header values
        # over latin-1 decoded - so a single high byte in the header would raise here and turn an
        # unauthorized request into a 500. Comparing bytes keeps it a plain mismatch.
        return secrets.compare_digest(token.encode("utf-8", "surrogateescape"), McritConfig.AUTH_TOKEN.encode("utf-8", "surrogateescape"))


def create_index():
    # TODO we will want to load config values and everyting from the database instead of initializing everytime here
    index = MinHashIndex()
    this_path = str(os.path.abspath(__file__))
    root_path = os.sep.join(this_path.split(os.sep)[:-2])
    index_path = os.sep.join([root_path, "db", "index.json"])
    index_json = None
    if os.path.isfile(index_path):
        LOGGER.info("Found index.json, loading and using data...")
        with open(index_path) as fjson:
            index_json = json.load(fjson)
        index.setStorageData(index_json)
    return index


def get_app():
    index = create_index()
    status_resource = StatusResource(index)
    family_resource = FamilyResource(index)
    sample_resource = SampleResource(index)
    function_resource = FunctionResource(index)
    match_resource = MatchResource(index)
    block_resource = BlocksResource(index)
    query_resource = QueryResource(index)
    job_resource = JobResource(index)

    _app = falcon.App(middleware=[AuthMiddleware()])
    _app.req_options.strip_url_path_trailing_slash = True
    _app.add_route("/", status_resource)
    _app.add_route("/status", status_resource, suffix="status")
    _app.add_route("/version", status_resource, suffix="version")
    _app.add_route("/config", status_resource, suffix="config")
    _app.add_route("/export", status_resource, suffix="export")
    _app.add_route("/export/{comma_separated_sample_ids}", status_resource, suffix="export_selection")
    # NOTE: adds to storage, does not replace storage
    _app.add_route("/import", status_resource, suffix="import")  # post
    # drops storage and sets up new empty instance
    _app.add_route("/respawn", status_resource, suffix="respawn")  # post
    # schedule a job that calculates all missing minhashes for all samples/functions, in case the respective jobs failed before
    _app.add_route("/complete_minhashes", status_resource, suffix="complete_minhashes")  # get
    # schedule a job that drops all minhash bands and rebuilds the index using all existing minhashes
    _app.add_route("/rebuild_index", status_resource, suffix="rebuild_index")  # get
    # schedule a job that sets every family's sample/function counters from the collections (#151)
    _app.add_route("/recompute_family_stats", status_resource, suffix="recompute_family_stats")  # post
    # schedule a job that deletes the GridFS files and chunks left behind by deleted jobs (#80)
    _app.add_route("/delete_orphaned_queue_files", status_resource, suffix="delete_orphaned_queue_files")  # post
    # schedule a job that updates all function_entries where the pichash was possibly calculated with an outdated SMDA version
    # schedule a job that rebuilds the inverted picblockhash index getUniqueBlocks reads.
    # An instance upgrading into this feature has no index and keeps using the old full scan until
    # this runs, so it is the one operator action the feature needs.
    _app.add_route("/rebuild_picblockhash_index", status_resource, suffix="rebuild_picblockhash_index")  # get
    _app.add_route("/rebuild_function_range_index", status_resource, suffix="rebuild_function_range_index")  # get
    _app.add_route("/rebuild_band_df_index", status_resource, suffix="rebuild_band_df_index")  # get
    _app.add_route("/recalculate_pichashes", status_resource, suffix="recalculate_pichashes")  # get
    # schedule a job that rehashes only the samples whose minhashes an older smda escaper produced (#142)
    _app.add_route("/repair_minhashes", status_resource, suffix="repair_minhashes")  # post
    # schedule a job that updates all function_entries where the minhash was possibly calculated with an outdated SMDA version
    _app.add_route("/recalculate_minhashes", status_resource, suffix="recalculate_minhashes")  # get

    # search suitable fields based on query
    _app.add_route("/search/families", status_resource, suffix="search_families")
    _app.add_route("/search/samples", status_resource, suffix="search_samples")
    _app.add_route("/search/functions", status_resource, suffix="search_functions")

    _app.add_route("/families", family_resource, suffix="collection")
    # post only: batch lookup, body is a comma separated list of family_ids
    _app.add_route("/families/ids", family_resource, suffix="by_ids")
    # supports GET, PUT (for modification of family_name, is_library), DELETE (for all samples)
    _app.add_route("/families/{family_id:int}", family_resource)

    # supports GET and POST (to insert one sample)
    _app.add_route("/samples", sample_resource, suffix="collection")
    # post only
    _app.add_route("/samples/binary", sample_resource, suffix="submit_binary")
    # post only: batch lookup, body is a comma separated list of sample_ids
    _app.add_route("/samples/ids", sample_resource, suffix="by_ids")
    # supports GET, PUT (for modification of family_name, version, component, is_library), DELETE (for one sample)
    _app.add_route("/samples/{sample_id:int}", sample_resource)
    #
    _app.add_route("/samples/sha256/{sample_sha256}", sample_resource, suffix="by_sha256")
    _app.add_route("/samples/{sample_id:int}/functions", sample_resource, suffix="functions")
    _app.add_route(
        "/samples/{sample_id:int}/functions/{function_id:int}",
        sample_resource,
        suffix="function",
    )

    _app.add_route("/functions", function_resource, suffix="collection")
    _app.add_route("/functions/{function_id:int}", function_resource)

    _app.add_route("/matches/sample/{sample_id:int}", match_resource, suffix="sample")
    _app.add_route("/matches/sample/cross/{sample_ids}", match_resource, suffix="sample_cross")
    _app.add_route("/matches/sample/{sample_id:int}/{sample_id_b:int}", match_resource, suffix="sample_vs")
    _app.add_route("/matches/function/{function_id:int}/{function_id_b:int}", match_resource, suffix="function_vs")
    _app.add_route("/matches/function/{function_id:int}", match_resource, suffix="function")

    _app.add_route("/uniqueblocks/samples/{comma_separated_sample_ids}", block_resource, suffix="unique_blocks_for_samples")
    _app.add_route("/uniqueblocks/family/{family_id:int}", block_resource, suffix="unique_blocks_for_family")

    _app.add_route("/query", query_resource, suffix="query_smda")
    _app.add_route("/query/binary", query_resource, suffix="query_binary")
    _app.add_route(
        "/query/binary/mapped/{base_address}",
        query_resource,
        suffix="query_binary_mapped",
    )
    _app.add_route("/query/function", query_resource, suffix="query_smda_function")
    _app.add_route("/query/pichash/{pichash}", query_resource, suffix="query_pichash")
    _app.add_route("/query/pichash/{pichash}/summary", query_resource, suffix="query_pichash_summary")
    _app.add_route("/query/picblockhash/{picblockhash}", query_resource, suffix="query_picblockhash")
    _app.add_route("/query/picblockhash/{picblockhash}/summary", query_resource, suffix="query_picblockhash_summary")

    _app.add_route("/jobs", job_resource, suffix="collection")
    _app.add_route("/jobs/stats", job_resource, suffix="stats")
    _app.add_route("/jobs/count", job_resource, suffix="count")
    _app.add_route("/jobs/{job_id}", job_resource)
    _app.add_route("/jobs/{job_id}/result", job_resource, suffix="job_result")
    _app.add_route("/results/{result_id}", job_resource, suffix="results")
    _app.add_route("/results/{result_id}/job", job_resource, suffix="result_job")

    return _app
