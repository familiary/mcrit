import json
import re

import falcon

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.utils import db_log_msg, get_username, jsonify, timing
from mcrit.storage.StorageInterface import BAND_DF_CUTOFF_MAX


class StatusResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    @timing
    def on_get(self, req, resp):
        resp.data = jsonify({"status": "successful", "data": {"message": "Welcome to MCRIT"}})
        db_log_msg(self.index, req, "StatusResource.on_get - success.")

    @timing
    def on_get_status(self, req, resp):
        with_pichash = True if "with_pichash" in req.params and req.params["with_pichash"].lower() == "true" else False
        resp.data = jsonify({"status": "successful", "data": self.index.getStatus(with_pichash=with_pichash)})
        db_log_msg(self.index, req, "StatusResource.on_get_status - success.")

    @timing
    def on_get_version(self, req, resp):
        resp.data = jsonify({"status": "successful", "data": self.index.getVersion()})
        db_log_msg(self.index, req, "StatusResource.on_get_version - success.")

    @timing
    def on_get_config(self, req, resp):
        resp.status = falcon.HTTP_NOT_IMPLEMENTED
        db_log_msg(self.index, req, "StatusResource.on_get_config - success / not implemented.")
        return

    @timing
    def on_get_export(self, req, resp):
        compress_data = True if "compress" in req.params and req.params["compress"].lower() == "true" else False
        try:
            exported_data = self.index.getExportData(compress_data=compress_data)
            resp.data = jsonify({"status": "successful", "data": exported_data})
            db_log_msg(self.index, req, "StatusResource.on_get_export - success.")
        except MemoryError:
            resp.data = jsonify({"status": "failed", "data": {"message": "Export exceeded assigned maximum memory limit and was cancelled."}})
            db_log_msg(self.index, req, "StatusResource.on_get_export - failed.")

    @timing
    def on_get_export_selection(self, req, resp, comma_separated_sample_ids=None):
        # NOTE if we encounter extreme cases (super long URLs), we might have to switch to post here.
        compress_data = True if "compress" in req.params and req.params["compress"].lower() == "true" else False
        exported_data = {}
        if comma_separated_sample_ids is not None and re.match(r"^\d+(?:[\s]*,[\s]*\d+)*$", comma_separated_sample_ids):
            target_sample_ids = [int(sample_id) for sample_id in comma_separated_sample_ids.split(",")]
            exported_data = self.index.getExportData(target_sample_ids, compress_data=compress_data)
        resp.data = jsonify({"status": "successful", "data": exported_data})
        db_log_msg(self.index, req, "StatusResource.on_get_export_selection - success.")

    @timing
    def on_post_import(self, req, resp):
        if not req.content_length:
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "POST request without body can't be processed."},
                }
            )
            resp.status = falcon.HTTP_400
            db_log_msg(self.index, req, "StatusResource.on_post_import - failed - no POST body.")
            return
        import_data = json.loads(req.stream.read())
        import_report = self.index.addImportData(import_data)
        resp.data = jsonify({"status": "successful", "data": import_report})
        db_log_msg(self.index, req, "StatusResource.on_post_import - success.")
        return

    @timing
    def on_post_respawn(self, req, resp):
        # this one has implicit "recalculation" because it nukes the whole DB
        self.index.respawn()
        resp.data = jsonify({"status": "successful", "data": {"message": "Successfully performed reset of MCRIT instance."}})
        db_log_msg(self.index, req, "StatusResource.on_post_respawn - success.")

    @timing
    def on_get_complete_minhashes(self, req, resp):
        minhash_report = self.index.updateMinHashes(None, force_recalculation=True, username=get_username(req))
        resp.data = jsonify({"status": "successful", "data": minhash_report})
        db_log_msg(self.index, req, "StatusResource.on_get_complete_minhashes - success.")
        return

    @timing
    def on_get_rebuild_index(self, req, resp):
        index_report = self.index.rebuildIndex(force_recalculation=True, username=get_username(req))
        resp.data = jsonify({"status": "successful", "data": index_report})
        db_log_msg(self.index, req, "StatusResource.on_get_rebuild_index - success.")
        return

    @timing
    def on_get_rebuild_picblockhash_index(self, req, resp):
        """Schedule a job that rebuilds the inverted picblockhash index getUniqueBlocks reads. Answers the job id."""
        index_report = self.index.rebuildPicBlockHashIndex(force_recalculation=True)
        resp.data = jsonify({"status": "successful", "data": index_report})
        db_log_msg(self.index, req, "StatusResource.on_get_rebuild_picblockhash_index - success.")
        return

    @timing
    def on_get_rebuild_function_range_index(self, req, resp):
        """Schedule a job that rebuilds the function->sample range index two-stage matching needs. Answers the job id."""
        index_report = self.index.rebuildFunctionRangeIndex(force_recalculation=True)
        resp.data = jsonify({"status": "successful", "data": index_report})
        db_log_msg(self.index, req, "StatusResource.on_get_rebuild_function_range_index - success.")
        return

    @timing
    def on_get_rebuild_band_df_index(self, req, resp):
        """Schedule a job that stores and indexes each band's posting-list length, so STORAGE_BAND_DF_CUTOFF can skip from the index. Answers the job id."""
        index_report = self.index.rebuildBandDfIndex(force_recalculation=True)
        resp.data = jsonify({"status": "successful", "data": index_report})
        db_log_msg(self.index, req, "StatusResource.on_get_rebuild_band_df_index - success.")
        return

    @timing
    def on_get_band_df_cutoff_coverage(self, req, resp):
        """Schedule a job that measures what STORAGE_BAND_DF_CUTOFF skips - band hashes and postings over the cutoff, per band and in total (#201). ``band_df_cutoff`` evaluates another cutoff than the configured one. Answers the job id."""
        band_df_cutoff = None
        if "band_df_cutoff" in req.params:
            # refused rather than ignored like a malformed matching parameter: silently measuring the
            # configured cutoff instead would answer a question that was not asked. The upper bound
            # is what a BSON integer holds; past it the job would fail on encoding the cutoff.
            try:
                band_df_cutoff = int(req.params["band_df_cutoff"])
            except (TypeError, ValueError):
                band_df_cutoff = -1
            if not 0 <= band_df_cutoff <= BAND_DF_CUTOFF_MAX:
                resp.status = falcon.HTTP_400
                resp.data = jsonify({"status": "failed", "data": {"message": f"band_df_cutoff must be an integer from 0 to {BAND_DF_CUTOFF_MAX}."}})
                db_log_msg(self.index, req, "StatusResource.on_get_band_df_cutoff_coverage - failed - invalid band_df_cutoff.")
                return
        job_id = self.index.getBandDfCutoffCoverage(band_df_cutoff=band_df_cutoff, force_recalculation=True, username=get_username(req))
        resp.data = jsonify({"status": "successful", "data": job_id})
        db_log_msg(self.index, req, "StatusResource.on_get_band_df_cutoff_coverage - success.")
        return

    def on_post_recompute_family_stats(self, req, resp):
        """Schedule a job that sets every family's sample, function and library counters from the collections, recreating missing family documents. Answers the job id."""
        job_id = self.index.recomputeFamilyStats(force_recalculation=True)
        resp.data = jsonify({"status": "successful", "data": job_id})
        db_log_msg(self.index, req, "StatusResource.on_post_recompute_family_stats - success.")
        return

    @timing
    def on_get_recalculate_pichashes(self, req, resp):
        index_report = self.index.recalculatePicHashes(force_recalculation=True, username=get_username(req))
        resp.data = jsonify({"status": "successful", "data": index_report})
        db_log_msg(self.index, req, "StatusResource.on_get_recalculate_pichashes - success.")
        return

    @timing
    def on_post_repair_minhashes(self, req, resp):
        """Schedule a job that rehashes only the samples whose minhashes an older smda escaper produced (see ``num_samples_with_stale_minhashes`` in the status). Answers the job id."""
        job_id = self.index.repairMinHashes(force_recalculation=True)
        resp.data = jsonify({"status": "successful", "data": job_id})
        db_log_msg(self.index, req, "StatusResource.on_post_repair_minhashes - success.")
        return

    @timing
    def on_get_recalculate_minhashes(self, req, resp):
        index_report = self.index.recalculateMinHashes(force_recalculation=True, username=get_username(req))
        resp.data = jsonify({"status": "successful", "data": index_report})
        db_log_msg(self.index, req, "StatusResource.on_get_recalculate_minhashes - success.")
        return

    @staticmethod
    def _get_search_args(params):
        result = {
            "search_term": params["query"],
            "cursor": params.get("cursor", None),
            "sort_by": params.get("sort_by", None),
            "is_ascending": params.get("is_ascending", "true").lower() != "false",
        }
        try:
            result["limit"] = int(params.get("limit"))
        except (TypeError, ValueError):
            pass
        return result

    def _respond_search(self, req, resp, search_method, endpoint):
        args = self._get_search_args(req.params)
        try:
            search_results = search_method(**args)
        except ValueError as unsupported_search:
            # a field/operator combination the backend cannot serve (e.g. a range comparison on
            # pichash) is a client error - it used to leave the responder as a 500 with a traceback
            resp.data = jsonify({"status": "failed", "data": {"message": str(unsupported_search)}})
            resp.status = falcon.HTTP_400
            db_log_msg(self.index, req, f"StatusResource.{endpoint} - failed - {unsupported_search}")
            return
        resp.data = jsonify({"status": "successful", "data": search_results})
        db_log_msg(self.index, req, f"StatusResource.{endpoint} - success.")

    @timing
    def on_get_search_families(self, req, resp):
        self._respond_search(req, resp, self.index.getFamilySearchResults, "on_get_search_families")

    @timing
    def on_get_search_samples(self, req, resp):
        self._respond_search(req, resp, self.index.getSampleSearchResults, "on_get_search_samples")

    @timing
    def on_get_search_functions(self, req, resp):
        self._respond_search(req, resp, self.index.getFunctionSearchResults, "on_get_search_functions")
