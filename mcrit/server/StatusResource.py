import json
import re

import falcon

from mcrit.index.MinHashIndex import ImportRejected, MinHashIndex
from mcrit.server.utils import db_log_msg, get_username, jsonify, timing
from mcrit.storage.StorageInterface import BAND_DF_CUTOFF_MAX


class StatusResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    @timing
    def on_get(self, req, resp):
        """Welcome message; a cheap reachability check for the server."""
        resp.data = jsonify({"status": "successful", "data": {"message": "Welcome to MCRIT"}})
        db_log_msg(self.index, req, "StatusResource.on_get - success.")

    @timing
    def on_get_status(self, req, resp):
        """Status and statistics of the instance: database state and timestamp, storage type, number of bands, samples, families, functions and (with ``with_pichash=true``, expensive) unique pichashes, the smda version and escaper fingerprint the instance hashes with."""
        with_pichash = True if "with_pichash" in req.params and req.params["with_pichash"].lower() == "true" else False
        resp.data = jsonify({"status": "successful", "data": self.index.getStatus(with_pichash=with_pichash)})
        db_log_msg(self.index, req, "StatusResource.on_get_status - success.")

    @timing
    def on_get_version(self, req, resp):
        """The version of the running mcrit server."""
        resp.data = jsonify({"status": "successful", "data": self.index.getVersion()})
        db_log_msg(self.index, req, "StatusResource.on_get_version - success.")

    @timing
    def on_get_config(self, req, resp):
        """Not implemented; answers 501."""
        resp.status = falcon.HTTP_NOT_IMPLEMENTED
        db_log_msg(self.index, req, "StatusResource.on_get_config - success / not implemented.")
        return

    def _respond_export(self, req, resp, responder, sample_ids=None):
        compress_data = True if "compress" in req.params and req.params["compress"].lower() == "true" else False
        try:
            export_json = self.index.getExportJson(sample_ids, compress_data=compress_data)
        except MemoryError:
            resp.data = jsonify({"status": "failed", "data": {"message": "Export exceeded assigned maximum memory limit and was cancelled."}})
            db_log_msg(self.index, req, f"StatusResource.{responder} - failed.")
            return
        # sent in the pieces it was built in, as joining them would hold the export's text a second time
        body = [b'{"status": "successful", "data": ', *export_json, b"}"]
        resp.content_length = sum(len(fragment) for fragment in body)
        resp.stream = iter(body)
        db_log_msg(self.index, req, f"StatusResource.{responder} - success.")

    @timing
    def on_get_export(self, req, resp):
        """Export every family, sample and function entry (with minhashes) of the instance for import into another one. ``compress=true`` compresses the function entries per sample."""
        self._respond_export(req, resp, "on_get_export")

    @timing
    def on_get_export_selection(self, req, resp, comma_separated_sample_ids=None):
        """Export only the samples with the given comma separated ids, in the same format as ``/export``."""
        # NOTE if we encounter extreme cases (super long URLs), we might have to switch to post here.
        if comma_separated_sample_ids is not None and re.match(r"^\d+(?:[\s]*,[\s]*\d+)*$", comma_separated_sample_ids):
            target_sample_ids = [int(sample_id) for sample_id in comma_separated_sample_ids.split(",")]
            self._respond_export(req, resp, "on_get_export_selection", target_sample_ids)
            return
        resp.data = jsonify({"status": "successful", "data": {}})
        db_log_msg(self.index, req, "StatusResource.on_get_export_selection - success.")

    @timing
    def on_post_import(self, req, resp):
        """Import the JSON body an export produced. Adds to the instance (ids are remapped, existing samples by sha256 are skipped); it does not replace it. Answers an import report."""
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
        try:
            body = req.stream.read()
            try:
                import_data = json.loads(body)
            except (ValueError, RecursionError) as error:
                # JSONDecodeError and UnicodeDecodeError are ValueErrors; a RecursionError is JSON nested too deeply
                raise ImportRejected(f"body is no JSON export ({type(error).__name__}: {error})") from error
            # what the export's content may inflate to is bounded by the size of the body (#300)
            import_report = self.index.addImportData(import_data, import_size=len(body))
        except ImportRejected as error:
            # a body that is no JSON, or an export of another version or configuration, or one that
            # cannot be read; any other error of an import, a failed insert say, is the server's
            resp.data = jsonify({"status": "failed", "data": {"message": f"Import rejected: {error}"}})
            resp.status = falcon.HTTP_400
            db_log_msg(self.index, req, "StatusResource.on_post_import - failed - import rejected.")
            return
        resp.data = jsonify({"status": "successful", "data": import_report})
        db_log_msg(self.index, req, "StatusResource.on_post_import - success.")
        return

    @timing
    def on_post_respawn(self, req, resp):
        """Drop the whole database and set up a fresh, empty instance."""
        # this one has implicit "recalculation" because it nukes the whole DB
        self.index.respawn()
        resp.data = jsonify({"status": "successful", "data": {"message": "Successfully performed reset of MCRIT instance."}})
        db_log_msg(self.index, req, "StatusResource.on_post_respawn - success.")

    @timing
    def on_get_complete_minhashes(self, req, resp):
        """Schedule a job that calculates every missing minhash - for samples submitted with deferred hashing, or whose hashing job failed earlier - and finishes the samples it completes. Answers the job id."""
        # finish_samples records the minhash version of what it hashed and drops its disassembly
        # where configured, as hashing on submission does; a separate method name would take these
        # jobs out of every listing that knows them as updateMinHashes, MCRITweb's job pages included
        minhash_report = self.index.updateMinHashes(None, finish_samples=True, force_recalculation=True, username=get_username(req))
        resp.data = jsonify({"status": "successful", "data": minhash_report})
        db_log_msg(self.index, req, "StatusResource.on_get_complete_minhashes - success.")
        return

    @timing
    def on_get_rebuild_index(self, req, resp):
        """Schedule a job that drops the band index and rebuilds it from the stored minhashes. Answers the job id."""
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
    def on_post_delete_orphaned_queue_files(self, req, resp):
        """Schedule a job that deletes the GridFS files and chunks no job refers to any more; with ``dry_run=true`` it only counts them. Answers the job id."""
        # required, and strictly true or false: a missing, repeated or misspelt value is refused
        # rather than taken as a real, destructive run
        dry_run = req.params.get("dry_run")
        dry_run = dry_run.lower().strip() if isinstance(dry_run, str) else None
        if dry_run not in ("true", "false"):
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "dry_run=true or dry_run=false has to be given in the query string."}})
            db_log_msg(self.index, req, "StatusResource.on_post_delete_orphaned_queue_files - failed - invalid dry_run.")
            return
        job_id = self.index.deleteOrphanedQueueFiles(dry_run == "true", force_recalculation=True)
        resp.data = jsonify({"status": "successful", "data": job_id})
        db_log_msg(self.index, req, "StatusResource.on_post_delete_orphaned_queue_files - success.")

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
        """Schedule a job that recalculates the pichashes of every sample hashed with an older smda. Answers the job id."""
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
        """Schedule a job that drops every minhash and recalculates all of them. Answers the job id."""
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
        try:
            args = self._get_search_args(req.params)
            search_results = search_method(**args)
        except ValueError as unsupported_search:
            # a field/operator combination the backend cannot serve (e.g. a range comparison on
            # pichash), a value the field can't hold, or a query the parser can't take apart is a
            # client error - it used to leave the responder as a 500 with a traceback
            message = str(unsupported_search)
            resp.data = jsonify({"status": "failed", "data": {"message": message}})
            resp.status = falcon.HTTP_400
            db_log_msg(self.index, req, f"StatusResource.{endpoint} - failed - {message}")
            return
        resp.data = jsonify({"status": "successful", "data": search_results})
        db_log_msg(self.index, req, f"StatusResource.{endpoint} - success.")

    @timing
    def on_get_search_families(self, req, resp):
        """Search families by name. Query parameters: ``query`` (search term, e.g. ``name:?emotet``), ``sort_by``, ``is_ascending`` (default true), ``limit``, ``cursor`` (forward/backward cursor of a previous page). Answers ``search_results`` keyed by id, a ``cursor`` pair and an ``id_match`` when the term is an id. A field/operator combination the backend cannot serve, a value the field cannot hold, an unusable cursor or a query the parser cannot take apart answers 400."""
        self._respond_search(req, resp, self.index.getFamilySearchResults, "on_get_search_families")

    @timing
    def on_get_search_samples(self, req, resp):
        """Search samples by filename, family, component, version or sha256 (3+ chars); parameters and answer as for ``/search/families``."""
        self._respond_search(req, resp, self.index.getSampleSearchResults, "on_get_search_samples")

    @timing
    def on_get_search_functions(self, req, resp):
        """Search functions by name (``pichash:``, ``offset:`` and the id/count fields support comparison operators); parameters and answer as for ``/search/families``."""
        self._respond_search(req, resp, self.index.getFunctionSearchResults, "on_get_search_functions")
