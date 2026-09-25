import datetime
import re

import falcon

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.queue.LocalQueue import Job
from mcrit.server.utils import db_log_msg, jsonify, timing

# TODO these should also return status and data in their json response


def _parse_int_csv(value):
    """Comma-separated ints; entries that do not parse are ignored, like start/limit."""
    ids = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.append(int(item))
        except ValueError:
            pass
    return ids


def _parse_str_csv(value):
    """Comma-separated ids; blank entries are ignored."""
    return [item.strip() for item in value.split(",") if item.strip()]


class JobResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    @staticmethod
    def _selection(req):
        """The parameters that select jobs, shared by the listing and its count."""
        return {
            "method": req.params.get("method", None),
            "state": req.params.get("state", None),
            "filter": req.params.get("filter", None),
            "username": req.params.get("username", None),
            "sample_ids": _parse_int_csv(req.params["sample_ids"]) if "sample_ids" in req.params else None,
            "job_ids": _parse_str_csv(req.params["job_ids"]) if "job_ids" in req.params else None,
        }

    def _reject_sample_ids_without_method(self, req, resp, selection) -> bool:
        if selection["sample_ids"] is None or selection["method"] is not None:
            return False
        resp.status = falcon.HTTP_400
        resp.data = jsonify({"status": "failed", "data": {"message": "sample_ids requires method to be set as well."}})
        db_log_msg(self.index, req, "JobResource - failed - sample_ids without method.")
        return True

    @timing
    def on_get_count(self, req, resp):
        """How many jobs match the same selection ``GET /jobs`` takes, without paging through them. Answers ``count``."""
        selection = self._selection(req)
        if self._reject_sample_ids_without_method(req, resp, selection):
            return
        count = self.index.getQueueCount(**selection)
        resp.data = jsonify({"status": "successful", "data": {"count": count}})
        db_log_msg(self.index, req, "JobResource.on_get_count - success.")

    @timing
    def on_get_collection(self, req, resp):
        """The queued jobs, newest first unless ``ascending=true``; ``start``, ``limit``, and the filters ``method`` (job method name), ``state``, ``filter`` (substring of the job descriptor), ``username`` (who requested the job), ``sample_ids`` (comma-separated; jobs of ``method``, which it requires, by their first argument, else a 400) and ``job_ids`` (comma-separated)."""
        # parse optional request parameters
        ascending = False
        if "ascending" in req.params:
            ascending = req.params["ascending"].lower().strip() == "true"
        selection = self._selection(req)
        if self._reject_sample_ids_without_method(req, resp, selection):
            return
        start_job_id = 0
        if "start" in req.params:
            try:
                start_job_id = int(req.params["start"])
            except ValueError:
                pass
        limit_job_count = 0
        if "limit" in req.params:
            try:
                limit_job_count = int(req.params["limit"])
            except ValueError:
                pass
        queue_data = self.index.getQueueData(start_index=start_job_id, limit=limit_job_count, ascending=ascending, **selection)
        resp.data = jsonify({"status": "successful", "data": queue_data})
        db_log_msg(self.index, req, "JobResource.on_get_collection - success.")

    @timing
    def on_get_stats(self, req, resp):
        """Queue statistics per method and state; ``with_refresh=true`` recounts instead of answering the cached numbers."""
        query_with_refresh = False
        if "with_refresh" in req.params:
            query_with_refresh = req.params["with_refresh"].lower().strip() == "true"
        queue_data = self.index.getQueueStats(refresh=query_with_refresh)
        resp.data = jsonify({"status": "successful", "data": queue_data})
        db_log_msg(self.index, req, "JobResource.on_get_stats - success.")

    @timing
    def on_delete_collection(self, req, resp):
        """Delete jobs matching all given filters: ``method``, ``created_before`` and ``finished_before`` (``YYYY-MM-DD`` or ``YYYY-MM-DDTHH:MM:SS``). Answers ``num_deleted``."""
        # parse optional request parameters, to be used as an "AND" query
        method_filter = None
        if "method" in req.params:
            method_filter = req.params["method"]
        created_before = None
        if "created_before" in req.params:
            try:
                if len(req.params["created_before"]) == 10:
                    created_before = datetime.datetime.strptime(req.params["created_before"], "%Y-%m-%d")
                else:
                    created_before = datetime.datetime.strptime(req.params["created_before"], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
        finished_before = None
        if "finished_before" in req.params:
            try:
                if len(req.params["finished_before"]) == 10:
                    finished_before = datetime.datetime.strptime(req.params["finished_before"], "%Y-%m-%d")
                else:
                    finished_before = datetime.datetime.strptime(req.params["finished_before"], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
        # newest first
        result = self.index.deleteQueueData(method=method_filter, created_before=created_before, finished_before=finished_before)
        resp.data = jsonify({"status": "successful", "data": {"num_deleted": result}})
        db_log_msg(self.index, req, "JobResource.on_delete_collection - success.")

    @timing
    def on_get(self, req, resp, job_id=None):
        """One job by its 24 hex digit id. Malformed ids answer 400, unknown ones 404."""
        # validate that we only allow hexstrings with 24 chars
        if job_id is None or not re.match("[a-fA-F0-9]{24}", job_id):
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid JobIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_get - failed - invalid job_id.")
            return
        data = self.index.getJobData(job_id)
        # TODO throw 404 if job_id is unknown
        # resp.status = falcon.HTTP_404
        resp.data = jsonify({"status": "successful", "data": data})
        db_log_msg(self.index, req, "JobResource.on_get - success.")

    @timing
    def on_delete(self, req, resp, job_id=None):
        """Delete one job (and its result) by id."""
        # validate that we only allow hexstrings with 24 chars
        if job_id is None or not re.match("[a-fA-F0-9]{24}", job_id):
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid JobIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_delete - failed - invalid job_id.")
            return
        result = self.index.deleteJob(job_id)
        # TODO throw 404 if job_id is unknown
        # resp.status = falcon.HTTP_404
        resp.data = jsonify({"status": "successful", "data": {"num_deleted": result}})
        db_log_msg(self.index, req, "JobResource.on_delete - success.")

    @staticmethod
    def _wants_compact(req):
        return "compact" in req.params and req.params["compact"].lower().strip() == "true"

    def _respond_with_result(self, resp, job_data, compact, result_bytes, parse_result):
        """Answer a stored result inside the success envelope.

        The result is stored as JSON, so unless the caller wants the compact form (which
        drops the function matches and therefore needs the parsed dict) the stored bytes go
        into the envelope as they are, instead of being parsed and serialised again - which
        cost more than reading the report for an 8 MB result (#152).
        """
        if compact:
            data = parse_result()
            if job_data and data is not None:
                job_info = Job(job_data, None)
                if job_info.is_matching_job or job_info.is_query_job:
                    data["matches"].pop("functions")
            resp.data = jsonify({"status": "successful", "data": data})
            return
        if result_bytes is None:
            resp.data = jsonify({"status": "successful", "data": None})
            return
        resp.data = b'{"status": "successful", "data": ' + result_bytes + b"}"

    @timing
    def on_get_results(self, req, resp, result_id=None):
        """The result stored under a 24 hex digit result id; ``compact=true`` strips the per-function matches."""
        # validate that we only allow hexstrings with 24 chars
        if result_id is None or not re.match("[a-fA-F0-9]{24}", result_id):
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid ResultIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_get_results - failed - invalid result_id.")
            return
        job_id = self.index.getJobIdForResult(result_id)
        job_data = self.index.getJobData(job_id)
        compact = self._wants_compact(req)
        # TODO throw 404 if job_id is unknown
        # resp.status = falcon.HTTP_404
        self._respond_with_result(resp, job_data, compact, None if compact else self.index.getResultBytes(result_id), lambda: self.index.getResult(result_id))
        db_log_msg(self.index, req, "JobResource.on_get_results - success.")

    @timing
    def on_get_job_result(self, req, resp, job_id=None):
        """The result of a job by job id, or null while it is not finished; ``compact=true`` strips the per-function matches."""
        # validate that we only allow hexstrings with 24 chars
        if job_id is None or not re.match("[a-fA-F0-9]{24}", job_id):
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid JobIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_get_job_result - failed - invalid job_id.")
            return
        job_data = self.index.getJobData(job_id)
        compact = self._wants_compact(req)
        # TODO throw 404 if job_id is unknown
        # resp.status = falcon.HTTP_404
        self._respond_with_result(resp, job_data, compact, None if compact else self.index.getResultBytesForJob(job_id), lambda: self.index.getResultForJob(job_id))
        db_log_msg(self.index, req, "JobResource.on_get_job_result - success.")

    @timing
    def on_get_result_job(self, req, resp, result_id=None):
        """The job that produced the result with the given id."""
        # validate that we only allow hexstrings with 24 chars
        if result_id is None or not re.match("[a-fA-F0-9]{24}", result_id):
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid ResultIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_get_job_result - failed - invalid result_id.")
            return
        job_id = self.index.getJobIdForResult(result_id)
        data = self.index.getJobData(job_id)
        # TODO throw 404 if job_id is unknown
        # resp.status = falcon.HTTP_404
        resp.data = jsonify({"status": "successful", "data": data})
        db_log_msg(self.index, req, "JobResource.on_get_result_job - success.")
