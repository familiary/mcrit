import datetime
import json
import re
from typing import Optional

import falcon

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.queue.LocalQueue import Job
from mcrit.server.utils import db_log_msg, jsonify, timing


def _csv_items(value):
    """The comma-separated items of a query parameter, which falcon hands over as a list when the
    parameter is repeated (``?job_ids=a&job_ids=b``; see REPEATABLE_PARAMETERS); a repeat reads as
    its comma-joined form."""
    parts = value if isinstance(value, list) else [value]
    return [item.strip() for part in parts for item in part.split(",")]


def _normalizeObjectId(value: Optional[str]) -> Optional[str]:
    """A job or result id as both queues store it - an ObjectId in lower-case hex - or None if it is not one."""
    if value is None or re.fullmatch("[0-9a-fA-F]{24}", value) is None:
        return None
    return value.lower()


def _parse_int_csv(value):
    """Comma-separated ints; entries that do not parse are ignored, like start/limit."""
    ids = []
    for item in _csv_items(value):
        if not item:
            continue
        try:
            ids.append(int(item))
        except ValueError:
            pass
    return ids


def _parse_str_csv(value):
    """Comma-separated ids; blank entries are ignored."""
    return [item for item in _csv_items(value) if item]


def _parseJobDate(value) -> Optional[datetime.datetime]:
    # a repeated parameter arrives as a list, which is no date either
    if not isinstance(value, str):
        return None
    for date_format in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.datetime.strptime(value, date_format)
        except ValueError:
            continue
    return None


class JobResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    def _not_found(self, req, resp, route: str, message: str, reason: str):
        resp.status = falcon.HTTP_404
        resp.data = jsonify({"status": "failed", "data": {"message": message}})
        db_log_msg(self.index, req, f"JobResource.{route} - failed - {reason}.")

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
        query_with_refresh = False
        if "with_refresh" in req.params:
            query_with_refresh = req.params["with_refresh"].lower().strip() == "true"
        queue_data = self.index.getQueueStats(refresh=query_with_refresh)
        resp.data = jsonify({"status": "successful", "data": queue_data})
        db_log_msg(self.index, req, "JobResource.on_get_stats - success.")

    @timing
    def on_delete_collection(self, req, resp):
        # parse optional request parameters, to be used as an "AND" query
        method_filter = None
        if "method" in req.params:
            method_filter = req.params["method"]
        # a date that doesn't parse used to be dropped, and with it the only thing keeping the
        # request from deleting every job and stored result
        dates = {}
        for name in ("created_before", "finished_before"):
            dates[name] = None
            if name in req.params:
                dates[name] = _parseJobDate(req.params[name])
                if dates[name] is None:
                    resp.status = falcon.HTTP_400
                    resp.data = jsonify({"status": "failed", "data": {"message": f"{name} has to be a date, YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS."}})
                    db_log_msg(self.index, req, f"JobResource.on_delete_collection - failed - invalid {name}.")
                    return
        created_before, finished_before = dates["created_before"], dates["finished_before"]
        # newest first
        result = self.index.deleteQueueData(method=method_filter, created_before=created_before, finished_before=finished_before)
        resp.data = jsonify({"status": "successful", "data": {"num_deleted": result}})
        db_log_msg(self.index, req, "JobResource.on_delete_collection - success.")

    @timing
    def on_get(self, req, resp, job_id=None):
        job_id = _normalizeObjectId(job_id)
        if job_id is None:
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid JobIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_get - failed - invalid job_id.")
            return
        data = self.index.getJobData(job_id)
        if data is None:
            self._not_found(req, resp, "on_get", "We don't have a job with that id.", f"unknown job_id: {job_id}")
            return
        resp.data = jsonify({"status": "successful", "data": data})
        db_log_msg(self.index, req, "JobResource.on_get - success.")

    @timing
    def on_delete(self, req, resp, job_id=None):
        job_id = _normalizeObjectId(job_id)
        if job_id is None:
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid JobIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_delete - failed - invalid job_id.")
            return
        result = self.index.deleteJob(job_id)
        if result == 0:
            self._not_found(req, resp, "on_delete", "We don't have a job with that id.", f"unknown job_id: {job_id}")
            return
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
        result_id = _normalizeObjectId(result_id)
        if result_id is None:
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid ResultIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_get_results - failed - invalid result_id.")
            return
        # the bytes also tell an unknown result from a stored JSON null, which getResult cannot
        result_bytes = self.index.getResultBytes(result_id)
        if result_bytes is None:
            self._not_found(req, resp, "on_get_results", "We don't have a result with that id.", f"unknown result_id: {result_id}")
            return
        compact = self._wants_compact(req)
        job_data = None
        if compact:
            # only the compact check needs the job; a result can outlive it, and is then served in full
            job_id = self.index.getJobIdForResult(result_id)
            job_data = self.index.getJobData(job_id) if job_id is not None else None
        self._respond_with_result(resp, job_data, compact, result_bytes, lambda: json.loads(result_bytes))
        db_log_msg(self.index, req, "JobResource.on_get_results - success.")

    @timing
    def on_get_job_result(self, req, resp, job_id=None):
        job_id = _normalizeObjectId(job_id)
        if job_id is None:
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid JobIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_get_job_result - failed - invalid job_id.")
            return
        job_data = self.index.getJobData(job_id)
        if job_data is None:
            self._not_found(req, resp, "on_get_job_result", "We don't have a job with that id.", f"unknown job_id: {job_id}")
            return
        compact = self._wants_compact(req)
        self._respond_with_result(resp, job_data, compact, None if compact else self.index.getResultBytesForJob(job_id), lambda: self.index.getResultForJob(job_id))
        db_log_msg(self.index, req, "JobResource.on_get_job_result - success.")

    @timing
    def on_get_result_job(self, req, resp, result_id=None):
        result_id = _normalizeObjectId(result_id)
        if result_id is None:
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "Valid ResultIDs are hexstrings with 24 characters."}})
            db_log_msg(self.index, req, "JobResource.on_get_result_job - failed - invalid result_id.")
            return
        job_id = self.index.getJobIdForResult(result_id)
        data = self.index.getJobData(job_id) if job_id is not None else None
        if data is None:
            # every stored result names its job, so a job id without job data means the job is gone
            if job_id is None:
                self._not_found(req, resp, "on_get_result_job", "We don't have a result with that id.", f"unknown result_id: {result_id}")
            else:
                self._not_found(req, resp, "on_get_result_job", "We don't have the job for that result id.", f"no job for result_id: {result_id}")
            return
        resp.data = jsonify({"status": "successful", "data": data})
        db_log_msg(self.index, req, "JobResource.on_get_result_job - success.")
