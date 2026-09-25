import re

import falcon

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.utils import db_log_msg, get_username, jsonify, timing


class FunctionResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    @timing
    def on_get(self, req, resp, function_id=None):
        """One function by id, with its disassembly when ``with_xcfg=true``. Unknown ids answer 404."""
        query_with_xcfg = False
        if "with_xcfg" in req.params:
            query_with_xcfg = req.params["with_xcfg"].lower().strip() == "true"
        if not self.index.isFunctionId(function_id):
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "We don't have a function with that id."},
                }
            )
            resp.status = falcon.HTTP_404
            db_log_msg(self.index, req, f"FunctionResource.on_get - failed - {function_id} unknown.")
            return
        data = self.index.getFunctionById(function_id, with_xcfg=query_with_xcfg).toDict()
        resp.data = jsonify(
            {
                "status": "successful",
                "data": data,
            }
        )
        db_log_msg(self.index, req, f"FunctionResource.on_get - success - {function_id}.")

    @timing
    def on_put(self, req, resp, function_id=None):
        """Rename a function; JSON body with ``function_name`` (up to 256 printable chars). The name is recorded as a label by the requesting user. Answers 200 on success (the rename is synchronous, not a job), 400 for a malformed body, 404 for an unknown id."""
        resp.status = falcon.HTTP_400
        if not req.content_length or not isinstance(req.media, dict):
            resp.data = jsonify({"status": "failed", "data": {"message": "PUT request without body can't be processed."}})
            db_log_msg(self.index, req, "FunctionResource.on_put - failed - no PUT body.")
            return
        information_update = req.media
        if "function_name" not in information_update:
            resp.data = jsonify({"status": "failed", "data": {"message": "Nothing to modify: function_name is the only modifiable field."}})
            db_log_msg(self.index, req, "FunctionResource.on_put - failed - nothing to modify.")
            return
        if not isinstance(information_update["function_name"], str) or not re.match(r"^[ -~]{0,256}$", information_update["function_name"]):
            resp.data = jsonify({"status": "failed", "data": {"message": "function_name may be 0-256 printable ASCII characters."}})
            db_log_msg(self.index, req, "FunctionResource.on_put - failed - invalid function name.")
            return
        if function_id is None or function_id < 0 or not self.index.isFunctionId(function_id):
            resp.data = jsonify({"status": "failed", "data": {"message": "We don't have a function with that id."}})
            resp.status = falcon.HTTP_404
            db_log_msg(self.index, req, f"FunctionResource.on_put - failed - {function_id} unknown.")
            return
        successful = self.index.modifyFunction(function_id, {"function_name": information_update["function_name"].strip()}, username=get_username(req))
        if successful:
            resp.data = jsonify({"status": "successful", "data": {"message": "Function modified."}})
            resp.status = falcon.HTTP_200
            db_log_msg(self.index, req, f"FunctionResource.on_put - success - modified function_id {function_id}.")
        else:
            resp.data = jsonify({"status": "failed", "data": {"message": "Failed to modify function."}})
            db_log_msg(self.index, req, f"FunctionResource.on_put - failed - function_id {function_id} not modified.")

    @timing
    def on_post_collection(self, req, resp):
        """The functions with the comma separated ids given in the request body, keyed by id; with ``with_label_only=true`` only those carrying a label."""
        if not req.content_length:
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "POST request without body can't be processed."},
                }
            )
            resp.status = falcon.HTTP_400
            db_log_msg(self.index, req, "FunctionResource.on_post - failed - no POST body.")
            return
        with_label_only = False
        if "with_label_only" in req.params:
            with_label_only = req.params["with_label_only"].lower().strip() == "true"
        # assume the POST body consists of comma separated function_ids
        post_body = req.stream.read()
        if re.match(rb"^\d+(?:[\s]*,[\s]*\d+)*$", post_body):
            target_function_ids = [int(function_id) for function_id in post_body.split(b",")]
            function_entries = {}
            for function_id in target_function_ids:
                function_entry = self.index.getFunctionById(function_id, with_xcfg=False)
                if function_entry:
                    function_dict = function_entry.toDict()
                    if with_label_only and not function_dict["function_labels"]:
                        continue
                    function_entries[function_id] = function_dict
            resp.data = jsonify({"status": "successful", "data": function_entries})
            resp.status = falcon.HTTP_200
            db_log_msg(self.index, req, "FunctionResource.on_post - success.")
            return
        resp.status = falcon.HTTP_400
        db_log_msg(self.index, req, "FunctionResource.on_post - failed - invalid body format.")

    @timing
    def on_get_collection(self, req, resp):
        """All functions keyed by id; optional ``start`` (first function id) and ``limit``."""
        # parse optional request parameters
        start_index = 0
        if "start" in req.params:
            try:
                start_index = int(req.params["start"])
            except ValueError:
                pass
        limit_function_count = 0
        if "limit" in req.params:
            try:
                limit_function_count = int(req.params["limit"])
            except ValueError:
                pass
        function_overview = {}
        function_entries = self.index.getFunctions(start_index, limit_function_count)
        for function_entry in function_entries:
            function_overview[function_entry.function_id] = function_entry.toDict()
        resp.data = jsonify({"status": "successful", "data": function_overview})
        db_log_msg(self.index, req, "FunctionResource.on_get_collection - success.")
