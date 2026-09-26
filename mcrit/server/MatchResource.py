import falcon

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.utils import db_log_msg, get_username, jsonify, readMatchingParams, timing


class MatchResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    @timing
    def on_get_sample(self, req, resp, sample_id=None):
        parameters = readMatchingParams(self.index, req, resp, "MatchResource.on_get_sample")
        if parameters is None:
            return
        if not self.index.isSampleId(sample_id):
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "We don't have a sample with that id."},
                }
            )
            resp.status = falcon.HTTP_404
            db_log_msg(self.index, req, "MatchResource.on_get_sample - failed - unknown sample_id.")
            return

        sample_matches = self.index.getMatchesForSample(sample_id, username=get_username(req), **parameters)
        resp.data = jsonify({"status": "successful", "data": sample_matches})
        db_log_msg(self.index, req, "MatchResource.on_get_sample - success.")
        # resp.status = falcon.HTTP_202
        # resp.location = "/matches/123"
        # resp.retry_after = 2

    @timing
    def on_get_sample_cross(self, req, resp, sample_ids=None):
        # a cross compare takes no shortlist: it could only drop the samples it names
        parameters = readMatchingParams(self.index, req, resp, "MatchResource.on_get_sample_cross", with_shortlist=False)
        if parameters is None:
            return
        if sample_ids is None:
            resp.status = falcon.HTTP_400
            resp.data = jsonify({"status": "failed", "data": {"message": "No sample_ids provided."}})
            db_log_msg(self.index, req, "MatchResource.on_get_sample_cross - failed - no sample_ids.")
            return
        sample_ids_list = [int(id) for id in sample_ids.split(",")]
        cross_matches = self.index.getMatchesCross(sample_ids_list, username=get_username(req), **parameters)
        resp.data = jsonify({"status": "successful", "data": cross_matches})
        db_log_msg(self.index, req, "MatchResource.on_get_sample_cross - success.")

    @timing
    def on_get_sample_vs(self, req, resp, sample_id=None, sample_id_b=None):
        # NOTE: We don't need to check if the kw parameters are None. The routing ensures that they are always set.
        parameters = readMatchingParams(self.index, req, resp, "MatchResource.on_get_sample_vs", with_shortlist=False)
        if parameters is None:
            return
        if not self.index.isSampleId(sample_id) or not self.index.isSampleId(sample_id_b):
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "We don't have a sample with that id."},
                }
            )
            resp.status = falcon.HTTP_404
            db_log_msg(self.index, req, "MatchResource.on_get_sample_vs - failed - unknown sample_id.")
            return
        sample_matches = self.index.getMatchesForSampleVs(sample_id, sample_id_b, username=get_username(req), **parameters)
        resp.data = jsonify({"status": "successful", "data": sample_matches})
        db_log_msg(self.index, req, "MatchResource.on_get_sample_vs - success.")

    @timing
    def on_get_function(self, req, resp, function_id=None):
        resp.status = falcon.HTTP_NOT_IMPLEMENTED
        return

    @timing
    def on_get_function_vs(self, req, resp, function_id=None, function_id_b=None):
        if function_id is None or function_id_b is None or not self.index.isFunctionId(function_id) or not self.index.isFunctionId(function_id_b):
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "We don't have a sample with that id."},
                }
            )
            resp.status = falcon.HTTP_404
            db_log_msg(self.index, req, "MatchResource.on_get_function_vs - failed - at least one unknown function_id.")
            return
        function_match = self.index.getMatchesFunctionVs(function_id, function_id_b)
        resp.data = jsonify({"status": "successful", "data": function_match})
        db_log_msg(self.index, req, "MatchResource.on_get_function_vs - success.")
