import re

import falcon

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.utils import db_log_msg, get_username, getExcludedPicBlockHashes, getUniqueBlocksParams, jsonify, timing


class BlocksResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    def _postParams(self, req, resp, handler):
        """A POST's query parameters and the negative set from its body (#283), or None after answering a 400."""
        parameters = getUniqueBlocksParams(req.params)
        try:
            excluded = getExcludedPicBlockHashes(req.get_media(default_when_empty=None))
        except ValueError as error:
            message = str(error)
        # json.loads raises RecursionError, unwrapped by falcon, for a deeply nested body
        except (falcon.MediaMalformedError, falcon.HTTPUnsupportedMediaType, RecursionError):
            message = "the body must be JSON"
        else:
            # an empty set excludes nothing and makes the same job as a GET
            if excluded:
                parameters["exclude_picblockhashes"] = excluded
            return parameters
        resp.status = falcon.HTTP_400
        resp.data = jsonify({"status": "failed", "data": {"message": message}})
        db_log_msg(self.index, req, f"BlocksResource.{handler} - failed - {message}")
        return None

    def _uniqueBlocksForFamily(self, req, resp, family_id, parameters):
        samples = self.index.getSamplesByFamilyId(family_id)
        target_sample_ids = [sample.sample_id for sample in samples]
        blocks_result = self.index.getUniqueBlocks(target_sample_ids, family_id=family_id, username=get_username(req), **parameters)
        resp.data = jsonify({"status": "successful", "data": blocks_result})

    def _uniqueBlocksForSamples(self, req, resp, comma_separated_sample_ids, parameters):
        blocks_result = {}
        if comma_separated_sample_ids is not None and re.match(r"^\d+(?:[\s]*,[\s]*\d+)*$", comma_separated_sample_ids):
            target_sample_ids = [int(sample_id) for sample_id in comma_separated_sample_ids.split(",")]
            blocks_result = self.index.getUniqueBlocks(target_sample_ids, username=get_username(req), **parameters)
        resp.data = jsonify({"status": "successful", "data": blocks_result})

    @timing
    def on_get_unique_blocks_for_family(self, req, resp, family_id: int):
        db_log_msg(self.index, req, "BlocksResource.on_get_unique_blocks_for_family")
        self._uniqueBlocksForFamily(req, resp, family_id, getUniqueBlocksParams(req.params))

    @timing
    def on_get_unique_blocks_for_samples(self, req, resp, comma_separated_sample_ids=None):
        db_log_msg(self.index, req, "BlocksResource.on_get_unique_blocks")
        self._uniqueBlocksForSamples(req, resp, comma_separated_sample_ids, getUniqueBlocksParams(req.params))

    # POST takes the same query parameters, and a negative set in its body that would not fit a URL

    @timing
    def on_post_unique_blocks_for_family(self, req, resp, family_id: int):
        db_log_msg(self.index, req, "BlocksResource.on_post_unique_blocks_for_family")
        parameters = self._postParams(req, resp, "on_post_unique_blocks_for_family")
        if parameters is not None:
            self._uniqueBlocksForFamily(req, resp, family_id, parameters)

    @timing
    def on_post_unique_blocks_for_samples(self, req, resp, comma_separated_sample_ids=None):
        db_log_msg(self.index, req, "BlocksResource.on_post_unique_blocks")
        parameters = self._postParams(req, resp, "on_post_unique_blocks")
        if parameters is not None:
            self._uniqueBlocksForSamples(req, resp, comma_separated_sample_ids, parameters)
