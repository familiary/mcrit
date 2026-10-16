import re

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.utils import db_log_msg, get_username, getUniqueBlocksParams, jsonify, timing


class BlocksResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    @timing
    def on_get_unique_blocks_for_family(self, req, resp, family_id: int):
        """Schedule a job that finds the basic blocks unique to the family's samples; ``covers_required`` (k of the k-of-n cover) and ``min_instructions`` (drop shorter blocks). Answers the job id."""
        db_log_msg(self.index, req, "BlocksResource.on_get_unique_blocks_for_family")
        parameters = getUniqueBlocksParams(req.params)
        blocks_result = {}
        samples = self.index.getSamplesByFamilyId(family_id)
        target_sample_ids = [sample.sample_id for sample in samples]
        blocks_result = self.index.getUniqueBlocks(target_sample_ids, family_id=family_id, username=get_username(req), **parameters)
        resp.data = jsonify({"status": "successful", "data": blocks_result})

    @timing
    def on_get_unique_blocks_for_samples(self, req, resp, comma_separated_sample_ids=None):
        """Schedule a job that finds the basic blocks unique to the comma separated sample ids; parameters as for ``/uniqueblocks/family/{family_id}``. Answers the job id."""
        db_log_msg(self.index, req, "BlocksResource.on_get_unique_blocks")
        parameters = getUniqueBlocksParams(req.params)
        blocks_result = {}
        if comma_separated_sample_ids is not None and re.match(r"^\d+(?:[\s]*,[\s]*\d+)*$", comma_separated_sample_ids):
            target_sample_ids = [int(sample_id) for sample_id in comma_separated_sample_ids.split(",")]
            blocks_result = self.index.getUniqueBlocks(target_sample_ids, username=get_username(req), **parameters)
        resp.data = jsonify({"status": "successful", "data": blocks_result})
