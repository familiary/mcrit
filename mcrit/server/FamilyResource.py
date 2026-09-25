import re

import falcon

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.server.utils import db_log_msg, get_username, jsonify, timing


class FamilyResource:
    def __init__(self, index: MinHashIndex):
        self.index = index

    @timing
    def on_get(self, req, resp, family_id=None):
        """One family by id, with its samples unless ``with_samples=false``. Unknown ids answer 404."""
        # parse optional request parameters
        with_samples = True
        if "with_samples" in req.params:
            with_samples = req.params["with_samples"].lower().strip() == "true"
        if self.index.getFamily(family_id) is None:
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "We don't have a family with that id."},
                }
            )
            resp.status = falcon.HTTP_404
            db_log_msg(self.index, req, f"FamilyResource.on_get - failed - family_id {family_id} unknown")
            return

        family = self.index.getFamily(family_id)
        if with_samples:
            samples = self.index.getSamplesByFamilyId(family_id)
            family.samples = {}
            for sample in samples:
                family.samples[sample.sample_id] = sample
        result = family.toDict()
        resp.data = jsonify({"status": "successful", "data": result})
        db_log_msg(self.index, req, f"FamilyResource.on_get - success - family_id {family_id}")

    @timing
    def on_post_by_ids(self, req, resp):
        """The families with the comma-separated ids in the body, keyed by family id, without their sample lists; ids that are not found are left out."""
        if not req.content_length:
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "POST request without body can't be processed."},
                }
            )
            resp.status = falcon.HTTP_400
            db_log_msg(self.index, req, "FamilyResource.on_post_by_ids - failed - no POST body.")
            return
        # assume the POST body consists of comma separated family_ids
        post_body = req.stream.read()
        if re.match(rb"^\d+(?:[\s]*,[\s]*\d+)*$", post_body):
            target_family_ids = [int(family_id) for family_id in post_body.split(b",")]
            family_entries = self.index.getFamiliesByIds(target_family_ids)
            data = {family_id: family_entry.toDict() for family_id, family_entry in family_entries.items()}
            resp.data = jsonify({"status": "successful", "data": data})
            resp.status = falcon.HTTP_200
            db_log_msg(self.index, req, "FamilyResource.on_post_by_ids - success.")
            return
        resp.status = falcon.HTTP_400
        db_log_msg(self.index, req, "FamilyResource.on_post_by_ids - failed - invalid body format.")

    @timing
    def on_put(self, req, resp, family_id=None):
        """Modify a family; JSON body with ``family_name`` (alphanumeric, dots, dashes, underscores) and/or ``is_library``. Answers 202 on success, 400 for a malformed body."""
        resp.status = falcon.HTTP_400
        if not req.content_length or not isinstance(req.media, dict):
            resp.data = jsonify({"status": "failed", "data": {"message": "PUT request without body can't be processed."}})
            db_log_msg(self.index, req, "FamilyResource.on_put - failed - no body")
            return
        # sanitize sample information
        information_update = req.media
        if "family_name" in information_update and not re.match(r"^(?![\-_.])(?!.*[\-_.]{2})(?!.*[\-_.]\Z)[a-zA-Z0-9._\-]{0,64}\Z", information_update["family_name"]):
            resp.data = jsonify({"status": "failed", "data": {"message": "family_name may be 0-64 alphanumeric chars with single dots, dashes, underscores inbetween."}})
            return
        if "is_library" in information_update:
            if not (isinstance(information_update["is_library"], bool) or information_update["is_library"] in ["True", "False", "true", "false", "0", "1", 0, 1]):
                resp.data = jsonify({"status": "failed", "data": {"message": "is_library must be boolean."}})
                db_log_msg(self.index, req, "FamilyResource.on_put - failed - Is_library was not boolean.")
                return
            if information_update["is_library"] in ["True", "true", "1", 1]:
                information_update["is_library"] = True
            elif information_update["is_library"] in ["False", "false", "0", 0]:
                information_update["is_library"] = False
        successful = self.index.modifyFamily(family_id, information_update, force_recalculation=True, username=get_username(req))
        if successful:
            resp.data = jsonify({"status": "successful", "data": {"message": "Family modified."}})
            resp.status = falcon.HTTP_202
            db_log_msg(self.index, req, f"FamilyResource.on_put - success - family_id {family_id} modified")
        else:
            resp.data = jsonify({"status": "failed", "data": {"message": "Failed to modify family."}})
            db_log_msg(self.index, req, f"FamilyResource.on_put - failed - family_id {family_id} not modified")

    @timing
    def on_delete(self, req, resp, family_id=None):
        """Delete a family and its samples; with ``keep_samples=true`` the samples move to the unknown family instead. Unknown ids answer 404."""
        if family_id is None or self.index.getFamily(family_id) is None:
            resp.data = jsonify(
                {
                    "status": "failed",
                    "data": {"message": "We don't have a family with that id."},
                }
            )
            resp.status = falcon.HTTP_404
            db_log_msg(self.index, req, "FamilyResource.on_delete - failed - due to unknown family")
            return
        # parse optional request parameters
        keep_samples = False
        if "keep_samples" in req.params:
            keep_samples = req.params["keep_samples"].lower().strip() == "true"
        successful = self.index.deleteFamily(family_id, keep_samples=keep_samples, force_recalculation=True, username=get_username(req))
        if successful:
            db_log_msg(self.index, req, f"FamilyResource.on_delete - success - family_id: {family_id}, keep_samples={keep_samples}")
            resp.data = jsonify({"status": "successful", "data": successful})
            resp.status = falcon.HTTP_202
        else:
            db_log_msg(self.index, req, f"FamilyResource.on_delete - failed - family_id: {family_id}, keep_samples={keep_samples}")
            resp.data = jsonify({"status": "failed", "data": {"message": "Failed to delete family."}})
            # TODO whats the correct code?
            resp.status = falcon.HTTP_410

    @timing
    def on_get_collection(self, req, resp):
        """All families keyed by id; optional ``start`` (first family id) and ``limit``."""
        db_log_msg(self.index, req, "FamilyResource.on_get_collection")
        # parse optional request parameters
        start_index = 0
        if "start" in req.params:
            try:
                start_index = max(0, int(req.params["start"]))
            except ValueError:
                pass
        limit_family_count = 0
        if "limit" in req.params:
            try:
                limit_family_count = max(0, int(req.params["limit"]))
            except ValueError:
                pass
        family_overview = {}
        index = 0
        for family_id, family_entry in self.index.getFamilies().items():
            if index >= start_index:
                if (limit_family_count == 0) or (len(family_overview) < limit_family_count):
                    family_overview[family_id] = family_entry.toDict()
                else:
                    break
            index += 1
        resp.data = jsonify({"status": "successful", "data": family_overview})
