import falcon

from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.libs.tags import MAX_TAGS_PER_ENTITY, MAX_TAGS_PER_REQUEST, TAG_ENTITIES, TagLimitError, normalizeTags
from mcrit.server.utils import db_log_msg, jsonify, timing


class TagResource:
    """Tags on families, samples and functions (#53).

    One instance per entity serves ``POST`` (add) and ``DELETE`` (remove) on
    ``/{families|samples|functions}/{id}/tags``, with a JSON body ``{"tags": [...]}``. Both are
    synchronous, answer 200 with the entity's resulting tags, 400 for a malformed body, an
    invalid tag, more than MAX_TAGS_PER_REQUEST tags or an add that would take the entity past
    MAX_TAGS_PER_ENTITY - without touching the entity - and 404 for an unknown id. There is
    deliberately no replace-all: two analysts tagging the same entity at once cannot erase each
    other's tags.

    The instance without an entity serves ``GET /tags?entity=family|sample|function``, the
    distinct tags of that entity with the number of entities carrying each. The answer holds one
    entry per distinct tag, so it grows with the tag vocabulary (at most 64 characters a tag), not
    with the number of tagged entities. It is not paginated: the storage has to count every
    tagged entity's tags to answer any page, so pages would repeat that work rather than save it,
    and one tag's entities are found by searching ``tags:<tag>``, which is paginated.
    """

    def __init__(self, index: MinHashIndex, entity=None):
        self.index = index
        self.entity = entity

    def _fail(self, req, resp, status, message, log_message):
        resp.status = status
        resp.data = jsonify({"status": "failed", "data": {"message": message}})
        db_log_msg(self.index, req, f"TagResource.{log_message}")

    def _change_tags(self, req, resp, route_params, is_adding):
        assert self.entity is not None
        responder = "on_post" if is_adding else "on_delete"
        # the route names its id after the entity, as the other routes on the same path do
        entity_id = route_params[f"{self.entity}_id"]
        if not req.content_length or not isinstance(req.media, dict) or "tags" not in req.media:
            return self._fail(req, resp, falcon.HTTP_400, 'Expected a JSON body {"tags": [...]}.', f"{responder} - failed - no tags in body.")
        try:
            # counted before any tag is normalised
            tags = normalizeTags(req.media["tags"], limit=MAX_TAGS_PER_REQUEST)
        except ValueError as invalid_tags:
            return self._fail(req, resp, falcon.HTTP_400, str(invalid_tags), f"{responder} - failed - invalid tags.")
        if not tags:
            return self._fail(req, resp, falcon.HTTP_400, "tags must name at least one tag.", f"{responder} - failed - empty tags.")
        if is_adding:
            try:
                resulting_tags = self.index.addTags(self.entity, entity_id, tags)
            except TagLimitError as too_many:
                return self._fail(req, resp, falcon.HTTP_400, str(too_many), f"{responder} - failed - {self.entity} {entity_id} at {MAX_TAGS_PER_ENTITY} tags.")
        else:
            resulting_tags = self.index.removeTags(self.entity, entity_id, tags)
        if resulting_tags is None:
            return self._fail(req, resp, falcon.HTTP_404, f"We don't have a {self.entity} with that id.", f"{responder} - failed - {self.entity} {entity_id} unknown.")
        resp.status = falcon.HTTP_200
        resp.data = jsonify({"status": "successful", "data": {"entity": self.entity, "entity_id": entity_id, "tags": resulting_tags}})
        db_log_msg(self.index, req, f"TagResource.{responder} - success - {self.entity} {entity_id} {'+' if is_adding else '-'}{len(tags)} tags.")

    @timing
    def on_post(self, req, resp, **route_params):
        """Add tags to the entity; tags it already carries are kept once."""
        self._change_tags(req, resp, route_params, is_adding=True)

    @timing
    def on_delete(self, req, resp, **route_params):
        """Remove tags from the entity; tags it does not carry are ignored."""
        self._change_tags(req, resp, route_params, is_adding=False)

    @timing
    def on_get_collection(self, req, resp):
        entity = req.params.get("entity")
        if entity not in TAG_ENTITIES:
            return self._fail(req, resp, falcon.HTTP_400, f"entity must be one of {', '.join(TAG_ENTITIES)}.", "on_get_collection - failed - invalid entity.")
        tag_counts = self.index.getTagCounts(entity)
        resp.data = jsonify({"status": "successful", "data": {"entity": entity, "tags": tag_counts}})
        db_log_msg(self.index, req, f"TagResource.on_get_collection - success - {entity}.")
