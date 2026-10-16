"""Tags on families, samples and functions (#53).

A tag is a short lower-case label - ``packed``, ``reviewed``, ``source:vt`` - that an analyst
attaches to an entity. Stored tags are always normalised and valid, so every writer goes through
normalizeTags, and the search and the distinct-tag listing can compare them as plain strings.

Two caps keep a tag list small, in the REST request as in the stored document:

- MAX_TAGS_PER_REQUEST: one POST or DELETE names at most this many tags, counted as sent, before
  any of them is normalised - so a request cannot make the server walk an arbitrarily long list.
- MAX_TAGS_PER_ENTITY: an entity carries at most this many. Adding tags that would take it past
  the cap is refused as a whole (TagLimitError), checked by the storage in the same atomic write
  that adds them. At 64 characters a tag, that keeps the array under 20 KB of BSON per document.
  The one exception is a rename that merges a family into another: it unions both families' tags
  uncapped, since a rename is no tagging request to refuse and dropping tags would lose them
  silently. A family merged past the cap takes no further tags until some are removed.
"""

import re
from typing import Any, List, Optional

# the entities that carry tags, as the REST API and McritClient name them
TAG_ENTITIES = ("family", "sample", "function")
# a letter or digit first, so a tag can neither start an operator ("$", "-") nor be blank;
# the colon allows namespaced tags such as "source:vt"
TAG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9 ._:\-]{0,63}$")
TAG_RULE = "a tag is 1-64 letters, digits, spaces, dots, colons, underscores or dashes, starting with a letter or digit, and is stored lower-cased"
# see the module docstring
MAX_TAGS_PER_REQUEST = 100
MAX_TAGS_PER_ENTITY = 256


class TagLimitError(ValueError):
    """Too many tags: in one request (MAX_TAGS_PER_REQUEST) or on one entity (MAX_TAGS_PER_ENTITY)."""


def normalizeTag(tag: str) -> str:
    """A tag as stored: stripped and lower-cased. Does not validate, see isValidTag."""
    return tag.strip().lower()


def isValidTag(tag) -> bool:
    """True for a string that is a valid tag once normalised."""
    return isinstance(tag, str) and TAG_PATTERN.fullmatch(normalizeTag(tag)) is not None


def normalizeTags(tags: Any, drop_invalid: bool = False, limit: Optional[int] = None) -> List[str]:
    """Tags as stored: normalised, each once, in the order given.

    Raises ValueError when tags is not a list (or tuple/set) of strings or any of them is invalid,
    naming the offending value; with drop_invalid=True those are skipped instead, for data that
    arrives from elsewhere (an import) and should not fail as a whole over one bad tag. With a
    limit, a list of more than that many entries raises TagLimitError before any is looked at.
    """
    if isinstance(tags, (str, bytes)) or not isinstance(tags, (list, tuple, set)):
        if drop_invalid:
            return []
        raise ValueError(f"tags must be a list of strings, not {type(tags).__name__}.")
    if limit is not None and len(tags) > limit:
        raise TagLimitError(f"at most {limit} tags per request, not {len(tags)}.")
    normalized = []
    for tag in tags:
        if not isValidTag(tag):
            if drop_invalid:
                continue
            raise ValueError(f"invalid tag {tag!r}: {TAG_RULE}.")
        normalized.append(normalizeTag(tag))
    # a dict keeps the first of each in order, and deduplicates in linear time
    return list(dict.fromkeys(normalized))


def mergeTags(tags: List[str], added: List[str]) -> List[str]:
    """tags followed by those of added it does not hold yet, each once and in order."""
    return list(dict.fromkeys([*tags, *added]))


def tagLimitMessage(entity: str, entity_id: int, num_tags: int) -> str:
    return f"a {entity} carries at most {MAX_TAGS_PER_ENTITY} tags: {entity} {entity_id} carries {num_tags}, and adding these would exceed that."


def checkTagEntity(entity: str) -> str:
    """The entity if it is one that carries tags, otherwise a ValueError naming the valid ones."""
    if entity not in TAG_ENTITIES:
        raise ValueError(f"entity must be one of {', '.join(TAG_ENTITIES)}, not {entity!r}.")
    return entity
