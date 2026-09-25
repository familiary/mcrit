#!/usr/bin/env python3
"""Generate the REST API reference (docs/api_reference.md) from the running route table.

Every route the Falcon app registers is listed with its HTTP methods, the responder that
serves it, the responder's docstring and the McritClient method that calls it, so the
document cannot drift from the code: tests/testApiReference.py regenerates it and compares.

Usage:
    python -m mcrit.server.api_reference            # rewrite docs/api_reference.md
    python -m mcrit.server.api_reference --check    # exit 1 when the file is stale
"""

import argparse
import ast
import inspect
import os
import re
import sys
from typing import Dict, List, Optional, Set, Tuple

import falcon.inspect

from mcrit.server.BlocksResource import BlocksResource
from mcrit.server.FamilyResource import FamilyResource
from mcrit.server.FunctionResource import FunctionResource
from mcrit.server.JobResource import JobResource
from mcrit.server.MatchResource import MatchResource
from mcrit.server.QueryResource import QueryResource
from mcrit.server.SampleResource import SampleResource
from mcrit.server.StatusResource import StatusResource

RESOURCE_CLASSES = {cls.__name__: cls for cls in (StatusResource, FamilyResource, SampleResource, FunctionResource, MatchResource, BlocksResource, QueryResource, JobResource)}
# responders Falcon adds on its own for every route
FALCON_DEFAULT_RESPONDERS = ("method_not_allowed", "options_responder")
DEFAULT_OUTPUT = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "docs", "api_reference.md")
CLIENT_SOURCE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "client", "McritClient.py")


def _is_placeholder(segment: str) -> bool:
    return segment.startswith("{") and segment.endswith("}")


def _match_quality(route_path: str, client_path: str) -> int:
    """0 = no match; otherwise 1 plus the number of segments where a placeholder on one side
    stands for a literal on the other (getJobData's /jobs/{job_id} also fits /jobs/stats and
    getQueueStatistics' /jobs/stats also fits /jobs/{job_id}; the exact pairing wins)."""
    route_segments = route_path.strip("/").split("/")
    client_segments = client_path.strip("/").split("/")
    if len(route_segments) != len(client_segments):
        return 0
    crossings = 0
    for route_segment, client_segment in zip(route_segments, client_segments):
        if route_segment == client_segment or (_is_placeholder(route_segment) and _is_placeholder(client_segment)):
            continue
        if _is_placeholder(client_segment) or _is_placeholder(route_segment):
            # one side names a literal the other leaves open: a match, but a weaker one
            crossings += 1
        else:
            return 0
    return crossings + 1


def _clients_for_route(route_path: str, method: str, calls: List[Tuple[str, str, str]]) -> List[str]:
    """The client methods calling a route; a call that fits some route without crossing a
    literal is not also attributed to the routes it only fits through a placeholder."""
    names = set()
    for verb, path, name in calls:
        if verb != method:
            continue
        quality = _match_quality(route_path, path)
        if quality == 0:
            continue
        best = min(q for q in (_match_quality(other, path) for other in _ROUTE_PATHS) if q)
        if quality == best:
            names.add(name)
    return sorted(names)


_ROUTE_PATHS: List[str] = []


# f-string placeholders the client appends for query parameters rather than path segments
_QUERY_PLACEHOLDER = re.compile(r"\{[^}]*(query|param|uri)[^}]*\}$")


def _public_names(
    method_name: str,
    callers: Dict[str, List[Tuple[str, Dict[str, str]]]],
    partials: Dict[str, List[Tuple[str, Dict[str, str]]]],
    seen: Optional[Set[str]] = None,
) -> List[Tuple[str, Dict[str, str]]]:
    """The public methods a request made in method_name is attributed to, with the helper parameters they bind.

    A private helper may itself be reached through another private helper (search_families ->
    _search_base -> _search_request), so the attribution follows private callers until it reaches
    public methods; a partialmethod binding found on the way is kept for the path substitution.
    """
    if not method_name.startswith("_"):
        return [(method_name, {})]
    seen = set() if seen is None else seen
    if method_name in seen:
        return []
    seen.add(method_name)
    names = list(partials.get(method_name, []))
    for caller, bound in callers.get(method_name, []):
        # a binding made further up reaches this helper when the parameter keeps its name
        for name, outer in _public_names(caller, callers, partials, set(seen)):
            entry = (name, {**outer, **bound})
            if entry not in names:
                names.append(entry)
    return names


def client_calls() -> List[Tuple[str, str, str]]:
    """(HTTP method, path as the client writes it, McritClient method) for every request in McritClient."""
    with open(CLIENT_SOURCE) as handle:
        tree = ast.parse(handle.read())
    calls = []
    client = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "McritClient")
    methods = [node for node in client.body if isinstance(node, ast.FunctionDef)]
    # a request made in a private helper is attributed to the public methods that call the helper
    # searchFamilies calls self._typed_search("families", ...): a constant argument binds the
    # helper's parameter the same way a partialmethod does
    by_name = {method.name: method for method in methods}
    callers: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
    for method in methods:
        for node in ast.walk(method):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == "self":
                callee = by_name.get(node.func.attr)
                params = callee.args.args[1:] if callee is not None else []
                bound = {param.arg: str(value.value) for param, value in zip(params, node.args) if isinstance(value, ast.Constant)}
                callers.setdefault(node.func.attr, []).append((method.name, bound))
    # search_families = functools.partialmethod(_search_base, "families") binds the helper's
    # first parameter after self to a constant, which names the path segment
    partials: Dict[str, List[Tuple[str, Dict[str, str]]]] = {}
    for node in client.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call) and ast.unparse(node.value.func).endswith("partialmethod"):
            helper_name = ast.unparse(node.value.args[0])
            helper = next((m for m in methods if m.name == helper_name), None)
            if helper is None or len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                continue
            bound = {param.arg: str(value.value) for param, value in zip(helper.args.args[1:], node.value.args[1:]) if isinstance(value, ast.Constant)}
            partials.setdefault(helper_name, []).append((node.targets[0].id, bound))
    for method in methods:
        for node in ast.walk(method):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == "requests"):
                continue
            url = node.args[0] if node.args else None
            if not isinstance(url, ast.JoinedStr):
                continue
            # f"{self.mcrit_server}/families/{family_id}{query_params}" -> "/families/{family_id}"
            path = ""
            for part in url.values:
                if isinstance(part, ast.Constant):
                    path += str(part.value)
                elif isinstance(part, ast.FormattedValue):
                    path += "{%s}" % ast.unparse(part.value)
            path = path.replace("{self.mcrit_server}", "")
            path = _QUERY_PLACEHOLDER.sub("", path).split("?")[0]
            variants = [path]
            if path.endswith("{summary_string}"):
                # "/summary" or nothing: the method serves the endpoint and its summary variant
                variants = [path[: -len("{summary_string}")], path[: -len("{summary_string}")] + "/summary"]
            names = _public_names(method.name, callers, partials)
            for variant in variants:
                for name, bound in names:
                    bound_variant = variant
                    for parameter, value in bound.items():
                        bound_variant = bound_variant.replace("{%s}" % parameter, value)
                    calls.append((node.func.attr.upper(), bound_variant.rstrip("/") or "/", name))
    return calls


def routes(app) -> List[Dict]:
    info = falcon.inspect.inspect_app(app)
    calls = client_calls()
    _ROUTE_PATHS[:] = [route.path for route in info.routes]
    result = []
    for route in info.routes:
        for method_info in route.methods:
            if method_info.function_name in FALCON_DEFAULT_RESPONDERS:
                continue
            resource_class = RESOURCE_CLASSES.get(route.class_name)
            responder = getattr(resource_class, method_info.function_name, None) if resource_class else None
            doc = inspect.getdoc(responder) if responder else None
            clients = _clients_for_route(route.path, method_info.method, calls)
            result.append(
                {
                    "path": route.path,
                    "method": method_info.method,
                    "resource": route.class_name,
                    "responder": method_info.function_name,
                    "doc": doc or "",
                    "clients": clients,
                }
            )
    return sorted(result, key=lambda entry: (entry["path"], entry["method"]))


def render(entries: List[Dict]) -> str:
    lines = [
        "# MCRIT REST API reference",
        "",
        "Generated by `python -m mcrit.server.api_reference` from the route table in `mcrit/server/application_routes.py`; do not edit by hand.",
        "",
        'Every response is a JSON document `{"status": "successful" | "failed", "data": ...}`. Requests carry the optional headers '
        "`apitoken` (required when the server was started with `MCRIT_AUTH_TOKEN`) and `username` (recorded with the jobs it creates). "
        "Endpoints described as scheduling a job answer the job id; fetch the outcome via `GET /jobs/{job_id}/result`. The Python client "
        "`mcrit.client.McritClient` wraps every endpoint; the *Client* column names the method.",
        "",
        "| Method | Path | Description | Client |",
        "|---|---|---|---|",
    ]
    for entry in entries:
        description = entry["doc"].replace("\n", " ").replace("|", "\\|") or "_undocumented_"
        clients = ", ".join("`%s`" % name for name in entry["clients"]) or "-"
        lines.append("| `%s` | `%s` | %s | %s |" % (entry["method"], entry["path"], description, clients))
    lines.append("")
    return "\n".join(lines)


def build(app=None) -> str:
    if app is None:
        from mcrit.server.application_routes import get_app

        app = get_app()
    return render(routes(app))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true", help="do not write; exit 1 if the file differs from the generated reference")
    args = parser.parse_args(argv)
    document = build()
    if args.check:
        current = open(args.out).read() if os.path.exists(args.out) else ""
        if current != document:
            print("%s is stale, regenerate with: python -m mcrit.server.api_reference" % args.out)
            return 1
        print("%s is up to date" % args.out)
        return 0
    with open(args.out, "w") as handle:
        handle.write(document)
    print("wrote %s (%d endpoints)" % (args.out, document.count("\n| `")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
