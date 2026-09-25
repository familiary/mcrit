import datetime
import functools
import logging
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from smda.common.SmdaReport import SmdaReport
from smda.Disassembler import Disassembler

from mcrit.queue.LocalQueue import Job
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.storage.SearchResult import SearchResult

# Only do basicConfig if no handlers have been configured
if not logging.root.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
LOGGER = logging.getLogger(__name__)


class JobTerminatedError(Exception):
    """Raised by awaitResult when the awaited job was terminated instead of finishing."""


def isJobTerminated(job):
    if job is None:
        return True

    return job.is_terminated


def isJobFailed(job):
    return (job is not None) and (job.is_failed)


def isJobFinishedTerminatedOrFailed(job):
    return isJobTerminated(job) or (job.result is not None) or isJobFailed(job)


class McritClientError(Exception):
    """A request the MCRIT server answered with a failure.

    Only raised in the client's raising modes (``raise_client_errors`` /
    ``raise_server_errors``); by default every failure answers ``None``. Carries the HTTP
    status the server sent and the message from its ``{"status": "failed", "data":
    {"message": ...}}`` body, when there was one.
    """

    def __init__(self, status_code, message="", url=""):
        self.status_code = status_code
        self.message = message
        self.url = url
        where = f" ({url})" if url else ""
        super().__init__(f"MCRIT answered {status_code}{where}: {message or 'no message'}")


class McritRequestError(McritClientError):
    """The server refused the request as such (a 4xx): the client asked for something that
    does not exist or sent something the server does not accept. Nothing is wrong on the
    server's side, so a caller can usually tell the user what was wrong with the input."""


class McritBadRequest(McritRequestError):
    """400: the request was malformed or carried invalid parameters."""


class McritNotFound(McritRequestError):
    """404: the sample, family, function or job the request named does not exist."""


class McritGone(McritRequestError):
    """410: the record existed and has been removed since."""


class McritUnauthorized(McritRequestError):
    """401 or 403: the API token is missing, invalid, or not allowed to do this."""


class McritConflict(McritRequestError):
    """409: the request collides with what is stored already (a binary that exists)."""


class McritServerError(McritClientError):
    """The server failed to answer the request (500, 501, an unexpected status, or a 2xx
    whose body reports ``"status": "failed"``). The request may or may not have been acted
    on, which is what makes this different from a refused request."""


_REQUEST_ERRORS = {400: McritBadRequest, 401: McritUnauthorized, 403: McritUnauthorized, 404: McritNotFound, 409: McritConflict, 410: McritGone}


def request_error_for(status):
    """The McritRequestError subclass for a 4xx status, McritRequestError itself for one
    without a class of its own."""
    return _REQUEST_ERRORS.get(status, McritRequestError)


def failure_message(response):
    """The message the server put into a failed answer, or an empty string.

    Every failure MCRIT sends is ``{"status": "failed", "data": {"message": "..."}}``;
    proxies and crashes can answer with anything, so a body that is not that shape yields
    an empty message rather than a second error.
    """
    try:
        body = response.json()
    except ValueError:
        return ""
    if not isinstance(body, dict):
        return ""
    data = body.get("data")
    if isinstance(data, dict) and isinstance(data.get("message"), str):
        return data["message"]
    return ""


def handle_response(response, raise_client_errors=False, raise_server_errors=False) -> Any:
    """The ``data`` of a successful answer, ``None`` for a failed one.

    With ``raise_client_errors`` any 4xx raises a :class:`McritRequestError` (400, 401/403,
    404, 409 and 410 have subclasses of their own); with ``raise_server_errors`` a 500, 501,
    any status this client does not know, and a 2xx that reports ``"status": "failed"``
    raise :class:`McritServerError`. Both default to False, so existing callers keep getting
    ``None``, which they cannot tell apart from "not found" (fkie-cad/mcritweb#43).
    """
    data = None
    status = response.status_code
    url = getattr(response, "url", "") or ""
    if status in [500, 501]:
        LOGGER.warning("McritClient received status code %d from MCRIT.", status)
        if raise_server_errors:
            raise McritServerError(status, failure_message(response), url)
    elif 400 <= status < 500:
        if raise_client_errors:
            raise request_error_for(status)(status, failure_message(response), url)
    elif status in [200, 202]:
        json_response = response.json()
        if "status" in json_response and json_response["status"] == "successful":
            data = json_response["data"]
        elif raise_server_errors:
            raise McritServerError(status, failure_message(response), url)
    elif raise_server_errors:
        LOGGER.warning("McritClient received unexpected status code %d from MCRIT.", status)
        raise McritServerError(status, failure_message(response), url)
    return data


# (connect, read) in seconds, as requests takes it. requests itself waits forever on both, so
# a server that is down behind a firewall or up but hung used to block the caller for good.
# The connect is bounded by default: establishing a connection to a reachable server never
# legitimately takes long. The read is not, because several endpoints (/import, /export, a
# /status over a large corpus) answer only once all their work is done and can take minutes;
# a caller that knows its own bound - MCRITweb behind a proxy that gives up after 300 s - sets
# one with `timeout=(connect, read)` or by assigning `client.timeout`.
DEFAULT_TIMEOUT = (10, None)


class McritClient:
    """Python client for the MCRIT REST API.

    Every public method maps to one endpoint (see docs/api_reference.md) and answers the
    parsed ``data`` of the server's JSON response, converted to the storage entry classes
    where one exists; ``None`` when the server answered with a failure status, unless the
    client was built to raise (``raise_client_errors``, ``raise_server_errors``). The return
    annotations describe this default mode; with ``raw_responses=True`` every method answers
    the ``requests.Response`` itself instead. Methods that
    schedule work on the MCRIT queue answer the job id, which ``getResultForJob`` or
    ``awaitResult`` turn into the result.

    Args:
        mcrit_server: base URL of the server, default ``http://localhost:8000``
        apitoken: sent as the ``apitoken`` header when the server requires one
        username: sent as the ``username`` header and recorded with the jobs this client creates
        raw_responses: when True every method returns the ``requests.Response`` unchanged
        raise_client_errors: a 4xx raises a :class:`McritRequestError` (``McritBadRequest``,
            ``McritUnauthorized``, ``McritNotFound``, ``McritConflict``, ``McritGone``) instead
            of answering None
        raise_server_errors: a 500, 501, unknown status or a failed 2xx raises
            :class:`McritServerError` instead of answering None
        timeout: ``(connect, read)`` seconds for every request, or one number for both; see
            ``DEFAULT_TIMEOUT``. A request that runs out raises
            ``requests.exceptions.ConnectTimeout`` or ``ReadTimeout``, like any other connection failure
    """

    def __init__(
        self,
        mcrit_server: Optional[str] = None,
        apitoken: Optional[str] = None,
        username: Optional[str] = None,
        raw_responses: bool = False,
        raise_client_errors: bool = False,
        raise_server_errors: bool = False,
        timeout: Union[None, float, Tuple[Optional[float], Optional[float]]] = DEFAULT_TIMEOUT,
    ) -> None:
        self.mcrit_server = "http://localhost:8000"
        self.headers = {}
        self.raw = True if raw_responses else False
        self.raise_client_errors = raise_client_errors
        self.raise_server_errors = raise_server_errors
        self.timeout = timeout
        if apitoken:
            self.headers.update({"apitoken": apitoken})
        if username:
            self.headers.update({"username": username})
        if mcrit_server is not None:
            self.mcrit_server = mcrit_server

    @staticmethod
    def _passthrough(response: requests.Response) -> Any:
        # raw_responses mode: the Response itself, whatever the method's annotation says
        return response

    def _handle(self, response: requests.Response) -> Any:
        """handle_response in this client's error mode."""
        return handle_response(response, raise_client_errors=self.raise_client_errors, raise_server_errors=self.raise_server_errors)

    def setApitoken(self, apitoken: str) -> None:
        """Send ``apitoken`` as the ``apitoken`` header from now on."""
        self.headers.update({"apitoken": apitoken})

    def setUsername(self, username: str) -> None:
        """Send ``username`` as the ``username`` header from now on; MCRIT records it with the jobs this client creates."""
        self.headers.update({"username": username})

    def _getMatchingRequestParams(
        self, minhash_threshold=None, pichash_size=None, force_recalculation=None, band_matches_required=None, exclude_self_matches=False, sample_group_only=False
    ):
        params = {}
        if minhash_threshold is not None:
            params["minhash_score"] = minhash_threshold
        if pichash_size is not None:
            params["pichash_size"] = pichash_size
        if force_recalculation is not None:
            params["force_recalculation"] = force_recalculation
        if band_matches_required is not None:
            params["band_matches_required"] = band_matches_required
        if exclude_self_matches:
            params["exclude_self_matches"] = True
        if sample_group_only:
            params["sample_group_only"] = True
        return params

    def respawn(self) -> Optional[Dict[str, Any]]:
        """POST /respawn: drop the whole database and set up a fresh, empty instance. Answers the server's confirmation message."""
        response = requests.post(f"{self.mcrit_server}/respawn", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def completeMinhashes(self) -> Optional[str]:
        """GET /complete_minhashes: schedule a job that calculates every missing minhash. Answers the job id."""
        response = requests.get(f"{self.mcrit_server}/complete_minhashes", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def rebuildIndex(self) -> Optional[str]:
        """GET /rebuild_index: schedule a job that drops the band index and rebuilds it from the stored minhashes. Answers the job id."""
        response = requests.get(f"{self.mcrit_server}/rebuild_index", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def rebuildPicBlockHashIndex(self) -> Optional[str]:
        """GET /rebuild_picblockhash_index: schedule a job that rebuilds the inverted picblockhash index getUniqueBlocks reads. Answers the job id."""
        response = requests.get(f"{self.mcrit_server}/rebuild_picblockhash_index", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def rebuildFunctionRangeIndex(self) -> Optional[str]:
        """GET /rebuild_function_range_index: schedule a job that rebuilds the function->sample range index two-stage matching needs. Answers the job id."""
        response = requests.get(f"{self.mcrit_server}/rebuild_function_range_index", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def rebuildBandDfIndex(self) -> Optional[str]:
        """GET /rebuild_band_df_index: schedule a job that stores and indexes each band's posting-list length, so STORAGE_BAND_DF_CUTOFF can skip from the index. Answers the job id."""
        response = requests.get(f"{self.mcrit_server}/rebuild_band_df_index", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def repairMinHashes(self) -> Optional[str]:
        """POST /repair_minhashes: schedule a job that rehashes only the samples whose minhashes an older smda escaper produced (#142). Answers the job id."""
        response = requests.post(f"{self.mcrit_server}/repair_minhashes", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def recomputeFamilyStats(self) -> Optional[str]:
        """POST /recompute_family_stats: schedule a job that sets every family's sample/function counters from the collections (#151). Answers the job id."""
        response = requests.post(f"{self.mcrit_server}/recompute_family_stats", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def recalculatePicHashes(self) -> Optional[str]:
        """GET /recalculate_pichashes: schedule a job that recalculates the pichashes of samples hashed with an older smda. Answers the job id."""
        response = requests.get(f"{self.mcrit_server}/recalculate_pichashes", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def recalculateMinHashes(self) -> Optional[str]:
        """GET /recalculate_minhashes: schedule a job that drops every minhash and recalculates all of them. Answers the job id."""
        response = requests.get(f"{self.mcrit_server}/recalculate_minhashes", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def addReport(self, smda_report: SmdaReport) -> Optional[Tuple[SampleEntry, Optional[str]]]:
        """POST /samples: submit a disassembled SMDA report as a new sample.

        Returns:
            the SampleEntry and the id of the minhashing job (None when the sample already existed or
            no hashing was scheduled); None when the server rejected the report
        """
        smda_json = smda_report.toDict()
        response = requests.post(f"{self.mcrit_server}/samples", json=smda_json, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            if "job_id" in data:
                job_id = data["job_id"]
            else:
                job_id = None
            return SampleEntry.fromDict(data["sample_info"]), job_id

    def addBinarySample(
        self,
        binary: bytes,
        filename: Optional[str] = None,
        family: Optional[str] = None,
        version: Optional[str] = None,
        is_dump: bool = False,
        base_addr: Optional[int] = None,
        bitness: Optional[int] = None,
    ) -> Optional[str]:
        """POST /samples/binary: submit a raw binary for disassembly and insertion.

        Args:
            binary: file content, or a memory dump when ``is_dump`` is set
            filename, family, version: metadata stored with the sample
            is_dump: disassemble as a mapped image loaded at ``base_addr``
            base_addr: image base of a dump
            bitness: 32 or 64, for dumps

        Returns:
            the id of the disassembly job; its result carries the sample entry
        """
        query_fields = []
        if filename is not None:
            query_fields.append(f"filename={filename}")
        if family is not None:
            query_fields.append(f"family={family}")
        if version is not None:
            query_fields.append(f"version={version}")
        if is_dump:
            query_fields.append("is_dump=1")
        if base_addr is not None:
            query_fields.append(f"base_addr=0x{base_addr:x}")
        if bitness is not None and bitness in [32, 64]:
            query_fields.append(f"bitness={bitness}")
        query_string = ""
        if len(query_fields) > 0:
            query_string = "?" + "&".join(query_fields)
        response = requests.post(f"{self.mcrit_server}/samples/binary{query_string}", data=binary, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    ###########################################
    ### Families
    ###########################################

    def modifyFamily(self, family_id: int, family_name: Optional[str] = None, is_library: Optional[bool] = None) -> Optional[Dict[str, Any]]:
        """PUT /families/{family_id}: rename a family and/or mark it as a library. Answers the confirmation message, None when rejected."""
        update_dict = {}
        if family_name is not None:
            update_dict["family_name"] = family_name
        if is_library is not None:
            update_dict["is_library"] = is_library
        response = requests.put(f"{self.mcrit_server}/families/{family_id}", update_dict, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getFamily(self, family_id: int, with_samples: bool = True) -> Optional[FamilyEntry]:
        """GET /families/{family_id}: one family, with its samples unless ``with_samples`` is False. None for an unknown id."""
        query_params = "?with_samples=true" if with_samples else "?with_samples=false"
        response = requests.get(f"{self.mcrit_server}/families/{family_id}{query_params}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return FamilyEntry.fromDict(data)
        return None

    def getFamiliesByIds(self, family_ids: List[int]) -> Dict[int, FamilyEntry]:
        """POST /families/ids: the families with these ids, keyed by family id, without their sample lists (as getFamily(..., with_samples=False)); ids that are not found are left out."""
        if not family_ids:
            return {}
        family_id_string = ",".join(["%d" % fid for fid in family_ids])
        response = requests.post(f"{self.mcrit_server}/families/ids", data=family_id_string, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return {int(k): FamilyEntry.fromDict(v) for k, v in data.items()}
        return {}

    def getFamilies(self) -> Optional[Dict[int, FamilyEntry]]:
        """GET /families: every family, keyed by family id."""
        response = requests.get(f"{self.mcrit_server}/families", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return {i: FamilyEntry.fromDict(entry) for i, entry in data.items()}
        return None

    def isFamilyId(self, family_id: int) -> bool:
        """GET /families/{family_id}: whether the id names a family."""
        response = requests.get(f"{self.mcrit_server}/families/{family_id}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return True
        return False

    def deleteFamily(self, family_id: int, keep_samples: bool = False) -> Optional[bool]:
        """DELETE /families/{family_id}: delete a family and its samples, or with ``keep_samples`` move the samples to the unknown family. Answers True on success, None for an unknown id."""
        query_params = "?keep_samples=true" if keep_samples else "?keep_samples=false"
        response = requests.delete(f"{self.mcrit_server}/families/{family_id}{query_params}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    ###########################################
    ### Samples
    ###########################################

    def isSampleId(self, sample_id: int) -> bool:
        """GET /samples/{sample_id}: whether the id names a sample."""
        response = requests.get(f"{self.mcrit_server}/samples/{sample_id}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return True
        return False

    def modifySample(
        self, sample_id: int, family_name: Optional[str] = None, version: Optional[str] = None, component: Optional[str] = None, is_library: Optional[bool] = None
    ) -> Optional[Dict[str, Any]]:
        """PUT /samples/{sample_id}: change a sample's family, version, component and/or library flag. Answers the confirmation message, None when rejected."""
        update_dict = {}
        if family_name is not None:
            update_dict["family_name"] = family_name
        if version is not None:
            update_dict["version"] = version
        if component is not None:
            update_dict["component"] = component
        if is_library is not None:
            update_dict["is_library"] = is_library
        response = requests.put(f"{self.mcrit_server}/samples/{sample_id}", update_dict, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def deleteSample(self, sample_id: int) -> Optional[bool]:
        """DELETE /samples/{sample_id}: delete a sample with its functions and index entries. Answers True on success."""
        response = requests.delete(f"{self.mcrit_server}/samples/{sample_id}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getSamplesByFamilyId(self, family_id: int) -> Optional[Dict[int, SampleEntry]]:
        """The samples of a family keyed by sample id (via GET /families/{family_id}); None for an unknown family."""
        family_data = self.getFamily(family_id)
        if family_data is not None:
            return family_data.samples
        return None

    def getSampleById(self, sample_id: int) -> Optional[SampleEntry]:
        """GET /samples/{sample_id}: one sample; None for an unknown id."""
        response = requests.get(f"{self.mcrit_server}/samples/{sample_id}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return SampleEntry.fromDict(data)

    def getSamplesByIds(self, sample_ids: List[int]) -> Dict[int, SampleEntry]:
        """POST /samples/ids: the samples with these ids, keyed by sample id; negative ids resolve against query samples, as in getSampleById, and ids that are not found are left out."""
        if not sample_ids:
            return {}
        sample_id_string = ",".join(["%d" % sid for sid in sample_ids])
        response = requests.post(f"{self.mcrit_server}/samples/ids", data=sample_id_string, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return {int(k): SampleEntry.fromDict(v) for k, v in data.items()}
        return {}

    def getSamples(self, start: int = 0, limit: int = 0) -> Optional[Dict[int, SampleEntry]]:
        """GET /samples: samples keyed by id, from sample id ``start`` on and at most ``limit`` many (0 = all)."""
        query_string = ""
        if (isinstance(start, int) and start >= 0) and (isinstance(limit, int) and limit >= 0):
            query_string = f"?start={start}&limit={limit}"
        response = requests.get(f"{self.mcrit_server}/samples{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return {int(k): SampleEntry.fromDict(v) for k, v in data.items()}

    ###########################################
    ### Functions
    ###########################################

    def getFunctionsBySampleId(self, sample_id: int) -> Optional[List[FunctionEntry]]:
        """GET /samples/{sample_id}/functions: every function of a sample; None for an unknown id."""
        response = requests.get(f"{self.mcrit_server}/samples/{sample_id}/functions", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return [FunctionEntry.fromDict(function_entry_dict) for function_entry_dict in data.values()]

    def getFunctions(self, start: int = 0, limit: int = 0) -> Optional[Dict[int, FunctionEntry]]:
        """GET /functions: functions keyed by id, from function id ``start`` on and at most ``limit`` many (0 = all)."""
        query_string = ""
        if (isinstance(start, int) and start >= 0) and (isinstance(limit, int) and limit >= 0):
            query_string = f"?start={start}&limit={limit}"
        response = requests.get(f"{self.mcrit_server}/functions{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return {int(k): FunctionEntry.fromDict(v) for k, v in data.items()}

    def getFunctionsByIds(self, function_ids: List[int], with_label_only: bool = False) -> Dict[int, FunctionEntry]:
        """POST /functions: the functions with the given ids keyed by id; with ``with_label_only`` only those carrying a label."""
        query_with_label_only = "?with_label_only=True" if with_label_only else ""
        function_id_string = ",".join(["%d" % fid for fid in function_ids])
        response = requests.post(f"{self.mcrit_server}/functions{query_with_label_only}", data=function_id_string, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return {int(k): FunctionEntry.fromDict(v) for k, v in data.items()}
        return {}

    def isFunctionId(self, function_id: int) -> bool:
        """GET /functions/{function_id}: whether the id names a function."""
        response = requests.get(f"{self.mcrit_server}/functions/{function_id}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return True
        return False

    def getFunctionById(self, function_id: int, with_xcfg: bool = False) -> Optional[FunctionEntry]:
        """GET /functions/{function_id}: one function, with its disassembly when ``with_xcfg`` is set; None for an unknown id."""
        query_with_xcfg = "?with_xcfg=True" if with_xcfg else ""
        response = requests.get(f"{self.mcrit_server}/functions/{function_id}{query_with_xcfg}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return FunctionEntry.fromDict(data)

    def modifyFunction(self, function_id: int, function_name: str) -> Optional[Dict[str, Any]]:
        """PUT /functions/{function_id}: rename a function; the name is also recorded as a label by this client's username. Answers the confirmation message, None when rejected."""
        response = requests.put(f"{self.mcrit_server}/functions/{function_id}", {"function_name": function_name}, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    ###########################################
    ### Matching
    ###########################################

    def requestMatchesForSmdaReport(
        self,
        smda_report: SmdaReport,
        minhash_threshold: Optional[int] = None,
        pichash_size: Optional[int] = None,
        band_matches_required: Optional[int] = None,
        force_recalculation: bool = False,
    ) -> Optional[str]:
        """POST /query: schedule matching of an SMDA report that is not stored in MCRIT against the corpus.

        Args:
            minhash_threshold: minimum minhash score (0-100) for a function match
            pichash_size: pichash size to match with
            band_matches_required: bands that must agree before minhashes are compared
            force_recalculation: ignore a cached result of the same request

        Returns:
            the job id; the result is a MatchingResult dict
        """
        smda_json = smda_report.toDict()
        params = self._getMatchingRequestParams(minhash_threshold, pichash_size, force_recalculation, band_matches_required)
        response = requests.post(f"{self.mcrit_server}/query", json=smda_json, headers=self.headers, params=params, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def requestMatchesForMappedBinary(
        self,
        binary: bytes,
        base_address: int,
        minhash_threshold: Optional[int] = None,
        pichash_size: Optional[int] = None,
        band_matches_required: Optional[int] = None,
        disassemble_locally: bool = True,
        force_recalculation: bool = False,
    ) -> Optional[str]:
        """Match a memory dump mapped at ``base_address`` against the corpus: disassembled with the local smda and sent to POST /query, or with ``disassemble_locally=False`` sent to POST /query/binary/mapped/{base_address} for the server to disassemble. Matching parameters as for requestMatchesForSmdaReport. Answers the job id, None when local disassembly failed."""
        if disassemble_locally:
            disassembler = Disassembler()
            smda_report = disassembler.disassembleBuffer(binary, base_address)
            if smda_report.status == "error":
                return None
            return self.requestMatchesForSmdaReport(
                smda_report,
                minhash_threshold=minhash_threshold,
                pichash_size=pichash_size,
                band_matches_required=band_matches_required,
                force_recalculation=force_recalculation,
            )

        params = self._getMatchingRequestParams(minhash_threshold, pichash_size, force_recalculation, band_matches_required)
        response = requests.post(f"{self.mcrit_server}/query/binary/mapped/{base_address}", binary, headers=self.headers, params=params, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def requestMatchesForUnmappedBinary(
        self,
        binary: bytes,
        minhash_threshold: Optional[int] = None,
        pichash_size: Optional[int] = None,
        band_matches_required: Optional[int] = None,
        disassemble_locally: bool = True,
        force_recalculation: bool = False,
    ) -> Optional[str]:
        """Match a file against the corpus: disassembled with the local smda and sent to POST /query, or with ``disassemble_locally=False`` sent to POST /query/binary for the server to disassemble. Matching parameters as for requestMatchesForSmdaReport. Answers the job id, None when local disassembly failed."""
        if disassemble_locally:
            disassembler = Disassembler()
            smda_report = disassembler.disassembleUnmappedBuffer(binary)
            if smda_report.status == "error":
                return None
            return self.requestMatchesForSmdaReport(
                smda_report,
                minhash_threshold=minhash_threshold,
                pichash_size=pichash_size,
                band_matches_required=band_matches_required,
                force_recalculation=force_recalculation,
            )

        params = self._getMatchingRequestParams(minhash_threshold, pichash_size, force_recalculation, band_matches_required)

        response = requests.post(f"{self.mcrit_server}/query/binary", binary, headers=self.headers, params=params, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def requestMatchesForSample(
        self,
        sample_id: int,
        minhash_threshold: Optional[int] = None,
        pichash_size: Optional[int] = None,
        band_matches_required: Optional[int] = None,
        force_recalculation: bool = False,
    ) -> Optional[str]:
        """GET /matches/sample/{sample_id}: schedule matching of a stored sample against the corpus; matching parameters as for requestMatchesForSmdaReport. Answers the job id."""
        params = self._getMatchingRequestParams(minhash_threshold, pichash_size, force_recalculation, band_matches_required)
        response = requests.get(f"{self.mcrit_server}/matches/sample/{sample_id}", headers=self.headers, params=params, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def requestMatchesForSampleVs(
        self,
        sample_id: int,
        other_sample_id: int,
        minhash_threshold: Optional[int] = None,
        pichash_size: Optional[int] = None,
        band_matches_required: Optional[int] = None,
        force_recalculation: bool = False,
    ) -> Optional[str]:
        """GET /matches/sample/{sample_id}/{other_sample_id}: schedule matching of one stored sample against another; matching parameters as for requestMatchesForSmdaReport. Answers the job id."""
        params = self._getMatchingRequestParams(minhash_threshold, pichash_size, force_recalculation, band_matches_required)
        response = requests.get(f"{self.mcrit_server}/matches/sample/{sample_id}/{other_sample_id}", headers=self.headers, params=params, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def requestMatchesCross(
        self,
        sample_ids: List[int],
        sample_group_only: bool = False,
        minhash_threshold: Optional[int] = None,
        pichash_size: Optional[int] = None,
        band_matches_required: Optional[int] = None,
        force_recalculation: bool = False,
    ) -> Optional[str]:
        """GET /matches/sample/cross/{sample_ids}: schedule cross matching of the samples against each other (``sample_group_only``) or against the corpus; matching parameters as for requestMatchesForSmdaReport. Answers the id of the job combining the per-sample results."""
        params = self._getMatchingRequestParams(minhash_threshold, pichash_size, force_recalculation, band_matches_required, sample_group_only=sample_group_only)
        response = requests.get(f"{self.mcrit_server}/matches/sample/cross/{','.join([str(id) for id in sample_ids])}", headers=self.headers, params=params, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getMatchFunctionVs(self, function_id_a: int, function_id_b: int) -> Optional[Dict[str, Any]]:
        """GET /matches/function/{function_id_a}/{function_id_b}: compare two stored functions directly (minhash score, pichash equality, the matched function entry). None for an unknown id."""
        response = requests.get(f"{self.mcrit_server}/matches/function/{function_id_a}/{function_id_b}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getMatchesForSmdaFunction(
        self,
        smda_report: SmdaReport,
        minhash_threshold: Optional[int] = None,
        pichash_size: Optional[int] = None,
        force_recalculation: Optional[bool] = None,
        band_matches_required: Optional[int] = None,
        exclude_self_matches: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """POST /query/function: match an SMDA report holding a single function synchronously; ``exclude_self_matches`` drops matches with the same sample. Answers the MatchingResult dict."""
        # TODO add the same parameter possibilities that are used for regular full matching jobs
        params = self._getMatchingRequestParams(minhash_threshold, pichash_size, force_recalculation, band_matches_required, exclude_self_matches)
        response = requests.post(f"{self.mcrit_server}/query/function", json=smda_report.toDict(), headers=self.headers, params=params, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getMatchesForPicHash(self, pichash: int, summary: bool = False) -> Optional[Any]:
        """GET /query/pichash/{pichash}[/summary]: the (family_id, sample_id, function_id) tuples of the functions with this pichash, or with ``summary`` the counts of families, samples and functions."""
        summary_string = "/summary" if summary else ""
        response = requests.get(f"{self.mcrit_server}/query/pichash/{pichash:016x}{summary_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getMatchesForPicBlockHash(self, picblockhash: int, summary: bool = False) -> Optional[Any]:
        """GET /query/picblockhash/{picblockhash}[/summary]: the (family_id, sample_id, function_id, offset) tuples of the basic blocks with this picblockhash, or with ``summary`` the counts of families, samples and functions."""
        summary_string = "/summary" if summary else ""
        response = requests.get(f"{self.mcrit_server}/query/picblockhash/{picblockhash:016x}{summary_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getSampleBySha256(self, sample_sha256: str) -> Optional[SampleEntry]:
        """GET /samples/sha256/{sha256}: one sample by its sha256; None when unknown or malformed."""
        response = requests.get(f"{self.mcrit_server}/samples/sha256/{sample_sha256}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is None:
            return None
        return SampleEntry.fromDict(data)

    ###########################################
    ### Status, Results
    ###########################################

    def getStatus(self, with_pichash: bool = True) -> Optional[Dict[str, Any]]:
        """GET /status: statistics of the instance (database state, counts of samples, families, functions and, with ``with_pichash``, unique pichashes, smda version and escaper fingerprint)."""
        query_string = ""
        if with_pichash:
            query_string = "?with_pichash=True"
        response = requests.get(f"{self.mcrit_server}/status{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getVersion(self) -> Optional[str]:
        """GET /version: the version of the MCRIT server."""
        response = requests.get(f"{self.mcrit_server}/version", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if isinstance(data, dict) and "version" in data:
            return data["version"]
        return None

    def getJobCount(self, filter: Optional[str] = None) -> Optional[int]:
        """GET /jobs: how many jobs the queue holds, optionally only those whose descriptor contains ``filter``."""
        query_string = ""
        if isinstance(filter, str) and filter is not None:
            if len(query_string) == 0:
                query_string = f"?filter={filter}"
            else:
                query_string += f"&filter={filter}"
        response = requests.get(f"{self.mcrit_server}/jobs{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return len(data)

    def getQueueStatistics(self, with_refresh: bool = False) -> Optional[Dict[str, Any]]:
        """GET /jobs/stats: queue statistics per method and state; ``with_refresh`` recounts instead of answering the cached numbers."""
        query_string = ""
        if with_refresh:
            if len(query_string) == 0:
                query_string = "?with_refresh=True"
        response = requests.get(f"{self.mcrit_server}/jobs/stats/{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    @staticmethod
    def _job_selection_query(method=None, filter=None, state=None, username=None, **more) -> str:
        """The query string of the parameters that select jobs, URL-encoded (a filter is free text)."""
        params = {"method": method, "filter": filter, "state": state, "username": username, **more}
        present = {key: value for key, value in params.items() if value is not None and value is not False and value != 0}
        return "?" + urllib.parse.urlencode(present, safe=",") if present else ""

    def getQueueData(
        self,
        start: int = 0,
        limit: int = 0,
        method: Optional[str] = None,
        filter: Optional[str] = None,
        state: Optional[str] = None,
        ascending: bool = False,
        username: Optional[str] = None,
        sample_ids: Optional[List[int]] = None,
        job_ids: Optional[List[str]] = None,
    ) -> Optional[List[Job]]:
        """GET /jobs: the queued jobs, newest first unless ``ascending``, from index ``start`` on and at most ``limit`` many (0 = all); ``method`` (job method name), ``state``, ``filter`` (substring of the job parameters, case-insensitive) and ``username`` (who requested it) narrow them down before paging, so a page is a page of the matches; ``sample_ids`` selects jobs of ``method`` by their first argument (the server then requires ``method``) and ``job_ids`` selects jobs by id."""
        query_string = self._job_selection_query(
            method=method,
            filter=filter,
            state=state,
            username=username,
            start=start if isinstance(start, int) else 0,
            limit=limit if isinstance(limit, int) else 0,
            ascending="True" if ascending else None,
            sample_ids=None if sample_ids is None else ",".join(str(sample_id) for sample_id in sample_ids),
            job_ids=None if job_ids is None else ",".join(str(job_id) for job_id in job_ids),
        )
        response = requests.get(f"{self.mcrit_server}/jobs/{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return [Job(job_data, None) for job_data in data]

    def getQueueCount(
        self,
        method: Optional[str] = None,
        filter: Optional[str] = None,
        state: Optional[str] = None,
        username: Optional[str] = None,
        sample_ids: Optional[List[int]] = None,
        job_ids: Optional[List[str]] = None,
    ) -> Optional[int]:
        """GET /jobs/count: how many jobs getQueueData would list for the same selection - what a paginated listing needs to size itself."""
        query_string = self._job_selection_query(
            method=method,
            filter=filter,
            state=state,
            username=username,
            sample_ids=None if sample_ids is None else ",".join(str(sample_id) for sample_id in sample_ids),
            job_ids=None if job_ids is None else ",".join(str(job_id) for job_id in job_ids),
        )
        response = requests.get(f"{self.mcrit_server}/jobs/count{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return data["count"]

    def deleteQueueData(
        self, method: Optional[str] = None, created_before: Optional[datetime.datetime] = None, finished_before: Optional[datetime.datetime] = None
    ) -> Optional[Dict[str, int]]:
        """DELETE /jobs: delete the jobs matching all given filters. Answers ``num_deleted``."""
        query_string = ""
        if isinstance(method, str) and method is not None:
            if len(query_string) == 0:
                query_string = f"?method={method}"
            else:
                query_string += f"&method={method}"
        if isinstance(created_before, datetime.datetime) and created_before is not None:
            if len(query_string) == 0:
                query_string = f"?created_before={created_before.strftime('%Y-%m-%dT%H:%M:%S')}"
            else:
                query_string += f"&created_before={created_before.strftime('%Y-%m-%dT%H:%M:%S')}"
        if isinstance(finished_before, datetime.datetime) and finished_before is not None:
            if len(query_string) == 0:
                query_string = f"?finished_before={finished_before.strftime('%Y-%m-%dT%H:%M:%S')}"
            else:
                query_string += f"&finished_before={finished_before.strftime('%Y-%m-%dT%H:%M:%S')}"
        response = requests.delete(f"{self.mcrit_server}/jobs/{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def deleteJob(self, job_id: str) -> Optional[Dict[str, int]]:
        """DELETE /jobs/{job_id}: delete one job and its result. Answers ``num_deleted``."""
        response = requests.delete(f"{self.mcrit_server}/jobs/{job_id}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getJobData(self, job_id: str) -> Optional[Job]:
        """GET /jobs/{job_id}: one job; None for an unknown or malformed id."""
        response = requests.get(f"{self.mcrit_server}/jobs/{job_id}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return Job(data, None)

    def getResultForJob(self, job_id: str, compact: bool = False) -> Optional[Any]:
        """GET /jobs/{job_id}/result: the result of a job, None while it has not finished; ``compact`` strips the per-function matches of a matching result."""
        query_string = "?compact=True" if compact else ""
        response = requests.get(f"{self.mcrit_server}/jobs/{job_id}/result{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getResult(self, result_id: str, compact: bool = False) -> Optional[Any]:
        """GET /results/{result_id}: the result stored under a result id; ``compact`` strips the per-function matches of a matching result."""
        query_string = "?compact=True" if compact else ""
        response = requests.get(f"{self.mcrit_server}/results/{result_id}{query_string}", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    def getJobForResult(self, result_id: str) -> Optional[Job]:
        """GET /results/{result_id}/job: the job that produced a result."""
        response = requests.get(f"{self.mcrit_server}/results/{result_id}/job", headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        data = self._handle(response)
        if data is not None:
            return Job(data, None)

    def awaitResult(self, job_id: Optional[str], sleep_time: float = 2, compact: bool = False) -> Optional[Any]:
        """Poll GET /jobs/{job_id} every ``sleep_time`` seconds until the job finished, then fetch its result.

        Raises:
            JobTerminatedError: when the job was terminated instead of finishing
        """
        if job_id is None:
            return None
        job = self.getJobData(job_id)
        while not isJobFinishedTerminatedOrFailed(job):
            time.sleep(sleep_time)
            job = self.getJobData(job_id)
        if isJobTerminated(job):
            raise JobTerminatedError
        assert job is not None
        result_id = job.result
        return self.getResult(result_id, compact=compact)

    ###########################################
    ### Import / Export
    ###########################################

    def getExportData(self, sample_ids: Optional[List[int]] = None, compress_data: bool = True) -> Optional[Dict[str, Any]]:
        """GET /export or /export/{sample_ids}: the export of the whole instance or of the given samples, for addImportData on another instance; ``compress_data`` compresses the function entries per sample.

        Raises:
            ValueError: when ``sample_ids`` is not a list of int
        """
        compress_uri_param = "?compress=True" if compress_data else ""
        result_data = {}
        if sample_ids is not None:
            if isinstance(sample_ids, list) and all(isinstance(item, int) for item in sample_ids):
                sample_ids_as_str = ",".join([str(sample_id) for sample_id in sample_ids])
                response = requests.get(f"{self.mcrit_server}/export/{sample_ids_as_str}{compress_uri_param}", headers=self.headers, timeout=self.timeout)
                result_data = self._passthrough(response) if self.raw else self._handle(response)
            else:
                raise ValueError("sample_ids must be a list of int.")
        else:
            response = requests.get(f"{self.mcrit_server}/export{compress_uri_param}", headers=self.headers, timeout=self.timeout)
            result_data = self._passthrough(response) if self.raw else self._handle(response)
        return result_data

    def addImportData(self, import_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """POST /import: import the data getExportData produced (ids are remapped, known samples skipped). Answers the import report.

        Raises:
            ValueError: when ``import_data`` is not a dict
        """
        if not isinstance(import_data, dict):
            raise ValueError("Can only forward dictionaries with export data.")
        response = requests.post(f"{self.mcrit_server}/import", json=import_data, headers=self.headers, timeout=self.timeout)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    ###########################################
    ### Unique Blocks
    ###########################################

    @staticmethod
    def _getUniqueBlocksParams(covers_required=None, min_instructions=None):
        # covers_required is the k of the k-of-n block cover, min_instructions drops shorter blocks
        # before it is chosen; omitted parameters leave the server defaults (10 and 0) in place
        params = {}
        if covers_required is not None:
            params["covers_required"] = covers_required
        if min_instructions is not None:
            params["min_instructions"] = min_instructions
        return params

    def requestUniqueBlocksForSamples(self, sample_ids: List[int], covers_required: Optional[int] = None, min_instructions: Optional[int] = None) -> Optional[str]:
        """GET /uniqueblocks/samples/{sample_ids}: schedule the search for basic blocks unique to these samples; ``covers_required`` is the k of the k-of-n block cover, ``min_instructions`` drops shorter blocks. Answers the job id.

        Raises:
            ValueError: when ``sample_ids`` is not a list of int
        """
        if isinstance(sample_ids, list) and all(isinstance(item, int) for item in sample_ids):
            sample_ids_as_str = ",".join([str(sample_id) for sample_id in sample_ids])
            params = self._getUniqueBlocksParams(covers_required, min_instructions)
            response = requests.get(f"{self.mcrit_server}/uniqueblocks/samples/{sample_ids_as_str}", headers=self.headers, params=params, timeout=self.timeout)
            result_data = self._passthrough(response) if self.raw else self._handle(response)
        else:
            raise ValueError("sample_ids must be a list of int.")
        return result_data

    def requestUniqueBlocksForFamily(self, family_id: int, covers_required: Optional[int] = None, min_instructions: Optional[int] = None) -> Optional[str]:
        """GET /uniqueblocks/family/{family_id}: schedule the search for basic blocks unique to the family's samples; parameters as for requestUniqueBlocksForSamples. Answers the job id.

        Raises:
            ValueError: when ``family_id`` is not an int
        """
        if isinstance(family_id, int):
            params = self._getUniqueBlocksParams(covers_required, min_instructions)
            response = requests.get(f"{self.mcrit_server}/uniqueblocks/family/{family_id}", headers=self.headers, params=params, timeout=self.timeout)
            result_data = self._passthrough(response) if self.raw else self._handle(response)
        else:
            raise ValueError("family_id must be an int.")
        return result_data

    ###########################################
    ### Search
    ###########################################

    # When performing an initial search, the cursor should be set to None.
    # Search results are of the following form:
    # {
    #     "search_results": {
    #         id1: found_entry1,
    #         id2: found_entry2,
    #         ...
    #     },
    #     "cursor": {
    #         "forward": forward cursor,
    #         "backward": backward cursor,
    #     }
    # }
    # To get further results, perform a search using the forward cursor.
    # To get back to the previous search results, use the backward cursor.
    # If no further or previous results are available, the forward or backward cursor will be None.
    #
    # IMPORTANT: A cursor shall only be used in combination with the same
    # search_term, is_ascending and sort_by value that were used when the cursor was returned from mcrit.
    # If those parameters are altered, mcrit's behavior is undefined.

    def _search_request(self, search_kind, search_term, cursor=None, is_ascending=True, sort_by=None, limit=None):
        params = {
            "query": search_term,
            "is_ascending": is_ascending,
        }
        if cursor is not None:
            params["cursor"] = cursor
        if sort_by is not None:
            params["sort_by"] = sort_by
        if limit is not None:
            params["limit"] = limit
        encoded_params = urllib.parse.urlencode(params)
        return requests.get(f"{self.mcrit_server}/search/{search_kind}?{encoded_params}", headers=self.headers, timeout=self.timeout)

    def _search_base(self, search_kind, search_term, cursor=None, is_ascending=True, sort_by=None, limit=None):
        response = self._search_request(search_kind, search_term, cursor=cursor, is_ascending=is_ascending, sort_by=sort_by, limit=limit)
        if self.raw:
            return self._passthrough(response)
        return self._handle(response)

    search_families = functools.partialmethod(_search_base, "families")

    search_samples = functools.partialmethod(_search_base, "samples")

    search_functions = functools.partialmethod(_search_base, "functions")

    # The typed counterparts (fkie-cad/mcritweb#64): the same search, answered as a
    # SearchResult whose entries are FamilyEntry/SampleEntry/FunctionEntry objects, like every
    # other accessor of this client. The search_* methods above keep answering the wire dict.

    def _typed_search(self, search_kind, entry_class, search_term, cursor=None, is_ascending=True, sort_by=None, limit=None):
        response = self._search_request(search_kind, search_term, cursor=cursor, is_ascending=is_ascending, sort_by=sort_by, limit=limit)
        if self.raw:
            # like the other camel-case accessors: the requests.Response itself
            return self._passthrough(response)
        data = self._handle(response)
        if data is None:
            return None
        return SearchResult.fromDict(data, entry_class)

    def searchFamilies(
        self, search_term: str, cursor: Optional[str] = None, is_ascending: bool = True, sort_by: Optional[str] = None, limit: Optional[int] = None
    ) -> Optional[SearchResult[FamilyEntry]]:
        """
        Search families by <search_term>, answered as FamilyEntry objects
        Supported by mcritweb API pass-through (as /search/families)
        """
        return self._typed_search("families", FamilyEntry, search_term, cursor=cursor, is_ascending=is_ascending, sort_by=sort_by, limit=limit)

    def searchSamples(
        self, search_term: str, cursor: Optional[str] = None, is_ascending: bool = True, sort_by: Optional[str] = None, limit: Optional[int] = None
    ) -> Optional[SearchResult[SampleEntry]]:
        """
        Search samples by <search_term>, answered as SampleEntry objects
        Supported by mcritweb API pass-through (as /search/samples)
        """
        return self._typed_search("samples", SampleEntry, search_term, cursor=cursor, is_ascending=is_ascending, sort_by=sort_by, limit=limit)

    def searchFunctions(
        self, search_term: str, cursor: Optional[str] = None, is_ascending: bool = True, sort_by: Optional[str] = None, limit: Optional[int] = None
    ) -> Optional[SearchResult[FunctionEntry]]:
        """
        Search functions by <search_term>, answered as FunctionEntry objects
        Supported by mcritweb API pass-through (as /search/functions)
        """
        return self._typed_search("functions", FunctionEntry, search_term, cursor=cursor, is_ascending=is_ascending, sort_by=sort_by, limit=limit)
