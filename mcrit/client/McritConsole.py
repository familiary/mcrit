import argparse
import hashlib
import json
import os
import re
import subprocess

from dotenv import load_dotenv
from smda.common.SmdaReport import SmdaReport
from smda.Disassembler import Disassembler

from mcrit.client.McritClient import McritClient
from mcrit.storage.FamilyEntry import FamilyEntry
from mcrit.storage.FunctionEntry import FunctionEntry
from mcrit.storage.MatchingResult import MatchingResult
from mcrit.storage.SampleEntry import SampleEntry

### Helper functionality for submissions


def is_pe_or_elf(input_path):
    """
    Don't perform a full validation, just check heuristically if the file starts with PE or ELF markings
    """
    data = b""
    with open(input_path, "rb") as fin:
        data = fin.read(4)
    if len(data) >= 4:
        is_elf = data[:4] == b"\x7fELF"
        is_pe = data[:2] == b"MZ"
        return is_elf or is_pe
    return False


def get_base_addr(input_path):
    """
    If provided path ends with 0x[0-9a-fA-F]{8,16}, we assume that this is an IMAGEBASE address.
    """
    base_addr = None
    baddr_match = re.search(re.compile("0x(?P<base_addr>[0-9a-fA-F]{8,16})$"), input_path)
    if baddr_match:
        base_addr = int(baddr_match.group("base_addr"), 16)
    return base_addr


def sha256(content):
    return hashlib.sha256(content).hexdigest()


def readFileContent(file_path):
    file_content = b""
    with open(file_path, "rb") as fin:
        file_content = fin.read()
    return file_content


def getFamilyName(input_path):
    family_name = ""
    abs_path = os.path.abspath(input_path)
    for folder in abs_path.split("/")[::-1]:
        if folder:
            family_name = folder
    return family_name


def getSampleVersion(input_path, family):
    sample_version = ""
    collected = []
    for folder in input_path.split("/"):
        if folder != family and folder != "":
            collected.append(folder)
    if collected:
        sample_version = os.sep.join(collected)
    return sample_version


def getFolderFilePath(input_root, input_path):
    abs_path = os.path.abspath(input_path)
    relative_filepath = abs_path[len(input_root) :]
    return relative_filepath


def is_smda_report(filepath):
    filename = os.path.basename(filepath)
    return filename.endswith(".smda")


def get_primary_smda_meta_data(filepath):
    if not is_smda_report(filepath):
        return None
    try:
        primary_smda_meta_data = {
            "sha256": None,
            "filename": None,
            "family": None,
            "version": None,
        }
        # do not parse as JSON because it's expensive, instead do a line-by-line search
        # in the beginning until xcfg is found and extract via regex
        success = False
        with open(filepath) as f_smda:
            while line := f_smda.readline():
                if "sha256" in line:
                    sha256_match = re.search(re.compile('"sha256": "(?P<sha256>[0-9a-fA-F]{64})"'), line)
                    if sha256_match:
                        primary_smda_meta_data["sha256"] = sha256_match.group("sha256")
                elif "filename" in line:
                    filename_match = re.search(re.compile('"filename": "(?P<filename>.*)"'), line)
                    if filename_match:
                        primary_smda_meta_data["filename"] = filename_match.group("filename")
                elif "family" in line:
                    family_match = re.search(re.compile('"family": "(?P<family>.*)"'), line)
                    if family_match:
                        primary_smda_meta_data["family"] = family_match.group("family")
                elif "version" in line:
                    version_match = re.search(re.compile('"version": "(?P<version>.*)"'), line)
                    if version_match:
                        primary_smda_meta_data["version"] = version_match.group("version")
                # stop searching after we found all 4 fields
                if (
                    primary_smda_meta_data["sha256"] is not None
                    and primary_smda_meta_data["filename"] is not None
                    and primary_smda_meta_data["family"] is not None
                    and primary_smda_meta_data["version"] is not None
                ):
                    success = True
                    break
                elif "xcfg" in line:
                    break
        return primary_smda_meta_data if success else None
    except OSError:
        return None


def getSmdaReportFromFilepath(args, filepath):
    filename = os.path.basename(filepath)
    smda_report = None
    if args.smda:
        if not is_smda_report(filepath):
            print(f"Skipping a file not recognized as SMDA report: {filepath}")
        else:
            try:
                smda_report = SmdaReport.fromFile(filepath)
            except Exception:
                print(f"Failed to parse SMDA report: {filepath}")
    else:
        if args.executables_only and not is_pe_or_elf(filepath):
            print(f"Skipping a file not recognized as executable: {filepath}")
        else:
            disassembler = Disassembler()
            if get_base_addr(filename) is not None:
                base_addr = get_base_addr(filename)
                try:
                    smda_report = disassembler.disassembleBuffer(readFileContent(filepath), base_addr)
                    smda_report.filename = filename
                except Exception:
                    import traceback

                    print(f"ERROR: SMDA caused an exception while processing this file: {filepath}")
                    print(traceback.format_exc())
                    return None
            else:
                try:
                    smda_report = disassembler.disassembleFile(filepath)
                except Exception:
                    import traceback

                    print(f"ERROR: SMDA caused an exception while processing this file: {filepath}")
                    print(traceback.format_exc())
                    return None
    # apply any of the forced flags: family, version, library
    if smda_report:
        if args.mode in ["file", "dir"]:
            if args.family is not None:
                smda_report.family = args.family
            if args.version is not None:
                smda_report.version = args.version
        if args.mode in ["file", "dir", "recursive"] and args.library:
            smda_report.is_library = True
        if args.output and not args.smda:
            if not os.path.exists(args.output):
                os.makedirs(args.output)
            output_filepath = os.sep.join([args.output, f"{smda_report.sha256}.smda"])
            if not os.path.exists(output_filepath):
                with open(output_filepath, "w") as f_smda:
                    json.dump(smda_report.toDict(), f_smda, sort_keys=True, indent=1)
    return smda_report


def submitViaSubprocess(args, filepath):
    command = ["python", "-m", "mcrit", "client", "submit"]
    if args.server:
        command.extend(["--server", args.server])
    if args.apitoken:
        command.extend(["--apitoken", args.apitoken])
    if args.family:
        command.extend(["--family", args.family])
    if args.version:
        command.extend(["--version", args.version])
    if args.library:
        command.extend(["--library"])
    if args.executables_only:
        command.extend(["--executables_only"])
    if args.output:
        command.extend(["--output", args.output])
    if args.smda:
        command.extend(["--smda"])
    command.append(filepath)
    console_handle = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout_result, stderr_result = console_handle.communicate(timeout=args.worker_timeout)
        stdout_result = stdout_result.strip().decode("utf-8")
        if stdout_result:
            print("STDOUT logs from subprocess: ", stdout_result)
        if stderr_result:
            stderr_result = stderr_result.strip().decode("utf-8")
            print("STDERR logs from subprocess: ", stderr_result)
    except subprocess.TimeoutExpired:
        print(f"Processing {str(filepath)} with a spawned worker timed out during processing.")


### Main Processing


class McritConsole:
    def __init__(self) -> None:
        self.parser = argparse.ArgumentParser(description="Console interaction with MCRIT.")
        subparsers = self.parser.add_subparsers(dest="command")
        self.parser_client = subparsers.add_parser("client")
        subparser_client = self.parser_client.add_subparsers(dest="client_command")
        self.parser_client.add_argument("--server", type=str, default=None, help="The MCRIT server to connect to (overrides dotnev/env).")
        self.parser_client.add_argument("--apitoken", type=str, default=None, help="API token to use for the connection (overrides dotnev/env).")
        subparser_client.add_parser("status")
        # client submit
        client_submit = subparser_client.add_parser("submit", help="Various ways of file submission incl. disassembly using SMDA if needed.")
        client_submit.add_argument("filepath", type=str, help="Submit the folllowing <filepath>, indicating a (file/dir).")
        client_submit.add_argument(
            "--mode",
            type=str,
            default="file",
            choices=["file", "dir", "recursive", "malpedia"],
            help="Submit a single <file> or all files in a <dir>. Use <recursive> submission for a folder structured as ./family_name/version/version/files. Synchronize <malpedia> into MCRIT. Default: <file>.",
        )
        client_submit.add_argument("-f", "--family", type=str, help="Set/Override SmdaReport with this family (only in modes: file/dir)")
        client_submit.add_argument("-v", "--version", type=str, help="Set/Override SmdaReport with this version (only in modes: file/dir)")
        client_submit.add_argument(
            "-l", "--library", action="store_true", help="Set/Override SmdaReport with the library flag (only in modes: file/dir/recursive, default: False)."
        )
        client_submit.add_argument(
            "-u", "--force_update", action="store_true", help="Force update of family/version/library, even if file is already in MCRIT (only in modes: file/dir, default: False)."
        )
        client_submit.add_argument("-x", "--executables_only", action="store_true", help="Only process files that are parsable PE or ELF files (default: False).")
        client_submit.add_argument("-o", "--output", type=str, help="Optionally store SMDA reports in folder OUTPUT, which is created if not existing.")
        client_submit.add_argument(
            "-s", "--smda", action="store_true", help="Do not disassemble, instead only submit files that are recognized as SMDA reports (only works with modes: file/dir)."
        )
        client_submit.add_argument("-w", "--worker", action="store_true", help="Spawn workers to process the submission (only in modes: dir/recursive/malpedia, default: False).")
        client_submit.add_argument("-t", "--worker-timeout", type=int, default=300, help="Timeout for workers to conclude the submission (default: 300 seconds).")
        # client query
        client_query = subparser_client.add_parser("query", help="Query MCRIT with a sample incl. disassembly using SMDA if needed.")
        client_query.add_argument("filepath", type=str, help="Submit the folllowing <filepath>.")
        client_query.add_argument("-a", "--base_addr", type=str, help="Set a base_addr and treat this file as mapped buffer (0x<addr> as hexadecimal int or <addr> as decimal int.")
        client_query.add_argument("-b", "--bitness", type=str, help="When processing as buffer, use this bitness ([32, 64] - default: 32 bit)")
        client_query.add_argument("-o", "--output", type=str, help="Optionally store matching report in folder OUTPUT.")
        client_query.add_argument("-s", "--smda", action="store_true", help="Assume provided input file is a SMDA report.")
        client_query.add_argument("-f", "--force-recalculation", action="store_true", help="Do not rely on cached results, instead always recalculate the matching.")
        # client import / export / search
        client_import = subparser_client.add_parser("import", help="Import of previously exported data in the MCRIT format.")
        client_import.add_argument("filepath", type=str, help="Import a given <filepath> containing MCRIT data into the storage.")
        client_export = subparser_client.add_parser("export", help="Export of data from MCRIT.")
        client_export.add_argument("filepath", type=str, help="Export the full storage into <filepath>.")
        client_export.add_argument("--sample_ids", type=str, help="Limit export to a list of comma-separated <sample_ids>.")
        client_search = subparser_client.add_parser("search", help="Search for arbitrary data based on a search string.")
        client_search.add_argument("search_term", type=str, help="Conduct a search with a given <search_term>.")
        client_queue = subparser_client.add_parser("queue", help="Inspection of the current processing queue.")
        client_queue.add_argument("--filter", type=str, help="Filter queue entries with this term.")

    def run(self):
        ARGS = self.parser.parse_args()
        # try to load .env file first
        THIS_FILE_PATH = str(os.path.abspath(__file__))
        PROJECT_ROOT = str(os.path.abspath(os.sep.join([THIS_FILE_PATH, "..", "..", ".."])))
        env_path = os.path.join(PROJECT_ROOT, ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
        # regardless of outcome for dotenv, try to load from env
        server = os.environ.get("MCRIT_CLI_SERVER")
        apitoken = os.environ.get("MCRIT_CLI_APITOKEN")
        # always override with command line arguments
        if ARGS.server:
            server = ARGS.server
        if ARGS.apitoken:
            apitoken = ARGS.apitoken
        self.client = McritClient(server, apitoken)
        if ARGS.client_command == "status":
            self._handle_status(ARGS)
        elif ARGS.client_command == "import":
            self._handle_import(ARGS)
        elif ARGS.client_command == "export":
            self._handle_export(ARGS)
        elif ARGS.client_command == "search":
            self._handle_search(ARGS)
        elif ARGS.client_command == "queue":
            self._handle_queue(ARGS)
        elif ARGS.client_command == "query":
            self._handle_query(ARGS)
        elif ARGS.client_command == "submit":
            self._handle_submit(ARGS)
        else:
            print(self.parser_client.print_help())

    def _handle_status(self, args):
        result = self.client.getStatus(with_pichash=False)
        if result:
            print(f"DB:        {result['status']['storage_type']} - {result['status']['db_state']} | {result['status']['db_timestamp']}")
            print(f"Families:  {result['status']['num_families']}")
            print(f"Samples:   {result['status']['num_samples']}")
            print(f"Functions: {result['status']['num_functions']}")
        else:
            print("Failed to retrieve status from the server.")

    def _handle_import(self, args):
        if os.path.isfile(args.filepath):
            with open(args.filepath) as fin:
                result = self.client.addImportData(json.load(fin))
                print(result)
        else:
            print("Your <filepath> does not exist.")

    def _handle_export(self, args):
        sample_ids = None
        if args.sample_ids is not None:
            sample_ids = [int(i) for i in args.sample_ids.split(",")]
        result = self.client.getExportData(sample_ids=sample_ids)
        with open(args.filepath, "w") as fout:
            json.dump(result, fout, indent=1)
        print(f"wrote export to {args.filepath}.")

    def _handle_search(self, args):
        result = self.client.search_families(args.search_term)
        if result["search_results"]:
            print("Family Search Results")
            for family_id, entry in result["search_results"].items():
                family_entry = FamilyEntry.fromDict(entry)
                print(f"{family_entry}")
            print("*" * 20)
        result = self.client.search_samples(args.search_term)
        if result["search_results"]:
            print("Sample Search Results")
            for sample_id, entry in result["search_results"].items():
                sample_entry = SampleEntry.fromDict(entry)
                print(f"{sample_entry}")
            print("*" * 20)
        result = self.client.search_functions(args.search_term)
        if result["search_results"]:
            print("Function Search Results")
            for function_id, entry in result["search_results"].items():
                function_entry = FunctionEntry.fromDict(entry)
                print(f"{function_entry}")

    def _handle_queue(self, args):
        result = self.client.getQueueData(filter=args.filter) or []
        for entry in result:
            job_id = entry.job_id
            result_id = entry.result
            created = entry.created_at if entry.created_at is not None else "-"
            started = entry.started_at if entry.started_at is not None else "-"
            finished = entry.finished_at if entry.finished_at is not None else "-"
            method = entry.parameters
            progress = entry.progress
            print(f"{job_id} {result_id} | {created} {started} {finished} | {method} - {progress}")

    def _handle_query(self, args):
        # run a number of sanity checks first
        if not os.path.exists(args.filepath):
            print("Your <filepath> does not exist.")
            return
        base_addr = None
        if args.base_addr is not None:
            try:
                if args.base_addr.startswith("0x"):
                    base_addr = int(args.base_addr, 16)
                else:
                    base_addr = int(args.base_addr)
            except ValueError:
                print("base_addr has invalid format.")
                return
        if args.bitness is not None and args.bitness not in ["32", "64"]:
            print("Invalid value for bitness provided.")
            return
        if args.output is not None and (not os.path.exists(args.output) or not os.path.isdir(args.output)):
            print("Your <output> is not a directory or does not exist.")
            return
        if args.smda:
            smda_report = SmdaReport.fromFile(args.filepath)
            job_id = self.client.requestMatchesForSmdaReport(smda_report, force_recalculation=args.force_recalculation)
        else:
            if base_addr:
                job_id = self.client.requestMatchesForMappedBinary(
                    readFileContent(args.filepath), disassemble_locally=False, base_address=base_addr, force_recalculation=args.force_recalculation
                )
            else:
                job_id = self.client.requestMatchesForUnmappedBinary(readFileContent(args.filepath), disassemble_locally=False, force_recalculation=args.force_recalculation)
        print(f"Started job: {job_id}, waiting for result...")
        compact_result_dict = self.client.awaitResult(job_id, sleep_time=2, compact=True)
        if args.output is not None:
            with open(args.output + os.sep + f"{job_id}.json", "w") as fout:
                json.dump(compact_result_dict, fout, indent=1)
        result = MatchingResult.fromDict(compact_result_dict)
        print(f"| {'Family':>30} | {'Version':>20} | {'Sample':>6} | {'SHA256':>8} | {'Func':>5} | {'Min':>5} | {'Pic':>5} | {'Lib':>5} | {'Direct':>13} | {'Freq':>13} |")
        print(
            "|" + "-" * 32 + "|" + "-" * 22 + "|" + "-" * 8 + "|" + "-" * 10 + "|" + "-" * 7 + "|" + "-" * 7 + "|" + "-" * 7 + "|" + "-" * 7 + "|" + "-" * 15 + "|" + "-" * 15 + "|"
        )
        for family_result in result.getBestSampleMatchesPerFamily(limit=20, malware_only=True):
            result_line = f"| {family_result.family:>30} | "
            result_line += f"{family_result.version:>20} | "
            result_line += f"{family_result.sample_id:>6} | "
            result_line += f"{family_result.sha256[:8]} | "
            result_line += f"{family_result.num_functions:>5} | "
            result_line += f"{family_result.matched_functions_minhash:>5} | "
            result_line += f"{family_result.matched_functions_pichash:>5} | "
            result_line += f"{family_result.matched_functions_library:>5} | "
            result_line += f"{family_result.matched_percent_score_weighted:>6.2f} {family_result.matched_percent_nonlib_score_weighted:>6.2f} | "
            result_line += f"{family_result.matched_percent_frequency_weighted:>6.2f} {family_result.matched_percent_nonlib_frequency_weighted:>6.2f} | "
            print(result_line)

    def _handle_submit(self, args):
        # run a number of sanity checks first
        if not os.path.exists(args.filepath):
            print("Your <filepath> does not exist.")
            return
        args.filepath = os.path.abspath(args.filepath)
        if args.mode == "file" and not os.path.isfile(args.filepath):
            print("Mode <file> only works when <filepath> is a file.")
            return
        if args.mode in ["dir", "recursive", "malpedia"] and not os.path.isdir(args.filepath):
            print("Modes <dir|recursive|malpedia> only work when <filepath> is a directory.")
            return
        if args.output is not None and (not os.path.exists(args.output) or not os.path.isdir(args.output)):
            print("Your <output> is not a directory or does not exist.")
            return
        if args.smda and args.mode in ["recursive", "malpedia"]:
            print("Modes <recursive|malpedia> are not compatible with SMDA report loading.")
            return
        if args.worker and args.mode not in ["dir", "recursive", "malpedia"]:
            print("Mode <worker> only works with modes: dir/recursive/malpedia.")
            return
        # behavior according to the modes offered
        if args.mode == "file":
            self._handle_submit_file(args)
        if args.mode == "dir":
            self._handle_submit_dir(args)
        if args.mode == "recursive":
            self._handle_submit_recursive(args)
        if args.mode == "malpedia":
            self._handle_submit_malpedia(args)

    def _update_sample_metadata(self, sample_entry, args):
        sample_id = sample_entry.sample_id
        update_required = False
        modified_family = args.family if args.family is not None else sample_entry.family
        modified_version = args.version if args.version is not None else sample_entry.version
        modified_is_library = True if args.library else sample_entry.is_library
        if args.family is not None and args.family != sample_entry.family:
            update_required = True
        if args.version is not None and args.version != sample_entry.version:
            update_required = True
        if args.library and not sample_entry.is_library:
            update_required = True
        if update_required:
            print(f"UPDATING: Sample ID {sample_entry.sample_id} with SHA256: {sample_entry.sha256} - updating family/version/library.")
            self.client.modifySample(sample_id, family_name=modified_family, version=modified_version, is_library=modified_is_library)

    def _handle_submit_file(self, args):
        sample_sha256 = sha256(readFileContent(args.filepath))
        sample_entry = self.client.getSampleBySha256(sample_sha256)
        if sample_entry:
            if args.force_update:
                self._update_sample_metadata(sample_entry, args)
            else:
                print(f"SKIPPING: {args.filepath} - already in MCRIT.")
            return
        smda_report = getSmdaReportFromFilepath(args, args.filepath)
        if smda_report:
            print(smda_report)
            self.client.addReport(smda_report)

    def _handle_submit_dir(self, args):
        mcrit_sample_entries = self.client.getSamples() or {}
        mcrit_samples_by_sha256 = {}
        for sample_id, sample_entry in mcrit_sample_entries.items():
            mcrit_samples_by_sha256[sample_entry.sha256] = sample_entry
        for filename in os.listdir(args.filepath):
            filepath = os.sep.join([args.filepath, filename])
            if os.path.isfile(filepath):
                if args.worker:
                    submitViaSubprocess(args, filepath)
                else:
                    sample_sha256 = sha256(readFileContent(filepath))
                    if args.smda:
                        if not is_smda_report(filepath):
                            print(f"Skipping a file not recognized as SMDA report: {filepath}")
                            continue
                        primary_meta = get_primary_smda_meta_data(filepath)
                        if not primary_meta:
                            print(f"Skipping a file that where SMDA meta data extraction failed: {filepath}")
                            continue
                        if primary_meta["sha256"] in mcrit_samples_by_sha256:
                            if (
                                primary_meta["family"] is not None
                                and primary_meta["family"] == mcrit_samples_by_sha256[primary_meta["sha256"]].family
                                and primary_meta["version"] is not None
                                and primary_meta["version"] == mcrit_samples_by_sha256[primary_meta["sha256"]].version
                            ):
                                print(
                                    f"SKIPPING: {os.path.basename(filepath)} - with SHA256: {primary_meta['sha256']} - already in MCRIT as ({primary_meta['family']}|{primary_meta['version']})."
                                )
                                continue
                            else:
                                print(
                                    f"WARNING/SKIPPING: SHA256: {primary_meta['sha256']} - already in MCRIT but different family/version (FILE: ({primary_meta['family']}|{primary_meta['version']}) - MCRIT: ({mcrit_samples_by_sha256[primary_meta['sha256']].family}|{mcrit_samples_by_sha256[primary_meta['sha256']].version})."
                                )
                                continue
                    elif sample_sha256 in mcrit_samples_by_sha256:
                        sample_entry = mcrit_samples_by_sha256[sample_sha256]
                        if args.force_update:
                            self._update_sample_metadata(sample_entry, args)
                        else:
                            print(f"SKIPPING: {args.filepath} - already in MCRIT.")
                        continue
                    smda_report = getSmdaReportFromFilepath(args, filepath)
                    if smda_report and smda_report.xcfg:
                        print(smda_report)
                        self.client.addReport(smda_report)

    def _handle_submit_recursive(self, args):
        mcrit_samples = self.client.getSamples() or {}
        mcrit_samples_by_sha256 = {}
        for sample_id, sample in mcrit_samples.items():
            mcrit_samples_by_sha256[sample.sha256] = sample
        for root, subdir, files in sorted(os.walk(args.filepath)):
            folder_relative_path = getFolderFilePath(args.filepath, root)
            for filename in files:
                filepath = os.sep.join([root, filename])
                if os.path.isfile(filepath):
                    if sha256(readFileContent(filepath)) in mcrit_samples_by_sha256:
                        print(f"SKIPPING: {filepath} - already in MCRIT.")
                        continue
                    family_name = getFamilyName(folder_relative_path)
                    version = getSampleVersion(folder_relative_path, family_name)
                    print("Processing file: ", filepath, " as ", family_name, "|", version)
                    continue
                    if args.worker:
                        args.family = getFamilyName(folder_relative_path)
                        args.version = getSampleVersion(folder_relative_path, args.family)
                        submitViaSubprocess(args, filepath)
                    else:
                        smda_report = getSmdaReportFromFilepath(args, filepath)
                        if smda_report:
                            smda_report.family = getFamilyName(folder_relative_path)
                            smda_report.version = getSampleVersion(folder_relative_path, smda_report.family)
                            print(filepath)
                            print(smda_report)
                            self.client.addReport(smda_report)

    def _handle_submit_malpedia(self, args):
        # verify that we have a malpedia root
        malpedia_root = os.path.abspath(args.filepath)
        malpedia_root = malpedia_root.rstrip("/")
        if not malpedia_root.endswith("malpedia"):
            print("Error: You pointing to a folder named differently than 'malpedia'.")
            return
        if "malpedia.bib" not in os.listdir(malpedia_root):
            print("Error: 'malpedia.bib' is missing in that folder, are you sure you are poniting to a Malpedia repository?")
            return
        # get current status of all samples in MCRIT
        mcrit_samples = self.client.getSamples() or {}
        mcrit_samples_by_filename = {}
        for sample_id, sample in mcrit_samples.items():
            # verify that filename has malpedia format (starts with sha256 and _unpacked/_dump)
            if sample.filename in mcrit_samples_by_filename:
                print(f"WARNING: filename {sample.filename} appears to exist more than once in your MCRIT instance, now using SHA256 {sample.sha256[:8]}.")
            mcrit_samples_by_filename[sample.filename] = sample
        # get all unpacked/dumped files and their family/version by crawling given Malpedia location
        malpedia_samples_by_filename = self._getMalpediaSamplesByFilename(malpedia_root)
        # use submission filenames within MCRIT to check for presence of files and verify their family/version identity
        for filename, malpedia_info in malpedia_samples_by_filename.items():
            malpedia_family = malpedia_info["family"]
            malpedia_version = malpedia_info["version"]
            malpedia_filepath = malpedia_info["filepath"]
            if filename in mcrit_samples_by_filename:
                mcrit_family = mcrit_samples_by_filename[filename].family
                mcrit_version = mcrit_samples_by_filename[filename].version
                # warn about files were family/version mismatches.
                if malpedia_family != mcrit_family or malpedia_version != mcrit_version:
                    print(f"WARNING: Sample {mcrit_samples_by_filename[filename].sample_id} with filename {filename} has different family/version information: ")
                    print(f"* Malpedia: {malpedia_family}|{malpedia_version}")
                    print(f"* MCRIT: {mcrit_family}|{mcrit_version}")
            # directly add all files in Malpedia but missing in MCRIT
            else:
                if args.worker:
                    args.family = malpedia_family
                    args.version = malpedia_version
                    submitViaSubprocess(args, malpedia_filepath)
                else:
                    smda_report = getSmdaReportFromFilepath(args, malpedia_filepath)
                    if smda_report:
                        smda_report.family = malpedia_family
                        smda_report.version = malpedia_version
                        print(malpedia_filepath)
                        print(smda_report)
                        self.client.addReport(smda_report)
        # warn about files that appear deleted because not present in Malpedia but in MCRIT (based on name schema)
        for filename, mcrit_sample in mcrit_samples_by_filename.items():
            if self._isMalpediaFilename(filename) and filename not in malpedia_samples_by_filename:
                print(f"WARNING: Sample {mcrit_sample.sample_id} with filename {filename} ({mcrit_sample.family}|{mcrit_sample.version}) present in MCRIT but not in Malpedia?")

    def _getMalpediaSamplesByFilename(self, malpedia_root):
        malpedia_samples_by_filename = {}
        for root, subdir, files in sorted(os.walk(malpedia_root)):
            if "win." not in root or "elf." in root:
                continue
            folder_relative_path = getFolderFilePath(malpedia_root, root)
            for filename in sorted(files):
                if self._isMalpediaFilename(filename):
                    filepath = root + os.sep + filename
                    sample_family = getFamilyName(folder_relative_path)
                    malpedia_samples_by_filename[filename] = {
                        "filename": filename,
                        "filepath": filepath,
                        "family": sample_family,
                        "version": getSampleVersion(folder_relative_path, sample_family),
                    }
        return malpedia_samples_by_filename

    def _isMalpediaFilename(self, filename):
        malpedia_file_pattern = re.compile("^[0-9a-f]{64}(_unpacked|dump7?_0x[0-9a-fA-F]{8,16})")
        return re.search(malpedia_file_pattern, filename)
