# MCRIT CLI

In this document we describe the different ways of interaction enabled by the MCRIT console client.

## Submit

The `submit` command supports 4 methods of data submission, supported by a number of auxiliary flags.
Here is its documentation:

```
usage: mcrit client submit [-h] [--mode {file,dir,recursive,malpedia}] [-f FAMILY] [-v VERSION] [-l] [-u] [-x]
                           [-o OUTPUT] [-s] [--disassembler {smda,ida}] [--ida-sigs IDA_SIGS]
                           [--ida-sig-min-matches IDA_SIG_MIN_MATCHES] [-w] [-t WORKER_TIMEOUT]
                           filepath

positional arguments:
  filepath              Submit the folllowing <filepath>, indicating a (file/dir).

options:
  -h, --help            show this help message and exit
  --mode {file,dir,recursive,malpedia}
                        Submit a single <file> or all files in a <dir>. Use <recursive> submission for a folder
                        structured as ./family_name/version/version/files. Synchronize <malpedia> into MCRIT. Default:
                        <file>.
  -f, --family FAMILY   Set/Override SmdaReport with this family (only in modes: file/dir)
  -v, --version VERSION
                        Set/Override SmdaReport with this version (only in modes: file/dir)
  -l, --library         Set/Override SmdaReport with the library flag (only in modes: file/dir/recursive, default:
                        False).
  -u, --force_update    Force update of family/version/library, even if file is already in MCRIT (only in modes:
                        file/dir, default: False).
  -x, --executables_only
                        Only process files that are parsable PE or ELF files (default: False).
  -o, --output OUTPUT   Optionally store SMDA reports in folder OUTPUT, which is created if not existing.
  -s, --smda            Do not disassemble, instead only submit files that are recognized as SMDA reports (only works
                        with modes: file/dir).
  --disassembler {smda,ida}
                        Disassembler used to produce reports for submitted files (default: smda).
  --ida-sigs IDA_SIGS   Root folder of an unpacked FLIRT signature bundle, probed per sample and applied where enough
                        functions match (only with --disassembler ida).
  --ida-sig-min-matches IDA_SIG_MIN_MATCHES
                        Number of functions a FLIRT signature must name to be kept (only with --disassembler ida,
                        default: 10).
  -w, --worker          Spawn workers to process the submission (only in modes: dir/recursive/malpedia, default:
                        False).
  -t, --worker-timeout WORKER_TIMEOUT
                        Timeout for workers to conclude the submission (default: 300 seconds).
```

### File

Submit a single file.
If the provided path ends with `0x[0-9a-fA-F]{8,16}`, the client will assume that this is a mapped file (no evaluation of PE header) and use the given addr as IMAGEBASE.
```bash
$ mcrit client submit --mode file ~/malpedia/win.wannacryptor/vt-2017-05-05/0345782378ee7a8b48c296a120625fd439ed8699ae857c4f84befeb56e727366_dump_0x00400000
 0.906s -> (architecture: intel.32bit, base_addr: 0x00400000): 922 functions
```

### Directory

Submit all files in a directory (no recursion).

```bash
$ mcrit client submit --mode dir ~/malpedia/win.wannacryptor/vt-2017-05-12/
 0.763s -> (architecture: intel.32bit, base_addr: 0x00400000): 926 functions
 0.884s -> (architecture: intel.32bit, base_addr: 0x00400000): 926 functions
 1.378s -> (architecture: intel.32bit, base_addr: 0x00400000): 165 functions
 0.830s -> (architecture: intel.32bit, base_addr: 0x00400000): 926 functions
 ```

 ### Recursive

 Recursively submit all files found in a directory, assuming a structure like
 ```
 ./family_name/version/version/files
 ```
 and use `family_name` and (optionally, if nested enough) `version` as fields in MCRIT.


 ### Malpedia

Dedicated mode to synchronize all data available in the Malpedia repository.
If files from Malpedia are already available in MCRIT, they will be skipped on the next execution, making this command suitable to synchronize updated states of Malpedia into MCRIT.
Only ELF and PE (win.*) families and only files labeled as `_unpacked` or `_dump_0x...`  will be considered and processed.
Requires directory name to be called `malpedia`, sanity checks if there is a `malpedia.bib` file present as in the repo.

 ```
 $ mcrit client submit --mode malpedia ~/malpedia
/home/analyst/work/Repositories/malpedia/win.3cx_backdoor/11be1803e2e307b647a8a7e02d128335c448ff741bf06bf52b332e0bbf423b03_unpacked
 1.625s -> (architecture: intel.64bit, base_addr: 0x180000000): 717 functions
/home/analyst/work/Repositories/malpedia/win.8t_dropper/2019-01-23/b541e0e29c34800a067b060d9ee18d8d35c75f056f4246b1ce9561a5441d5a0f_unpacked
 0.305s -> (architecture: intel.32bit, base_addr: 0x10000000): 213 functions
[...]
```

### Using IDA Pro as the disassembler

With `--disassembler ida`, files are disassembled by IDA Pro running headlessly instead of by SMDA.
This requires:

* IDA Pro 9.1 or newer, with its licence accepted once in the GUI (the headless library reuses that acceptance),
* `pip install "mcrit[ida]"`,
* the environment variable `IDADIR` pointing at the IDA installation directory that contains `libidalib`.

IDA's auto-analysis is exported to an SMDA report exactly as the MCRIT IDA plugin does, and that report
is then submitted through the normal path - so skipping files already present, `--force_update`,
`--output` and the family/version derivation of the `dir`/`recursive`/`malpedia` modes all behave as
they do with SMDA. Reports produced this way carry `smda_version` `"MCRIT4IDA cli via SMDA <version>"`.

Each file is processed in its own subprocess, because the headless IDA library holds a single database
per process and one malformed sample must not end the batch. Worker mode is therefore switched on
automatically for the `dir`, `recursive` and `malpedia` modes; raise `--worker-timeout` for large files
or when using `--ida-sigs`, as a timed-out file is skipped without being submitted.

`--ida-sigs` points at an unpacked Hex-Rays FLIRT Signature Bundle, i.e. the folder that contains the
top-level `windows/ linux/ golang/ rust/` directories. Candidate signatures are narrowed by the file
format and architecture of the sample, then each candidate is applied and kept only if it names at
least `--ida-sig-min-matches` functions, and undone otherwise.

Caveats:

* FLIRT results reach MCRIT only as function names, which the server stores as function labels attributed
  to the submitting user. There is no per-function library flag - `--library` remains a per-sample property.
* The sample is copied to a temporary directory before analysis, so no IDA database files appear next to it.
* IDA loads a file of no format it recognises as a raw binary instead of refusing it, so a file in which
  it finds no functions is skipped rather than stored as an empty sample.
* If the `ida-domain` package is missing, the run stops with an install hint before touching any file.
* Base address suffixes in filenames (`..._dump_0x00400000`) are ignored; IDA's loader decides the base address.

```bash
$ mcrit client submit ./samples --mode recursive --disassembler ida --ida-sigs /path/to/signatures-bundle -t 1800
```


## Export

This command can be used to export samples into MCRIT-compatible files, e.g.:

```bash
usage: mcrit client export [-h] [--sample_ids SAMPLE_IDS] filepath

$ mcrit client export --sample_ids 0 sample_0.mcrit
wrote export to sample_0.mcrit.

$ head sample_0.mcrit
{
 "content": {
  "is_compressed": true,
  "num_families": 1,
  "num_samples": 1,
  "num_functions": 214
 },
 "config": {
  "version": "0.19.0",
  "shingler": "7ae53d3b2514730a4d48f993a3e4cd6c6d4a5ca26f93bbed98e0f498295552de",
[...]
```

## Import

This command can be used to import previously exported sample(s), e.g.:

```bash
usage: mcrit client import [-h] filepath

$ mcrit client import sample_0.mcrit
{'num_samples_imported': 0, 'num_samples_skipped': 1, 'num_functions_imported': 0, 'num_functions_skipped': 214, 'num_families_imported': 0, 'num_families_skipped': 1}

```

## Search

This command can be used to search across families, samples, and functions, e.g.:

```bash
usage: mcrit client search [-h] search_term

$ mcrit client search wanna
Family Search Results
Famliy 2 (win.wannacry):
********************
Sample Search Results
Sample 1 (intel, 32 bit) - ca29de1dc8817868c93e54b09f557fe14e40083c0955294df5bd91f52ba469c8_unpacked (win.wannacry):
Sample 2 (intel, 32 bit) - 3e6de9e2baacf930949647c399818e7a2caea2626df6a468407854aaa515eed9 (win.wannacry):
********************
```

## Queue

This command can be used to get a view on all queued jobs and their processing status, e.g.:

```bash
$ mcrit client queue
64243b27f3876416bffad86e 64243b28cbc77c2df4d8d79f | 2023-03-29T13:20:39.065Z 2023-03-29T13:20:39.114Z 2023-03-29T13:20:40.593Z | updateMinHashesForSample(2) - 1
64131888fbb4d9d4a029164d 6413188c15e4f20d519b35ba | 2023-03-16T13:24:24.707Z 2023-03-16T13:24:24.755Z 2023-03-16T13:24:28.366Z | addBinarySample(None, ca29de1dc8817868c93e54b09f557fe14e40083c0955294df5bd91f52ba469c8_unpacked, win.wannacry, , False, 0, 32) - 1
641316eefbb4d9d4a029164a 641316f115e4f20d519b322b | 2023-03-16T13:17:34.834Z 2023-03-16T13:17:34.859Z 2023-03-16T13:17:37.238Z | addBinarySample(None, 766d7d591b9ec1204518723a1e5940fd6ac777f606ed64e731fd91b0b4c3d9fc_dump_0x10000000, win.contopee, , True, 268435456, 32) - 1
```
