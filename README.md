# MinHash-based Code Relationship & Investigation Toolkit (MCRIT)
[![Test](https://github.com/danielplohmann/mcrit/actions/workflows/test.yml/badge.svg)](https://github.com/danielplohmann/mcrit/actions/workflows/test.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/danielplohmann/mcrit)

MCRIT is a framework created to simplify the application of the MinHash algorithm in the context of code similarity.
It can be used to rapidly implement "shinglers", i.e. methods which encode properties of disassembled functions, to then be used for similarity estimation via the MinHash algorithm.
It is tailored to work with disassembly reports emitted by [SMDA](https://github.com/danielplohmann/smda).

## Usage

### Dockerized Usage

We highly recommend to use the fully packaged [docker-mcrit](https://github.com/danielplohmann/docker-mcrit) for trivial deployment and usage.
First and foremost, this will ensure that you have fully compatible versions across all components, including a database for persistence and a web frontend for convenient interaction.

### Standalone Usage

Installing MCRIT on its own will require some more steps.
For the following, we assume Ubuntu as host operating system.

MCRIT requires **Python 3.12 or newer**. Its dependencies are declared in `pyproject.toml` and are installed together with the package:

```bash
# install python and MCRIT along with its dependencies
$ sudo apt install python3 python3-pip
$ pip install -e .
```

By default, MongoDB 5.0 is used as backend, which is also the recommended mode of operation as it provides a persistent data storage.
The following commands outline an example installation on Ubuntu:
```bash
# fetch mongodb signing key
$ sudo apt-get install gnupg
$ wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | sudo apt-key add -
# add package repository (Ubuntu 22.04)
$ echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list
# OR add package repository (Ubuntu 20.04)
$ echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list
# OR add package repository (Ubuntu 18.04)
$ echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list
# install mongodb
$ sudo apt-get update
$ sudo apt-get install -y mongodb-org
# start mongodb as a service
$ sudo systemctl start mongod
# optionally configure to start the service with system startup
$ sudo systemctl enable mongod
```

When doing the standalone installation, you possibly want to install the MCRIT module based on the cloned repository, like so:

```bash
$ pip install -e .
```

After this initial installation and if desired, MCRIT can be used without an internet connection.


#### Operation

The MCRIT backend is generally divided into two components, a server providing an API interface to work with and one or more workers processing queued jobs.
They can be started in seperate shells using:

```bash
$ mcrit server
```

and

```bash
$ mcrit worker
```

By default, the REST API server will be listening on [http://127.0.0.1:8000/](http://127.0.0.1:8000/).


## Interaction

Regardless of your choice for installation, once running you can interact with the MCRIT backend.


### MCRIT Client

We have created a Python client module that is capable of working with all available endpoints of the server.
Documentation for this client module is currently in development.

### MCRIT CLI

There is also a CLI which is based on this client package, examples:

```bash
# query some stats of the data stored in the backend
$ mcrit client status
{'status': {'db_state': 187, 'storage_type': 'mongodb', 'num_bands': 20, 'num_samples': 137, 'num_families': 14, 'num_functions': 129110, 'num_pichashes': 25385}}
# submit a malware sample with filename sample_unpacked, using family name "some_family"
$ mcrit client submit sample_unpacked -f some_family
 1.039s -> (architecture: intel.32bit, base_addr: 0x10000000): 634 functions
```

A more extensive documentation of the MCRIT CLI is available [here](docs/mcrit-cli.md)

### MCRIT IDA Plugin

An IDA plugin is also currently under development.
To use it, first create your own config.py and make required changes depending on the deployment of your MCRIT instance:
```
cp ./plugins/ida/template.config.py ./plugins/ida/config.py
nano ./plugins/ida/config.py
```

Then simply run the script found at

```
./plugins/ida/ida_mcrit.py
```

in IDA.

### Reference Data

In July 2023, we started populating a [Github repository](https://github.com/danielplohmann/mcrit-data) which contains ready-to-use reference data for common compilers and libraries.


## Known Limitations

See [docs/limitations.md](docs/limitations.md) for what MCRIT's model deliberately does not capture.

## Version History

See [CHANGELOG.md](CHANGELOG.md) for the full release history.

## Credits & Notes

Thanks to Steffen Enders and Paul Hordiienko for their contributions to the internal research prototype of this project!
Thanks to Manuel Blatt for his extensive contributions to and refactorings of this project as well as for the client module!

Pull requests welcome! :)

## License
```
    MinHash-based Code Relationship & Investigation Toolkit (MCRIT)
    Copyright (C) 2022  Daniel Plohmann, Manuel Blatt

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    Some plug-ins and libraries may have different licenses.
    If so, a license file is provided in the plug-in's folder.
```
