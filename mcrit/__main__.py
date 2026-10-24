import os
import re
import sys
from typing import Any


def runWorker(profiling=False):
    from mcrit.Worker import Worker

    with Worker(profiling=profiling) as worker:
        worker.run()


def runSpawningWorker(profiling=False):
    from mcrit.SpawningWorker import SpawningWorker

    with SpawningWorker(profiling=profiling) as spawning_worker:
        spawning_worker.run()


def runSingleJobWorker(job_id, profiling=False):
    from mcrit.SingleJobWorker import SingleJobWorker

    with SingleJobWorker(job_id, profiling=profiling) as single_job_worker:
        single_job_worker.run()


def normalizeBind(bind: str) -> str:
    """A bind without a host (":8000") means every interface to gunicorn and is refused by waitress: read it as loopback."""
    bind = bind.strip()
    return "127.0.0.1" + bind if bind.startswith(":") else bind


def runServer(profiling=False, uses_gunicorn=False):
    import platform

    from waitress import serve

    from mcrit.config.GunicornConfig import GunicornConfig
    from mcrit.server.wsgi import app

    # gunicorn is not installed on every platform (e.g. Windows), so the server class
    # below can only be defined when the import succeeded.
    BaseApplication: Any = None
    try:
        from gunicorn.app.base import BaseApplication as _GunicornBaseApplication

        BaseApplication = _GunicornBaseApplication
    except Exception:
        pass

    bind = normalizeBind(GunicornConfig().BIND)
    if bind != GunicornConfig().BIND.strip():
        print(f"[!] The configured bind address has no host, listening on {bind} only. Use 0.0.0.0:<port> to serve other hosts.")

    def buildGunicornServer(wsgi_app):
        assert BaseApplication is not None
        gunicorn_base: Any = BaseApplication

        class gunicornServer(gunicorn_base):
            def __init__(self, app):
                self.app = app
                super().__init__()

            def load_config(self):
                if self.cfg is None:
                    return
                for key, value in GunicornConfig().toDict().items():
                    try:
                        self.cfg.set(key.lower(), bind if key == "BIND" else value)
                    except AttributeError:
                        continue

            def load(self):
                return self.app

        return gunicornServer(wsgi_app)

    wrapped_app = app
    if profiling:
        print("[!] Running as profiled application.")
        from werkzeug.middleware.profiler import ProfilerMiddleware

        profile_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "profiler")
        os.makedirs(profile_dir, exist_ok=True)
        wrapped_app = ProfilerMiddleware(
            wrapped_app,
            restrictions=[30],
            profile_dir=profile_dir,
            filename_format="{method}-{path}-{time:.0f}-{elapsed:.0f}ms.prof",
        )

    platform_name = platform.system().lower()
    if platform_name == "linux" and BaseApplication is not None and (GunicornConfig().USE_GUNICORN or uses_gunicorn):
        print("[!] Detected linux platform and gunicorn availability. Using gunicorn deployment.")
        buildGunicornServer(wrapped_app).run()
        sys.exit()
    elif platform_name == "windows":
        print("[!] Detected windows platform. Using waitress deployment.")
    else:
        print("[!] Could not determine platform, gunicorn not available or activated. Defaulting to waitress deployment.")
    # TODO consider allowing an argument to pass an configuration for initial setup of the instance
    serve(wrapped_app, listen=bind)


def runClient():
    from mcrit.client.McritConsole import McritConsole

    # we will re-parse arguments with more detail in McritConsole to keep this file concise
    console_client = McritConsole()
    console_client.run()


def main():
    # do not use argparse for processing here to allow using argparse help for the more intricate things in McritConsole
    if len(sys.argv) >= 2:
        is_profiling = False
        uses_gunicorn = False
        if len(sys.argv) >= 3 and "--profile" in sys.argv[2:]:
            is_profiling = True
        if len(sys.argv) >= 3 and "--gunicorn" in sys.argv[2:]:
            uses_gunicorn = True
        if sys.argv[1] in ["server", "worker", "spawningworker", "singlejobworker"]:
            # for good, before anything is spawned: the pools, the resource trackers and the replacements for
            # crashed pool processes of these long-running commands all start as `python -c` with the working
            # directory first on sys.path, unless they inherit this
            os.environ["PYTHONSAFEPATH"] = "1"
        if sys.argv[1] == "server":
            runServer(profiling=is_profiling, uses_gunicorn=uses_gunicorn)
        elif sys.argv[1] == "worker":
            runWorker(profiling=is_profiling)
        elif sys.argv[1] == "spawningworker":
            runSpawningWorker(profiling=is_profiling)
        elif sys.argv[1] == "singlejobworker":
            # parse job_id directly from full command line string
            job_id = None
            match = re.search("--job_id (?P<job_id>[0-9a-fA-F]{24})", " ".join(sys.argv))
            if match:
                job_id = match.group("job_id")
                runSingleJobWorker(job_id, profiling=is_profiling)
            else:
                print("Was not able to recognize a job_id (24char hex string), did you pass it using --job_id?")
        elif sys.argv[1] == "client":
            runClient()
        else:
            print("Unrecognized command, please use {{server, worker, spawningworker, client}}")
    else:
        print(f"Usage: {sys.argv[0]} {{server, worker, spawningworker, client}}")
        print("Optionally use --profiling for {server, worker, spawningworker}")
        sys.exit()


if __name__ == "__main__":
    main()
