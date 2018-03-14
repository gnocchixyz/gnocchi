# Copyright (c) 2018 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import pkg_resources
import platform
import shutil
import subprocess
import sys


def usage(msg, subsystem_mapping):
    print("%s Valid subcommands are make-links, %s" %
          (msg, ", ".join(subsystem_mapping.keys())))
    return 1


def bootstrap():
    subsystem_mapping = dict(
        (ep.name, ep)
        for ep in pkg_resources.iter_entry_points('console_scripts')
        if ep.module_name.startswith("gnocchi.")
    )

    # NOTE(sileht): gnocchi-api is a script and not an entrypoint
    subsystem_mapping["gnocchi-api"] = pkg_resources.EntryPoint.parse(
        "api = gnocchi.cli.api:api")

    binary = os.path.basename(sys.argv[0])
    if binary in subsystem_mapping:
        # NOTE(sileht): Started from a known symlink name
        return subsystem_mapping[binary].load(require=False)()

    elif len(sys.argv) < 2:
        return usage("A subcommand must be passed.",
                     subsystem_mapping)

    else:
        subsystem = "gnocchi-%s" % sys.argv[1]
        if subsystem == "gnocchi-make-links":
            for s in subsystem_mapping:
                os.symlink(binary, s)
                print("%s symlink created" % s)
            return 0
        elif subsystem in subsystem_mapping:
            sys.argv = [subsystem] + sys.argv[2:]
            return subsystem_mapping[subsystem].load(require=False)()
        else:
            return usage("Subcommand %s is invalid." % subsystem,
                         subsystem_mapping)


def build(path="dist", version=None):
    if not os.getenv("VIRTUAL_ENV"):
        raise RuntimeError("Expected to be run into a tox virtualenv")

    # NOTE(sileht): Less binary wheel the zip will have, lower the issues
    # due to library compatibility will be.
    #
    # Here the current list of platform dependent wheels for py35:
    # * cradox-2.0.2-cp35-cp35m-linux_x86_64.whl
    # * lz4-1.0.0-cp35-cp35m-linux_x86_64.whl
    # * MarkupSafe-1.0-cp35-cp35m-linux_x86_64.whl
    # * msgpack-0.5.6-cp35-cp35m-linux_x86_64.whl
    # * netifaces-0.10.6-cp35-cp35m-linux_x86_64.whl
    # * numpy-1.14.2-cp35-cp35m-linux_x86_64.whl
    # * psycopg2-2.7.4-cp35-cp35m-linux_x86_64.whl
    # * PyYAML-3.12-cp35-cp35m-linux_x86_64.whl
    # * setproctitle-1.1.10-cp35-cp35m-linux_x86_64.whl
    # * SQLAlchemy-1.2.5-cp35-cp35m-linux_x86_64.whl
    # * ujson-1.35-cp35-cp35m-linux_x86_64.whl
    # * wrapt-1.10.11-cp35-cp35m-linux_x86_64.whl

    # FIXME(sileht): We shouldn't build cradox because, it depends on the
    # librados2 installed but if we don't, we have to allow site package, and
    # the build may not have all dependencies.
    extras = "mysql,postgresql,redis,keystone,swift,s3,ceph,prometheus"

    pyvers = "py%s%s" % sys.version_info[0:2]

    if version is None:
        src = ".[%s]" % extras
        from setuptools_scm import get_version
        version = get_version(root='..', relative_to=__file__)
    else:
        src = "gnocchi==%s[%s]" % (version, extras)

    output = "%s/gnocchi-%s-%s-%s_%s" % (path, version, pyvers,
                                         platform.system(),
                                         platform.machine())
    print(output)

    cachedir = "%s/../../.pex" % os.getenv("VIRTUAL_ENV")
    cache = "--cache-dir=%s" % cachedir

    # Ensure we don't reuse a already built wheel of Gnocchi
    if os.path.exists("gnocchi.egg-info"):
        shutil.rmtree("gnocchi.egg-info")
    if os.path.exists(cachedir):
        for name in os.listdir(cachedir):
            if name.startswith("gnocchi-"):
                os.unlink("%s/%s" % (cachedir, name))

    p = subprocess.Popen(["pex", src, "-v", cache,
                          "-m", "gnocchi.pex:bootstrap", "-o", output])
    if p.wait() != 0:
        raise Exception("Fail to generate binary")

    return output


if __name__ == "__main__":
    sys.exit(build())
