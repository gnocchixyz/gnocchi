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


from __future__ import absolute_import

import os
import subprocess

from distutils import version
from setuptools.command import develop
from setuptools.command import easy_install
from setuptools.command import egg_info
from setuptools.command import install_scripts

# NOTE(sileht): We use a template to set the right
# python version in the sheban
SCRIPT_TMPL = """
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
import sys
from gnocchi.cli import api

if __name__ == '__main__':
    sys.exit(api.api())
else:
    application = api.wsgi()
"""


def git(*args):
    p = subprocess.Popen(["git"] + list(args),
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    out, _ = p.communicate()
    return out.strip().decode('utf-8', 'replace')


class local_egg_info(egg_info.egg_info):
    def run(self):
        if os.path.exists(".git"):
            self._gen_changelog_and_authors()
        egg_info.egg_info.run(self)

    @staticmethod
    def _gen_changelog_and_authors():
        with open("AUTHORS", 'wb') as f:
            authors = git('log', '--format=%aN <%aE>')
            authors = sorted(set(authors.split("\n")))
            f.writelines([b"%s\n" % author.encode('utf8')
                          for author in authors])

        with open("ChangeLog", "wb") as f:
            f.write(b"CHANGES\n")
            f.write(b"=======\n\n")
            changelog = git('log', '--decorate=full', '--format=%s%x00%d')
            for line in changelog.split('\n'):
                msg, refname = line.split("\x00")

                if "refs/tags/" in refname:
                    refname = refname.strip()[1:-1]  # remove wrapping ()'s
                    # If we start with "tag: refs/tags/1.2b1, tag:
                    # refs/tags/1.2" The first split gives us "['', '1.2b1,
                    # tag:', '1.2']" Which is why we do the second split below
                    # on the comma
                    for tag_string in refname.split("refs/tags/")[1:]:
                        # git tag does not allow : or " " in tag names, so we
                        # split on ", " which is the separator between elements
                        candidate = tag_string.split(", ")[0]
                        try:
                            version.StrictVersion(candidate)
                        except ValueError:
                            pass
                        else:
                            f.write(b"\n%s\n" % candidate.encode('utf8'))
                            f.write(b"%s\n\n" % (b"-" * len(candidate)))

                if msg.startswith("Merge "):
                    continue
                if msg.endswith("."):
                    msg = msg[:-1]
                msg = msg.replace('*', '\*')
                msg = msg.replace('_', '\_')
                msg = msg.replace('`', '\`')
                f.write(b"* %s\n" % msg.encode("utf8"))


class local_install_scripts(install_scripts.install_scripts):
    def run(self):
        install_scripts.install_scripts.run(self)
        header = easy_install.get_script_header(
            "", easy_install.sys_executable, False)
        self.write_script("gnocchi-api", header + SCRIPT_TMPL)


class local_develop(develop.develop):
    def install_wrapper_scripts(self, dist):
        develop.develop.install_wrapper_scripts(self, dist)
        header = easy_install.get_script_header(
            "", easy_install.sys_executable, False)
        self.write_script("gnocchi-api", header + SCRIPT_TMPL)
