#!/usr/bin/env python
# Copyright (c) 2014 eNovance
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

import setuptools
import sys

from setuptools.command import develop
from setuptools.command import easy_install
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


PY3 = sys.version_info >= (3,)


class local_install_scripts(install_scripts.install_scripts):
    def run(self):
        # NOTE(tobias-urdin): Always install_scripts so that we get
        # gnocchi-api otherwise it's left out when installing with pip.
        self.no_ep = False
        install_scripts.install_scripts.run(self)
        # NOTE(sileht): Build wheel embed custom script as data, and put sheban
        # in script of the building machine. To workaround that build_scripts
        # on bdist_whell return '#!python' and then during whl install it's
        # replaced by the correct interpreter. We do the same here.
        bs_cmd = self.get_finalized_command('build_scripts')
        executable = getattr(bs_cmd, 'executable', easy_install.sys_executable)
        try:
            script = easy_install.get_script_header(
                "", executable) + SCRIPT_TMPL
        except AttributeError:
            script = easy_install.ScriptWriter.get_header(
                "", executable) + SCRIPT_TMPL
        if PY3:
            script = script.encode('ascii')
        self.write_script("gnocchi-api", script, 'b')


class local_develop(develop.develop):
    def install_wrapper_scripts(self, dist):
        develop.develop.install_wrapper_scripts(self, dist)
        if self.exclude_scripts:
            return
        try:
            script = easy_install.get_script_header("") + SCRIPT_TMPL
        except AttributeError:
            script = easy_install.ScriptWriter.get_header("") + SCRIPT_TMPL
        if PY3:
            script = script.encode('ascii')
        self.write_script("gnocchi-api", script, 'b')


cmdclass = {
    'develop': local_develop,
    'install_scripts': local_install_scripts,
}

try:
    from sphinx import setup_command
    cmdclass['build_sphinx'] = setup_command.BuildDoc
except ImportError:
    pass


setuptools.setup(
    cmdclass=cmdclass,
    py_modules=[],
)
