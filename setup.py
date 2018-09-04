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

import gnocchi.setuptools

cmdclass = {
    'egg_info': gnocchi.setuptools.local_egg_info,
    'develop': gnocchi.setuptools.local_develop,
    'install_scripts': gnocchi.setuptools.local_install_scripts,
}

try:
    from sphinx import setup_command
    cmdclass['build_sphinx'] = setup_command.BuildDoc
except ImportError:
    pass


setuptools.setup(
    setup_requires=['setuptools>=30.3.0',
                    'setuptools_scm!=1.16.0,!=1.16.1,!=1.16.2'],
    # Remove any local stuff to mimic pbr
    use_scm_version={'local_scheme': lambda v: ""},
    cmdclass=cmdclass,
)
