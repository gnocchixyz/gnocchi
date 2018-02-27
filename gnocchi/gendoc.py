# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
import json
import os
import subprocess
import sys
import tempfile

import jinja2
from oslo_config import generator
import six
import six.moves
import webob.request
import yaml

from gnocchi.tests import test_rest

# HACK(jd) Not sure why but Sphinx setup this multiple times, so we just avoid
# doing several times the requests by using this global variable :(
_RUN = False


def _format_json(txt):
    return json.dumps(json.loads(txt),
                      sort_keys=True,
                      indent=2)


def _extract_body(req_or_resp):
    # TODO(jd) Make this a Sphinx option
    if not req_or_resp.text:
        return ""

    if six.PY2:
        body = req_or_resp.body
    else:
        body = req_or_resp.text
    if req_or_resp.content_type.startswith("application/json"):
        body = _format_json(body)
    return "\n      ".join(body.split("\n"))


def _format_headers(headers):
    return "\n".join(
        "      %s: %s" % (k, v)
        for k, v in six.iteritems(headers))


def _response_to_httpdomain(response):
    return """
   .. sourcecode:: http

      HTTP/1.1 %(status)s
%(headers)s

      %(body)s""" % {
        'status': response.status,
        'body': _extract_body(response),
        'headers': _format_headers(response.headers),
    }


def _request_to_httpdomain(request):
    return """
   .. sourcecode:: http

      %(method)s %(path)s %(http_version)s
%(headers)s

      %(body)s""" % {
        'body': _extract_body(request),
        'method': request.method,
        'path': request.path_qs,
        'http_version': request.http_version,
        'headers': _format_headers(request.headers),
    }


def _format_request_reply(request, response):
    return (_request_to_httpdomain(request)
            + "\n"
            + _response_to_httpdomain(response))


class ScenarioList(list):
    def __getitem__(self, key):
        for scenario in self:
            if scenario['name'] == key:
                return scenario
        return super(ScenarioList, self).__getitem__(key)


multiversion_hack = """
import shutil
import sys
import os

local_branch_path = os.getcwd()
srcdir = os.path.join("%s", "..", "..")
os.chdir(srcdir)
sys.path.insert(0, srcdir)

version = sys.argv[1]

if version not in ["<local>", "master"]:
    # NOTE(sileht): Update _static files (mainly logos)
    if not os.path.exists("doc/source/_static"):
        os.makedirs("doc/source/_static")
    for f in ("doc/source/_static/gnocchi-icon.ico",
              "doc/source/_static/gnocchi-logo.png"):
        if os.path.exists(f):
            os.remove(f)
        shutil.copy(local_branch_path + "/" + f, f)

    # NOTE(sileht): We delete releasenotes from old documentation
    # only master will have it.
    if (os.path.exists("releasenotes")
            and os.path.exists("doc/source/releasenotes/unreleased.rst")):
        shutil.rmtree("releasenotes")
        shutil.move("doc/source/releasenotes", "backup")
        os.makedirs("doc/source/releasenotes")
        with open("doc/source/releasenotes/index.rst", "w") as f:
            f.write(\"\"\"
Release Notes
=============

Releases notes can be found `here </releasenotes/index.html>`_

.. raw:: html

    <meta http-equiv="refresh" content="0; url=/releasenotes/index.html">


\"\"\")



# NOTE(sileht): entry_points have old and new location mixed,
# We create symlink to fool pkg_resource so it will find them even
# if the new location is here.
try:
    os.symlink("storage/incoming", "gnocchi/incoming")
except OSError:
    pass

class FakeApp(object):
    def info(self, *args, **kwasrgs):
        pass

import gnocchi.gendoc
gnocchi.gendoc.setup(FakeApp())
"""


def setup(app):
    global _RUN
    if _RUN:
        return

    # NOTE(sileht): On gnocchi.xyz, we build a multiversion of the docs
    # all versions are built with the master gnocchi.gendoc sphinx extension.
    # So the hack here run an other python script to generate the rest.rst
    # file of old version of the module.
    # It also drop the database before each run.
    if sys.argv[0].endswith("sphinx-versioning"):
        subprocess.check_call(["dropdb", os.environ['PGDATABASE']])
        subprocess.check_call(["createdb", os.environ['PGDATABASE']])
        from sphinxcontrib.versioning import sphinx_
        version = sphinx_.EventHandlers.CURRENT_VERSION
        with tempfile.NamedTemporaryFile() as f:
            f.write(multiversion_hack % app.confdir)
            f.flush()
            subprocess.check_call(['python', f.name, version])
        _RUN = True
        return

    # TODO(jd) Do not hardcode doc/source
    with open("doc/source/rest.yaml") as f:
        scenarios = ScenarioList(yaml.load(f))

    test = test_rest.RestTest()
    test.auth_mode = "basic"
    test.setUpClass()
    test.setUp()
    webapp = test.app

    try:
        for entry in scenarios:
            if 'filter' in entry:
                entry['filter'] = jinja2.Template(entry['filter']).render(
                    scenarios=scenarios)

            template = jinja2.Template(entry['request'])
            fake_file = six.moves.cStringIO()
            content = template.render(scenarios=scenarios)
            if six.PY2:
                content = content.encode('utf-8')
            fake_file.write(content)
            fake_file.seek(0)
            request = webapp.RequestClass.from_file(fake_file)

            # TODO(jd) Fix this lame bug in webob < 1.7
            if (hasattr(webob.request, "http_method_probably_has_body")
               and request.method == "DELETE"):
                # Webob has a bug it does not read the body for DELETE, l4m3r
                clen = request.content_length
                if clen is None:
                    request.body = fake_file.read()
                else:
                    request.body = fake_file.read(clen)

            app.info("Doing request %s: %s" % (entry['name'],
                                               six.text_type(request)))
            with webapp.use_admin_user():
                response = webapp.request(request)
            entry['response'] = response
            entry['doc'] = _format_request_reply(request, response)
    finally:
        test.tearDown()
        test.tearDownClass()
    with open("doc/source/rest.j2", "r") as f:
        content = f.read()
        if six.PY2:
            content = content.decode("utf-8")
        template = jinja2.Template(content)
    with open("doc/source/rest.rst", "w") as f:
        content = template.render(scenarios=scenarios)
        if six.PY2:
            content = content.encode("utf-8")
        f.write(content)

    config_output_file = 'doc/source/gnocchi.conf.sample'
    app.info("Generating %s" % config_output_file)
    generator.main([
        '--config-file',
        '%s/gnocchi-config-generator.conf' % os.path.dirname(__file__),
        '--output-file', config_output_file,
    ])

    _RUN = True
