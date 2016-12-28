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

import jinja2
import six
import six.moves
import webob.request
import yaml

from gnocchi.tests import test_rest


# HACK(jd) Not sure why but Sphinx setup this multiple times, so we just avoid
# doing several times the requests by using this global variable :(
_RUN = False


def _setup_test_app():
    t = test_rest.RestTest()
    t.auth = True
    t.setUpClass()
    t.setUp()
    return t.app


def _format_json(txt):
    return json.dumps(json.loads(txt),
                      sort_keys=True,
                      indent=2)


def _extract_body(req_or_resp):
    # TODO(jd) Make this a Sphinx option
    if req_or_resp.content_type == "application/json":
        body = _format_json(req_or_resp.body)
    else:
        body = req_or_resp.body
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


def setup(app):
    global _RUN
    if _RUN:
        return
    webapp = _setup_test_app()
    # TODO(jd) Do not hardcode doc/source
    with open("doc/source/rest.yaml") as f:
        scenarios = ScenarioList(yaml.load(f))
    for entry in scenarios:
        template = jinja2.Template(entry['request'])
        fake_file = six.moves.cStringIO()
        fake_file.write(template.render(scenarios=scenarios).encode('utf-8'))
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
    with open("doc/source/rest.j2", "r") as f:
        template = jinja2.Template(f.read().decode('utf-8'))
    with open("doc/source/rest.rst", "w") as f:
        f.write(template.render(scenarios=scenarios).encode('utf-8'))
    _RUN = True
