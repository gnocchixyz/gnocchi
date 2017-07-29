# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat
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

import numpy

from gnocchi.rest import transformation
from gnocchi.tests import base


class TestTransformParser(base.BaseTestCase):
    def test_good(self):
        expressions = {
            "absolute": [("absolute", tuple())],
            "negative()": [("negative", tuple())],
            "negative:absolute": [("negative", tuple()),
                                  ("absolute", tuple())],
            "negative():absolute": [("negative", tuple()),
                                    ("absolute", tuple())],
            "resample(2)": [("resample", (numpy.timedelta64(2, 's'),))],
            "resample(5):absolute": [("resample",
                                      (numpy.timedelta64(5, 's'),)),
                                     ("absolute", tuple())],
        }
        for expr, expected in expressions.items():
            try:
                parsed = transformation.parse(expr)
            except transformation.TransformationParserError as e:
                self.fail("%s invalid: %s" % (expr, str(e)))
            for trans, trans_expected in zip(parsed, expected):
                self.assertEqual(trans.method, trans_expected[0])
                self.assertEqual(trans.args, trans_expected[1])

    def test_bad(self):
        expressions = [
            "::",
            "absolute(",
            "absolute(:negative)",
            "absolute:negative)",
            "foobar:",
            "absolute:",
            ":absolute",
            "absolute():negative():",
            "()",
            "(",
            "foobar",
            "foobar()",
            "resample()",
            "resample(,)",
            "resample(-2)",
            "resample(, 1.3)",
            "resample(a)",
            "resample(1.5, 1.3)",

        ]
        for expr in expressions:
            self.assertRaises(transformation.TransformationParserError,
                              transformation.parse, expr)
