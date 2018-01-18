# -*- encoding: utf-8 -*-
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
from oslotest import base

from gnocchi import archive_policy
from gnocchi import service


class TestArchivePolicy(base.BaseTestCase):

    def test_several_equal_granularities(self):
        self.assertRaises(ValueError,
                          archive_policy.ArchivePolicy,
                          "foobar",
                          0,
                          [(10, 12), (20, 30), (20, 30)],
                          ["*"])

    def test_aggregation_methods(self):
        conf = service.prepare_service([],
                                       default_config_files=[])

        ap = archive_policy.ArchivePolicy("foobar",
                                          0,
                                          [],
                                          ["*"])
        self.assertEqual(
            archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS,
            ap.aggregation_methods)

        ap = archive_policy.ArchivePolicy("foobar",
                                          0,
                                          [],
                                          ["last"])
        self.assertEqual(
            set(["last"]),
            ap.aggregation_methods)

        ap = archive_policy.ArchivePolicy("foobar",
                                          0,
                                          [],
                                          ["*", "-mean"])
        self.assertEqual(
            (archive_policy.ArchivePolicy.VALID_AGGREGATION_METHODS
             - set(["mean"])),
            ap.aggregation_methods)

        ap = archive_policy.ArchivePolicy("foobar",
                                          0,
                                          [],
                                          ["-mean", "-last"])
        self.assertEqual(
            (set(conf.archive_policy.default_aggregation_methods)
             - set(["mean", "last"])),
            ap.aggregation_methods)

        ap = archive_policy.ArchivePolicy("foobar",
                                          0,
                                          [],
                                          ["+12pct"])
        self.assertEqual(
            (set(conf.archive_policy.default_aggregation_methods)
             .union(set(["12pct"]))),
            ap.aggregation_methods)

    def test_max_block_size(self):
        ap = archive_policy.ArchivePolicy("foobar",
                                          0,
                                          [(20, 60), (10, 300), (10, 5)],
                                          ["-mean", "-last"])
        self.assertEqual(ap.max_block_size, 300)


class TestArchivePolicyItem(base.BaseTestCase):
    def test_zero_size(self):
        self.assertRaises(ValueError,
                          archive_policy.ArchivePolicyItem,
                          0, 1)
        self.assertRaises(ValueError,
                          archive_policy.ArchivePolicyItem,
                          1, 0)
        self.assertRaises(ValueError,
                          archive_policy.ArchivePolicyItem,
                          -1, 1)
        self.assertRaises(ValueError,
                          archive_policy.ArchivePolicyItem,
                          1, -1)
        self.assertRaises(ValueError,
                          archive_policy.ArchivePolicyItem,
                          2, None, 1)
