# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2017 Red Hat, Inc.
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


class UnAggregableTimeseries(Exception):
    """Error raised when timeseries cannot be aggregated."""
    def __init__(self, references, reason):
        self.references = references
        self.reason = reason
        super(UnAggregableTimeseries, self).__init__(reason)

    def jsonify(self):
        return {
            "cause": "Metrics can't being aggregated",
            "reason": self.reason,
            "detail": self.references
        }
