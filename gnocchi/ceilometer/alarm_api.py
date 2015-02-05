#
# Copyright 2015 eNovance
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

import cachetools
from ceilometer.api.controllers.v2 import base
from oslo_config import cfg
from oslo_serialization import jsonutils
import requests
import wsme
from wsme import types as wtypes

from gnocchi.ceilometer import utils


cfg.CONF.import_opt('url', 'gnocchi.ceilometer.alarm_evaluator',
                    group='alarm_gnocchi')


class GnocchiUnavailable(Exception):
    code = 503


class AlarmGnocchiThresholdRule(base.AlarmRule):
    comparison_operator = base.AdvEnum('comparison_operator', str,
                                       'lt', 'le', 'eq', 'ne', 'ge', 'gt',
                                       default='eq')
    "The comparison against the alarm threshold"

    threshold = wsme.wsattr(float, mandatory=True)
    "The threshold of the alarm"

    aggregation_method = wsme.wsattr(wtypes.text, mandatory=True)
    "The aggregation_method to compare to the threshold"

    evaluation_periods = wsme.wsattr(wtypes.IntegerType(minimum=1), default=1)
    "The number of historical periods to evaluate the threshold"

    granularity = wsme.wsattr(wtypes.IntegerType(minimum=1), default=60)
    "The time range in seconds over which query"

    @classmethod
    def validate_alarm(cls, alarm):
        alarm_rule = getattr(alarm, "%s_rule" % alarm.type)
        aggregation_method = alarm_rule['aggregation_method']
        if aggregation_method not in cls._get_aggregation_methods():
            raise wsme.exc.InvalidInput('aggregation_method',
                                        aggregation_method)

    @staticmethod
    @cachetools.ttl_cache(maxsize=1, ttl=600)
    def _get_aggregation_methods():
        ks_client = utils.get_keystone_client()
        gnocchi_url = cfg.CONF.alarm_gnocchi.url
        headers = {'Content-Type': "application/json",
                   'X-Auth-Token': ks_client.auth_token}
        try:
            r = requests.get("%s/v1/capabilities" % gnocchi_url,
                             headers=headers)
        except requests.ConnectionError as e:
            raise GnocchiUnavailable(e)
        if r.status_code / 200 != 1:
            raise GnocchiUnavailable(r.text)

        return jsonutils.loads(r.text).get('aggregation_method', [])


class AlarmGnocchiMetricOfResourcesThresholdRule(AlarmGnocchiThresholdRule):
    metric = wsme.wsattr(wtypes.text, mandatory=True)
    "The name of the metric"

    resource_constraint = wsme.wsattr(wtypes.text, mandatory=True)
    "The id of a resource or a expression to select multiple resources"

    resource_type = wsme.wsattr(wtypes.text, mandatory=True)
    "The resource type"

    def as_dict(self):
        rule = self.as_dict_from_keys(['granularity', 'comparison_operator',
                                       'threshold', 'aggregation',
                                       'evaluation_periods',
                                       'metric',
                                       'resource',
                                       'resource_type'])
        return rule


class AlarmGnocchiMetricsThresholdRule(AlarmGnocchiThresholdRule):
    metrics = wsme.wsattr([wtypes.text], mandatory=True)
    "A list of metric Ids"

    def as_dict(self):
        rule = self.as_dict_from_keys(['granularity', 'comparison_operator',
                                       'threshold', 'aggregation',
                                       'evaluation_periods',
                                       'metrics'])
        return rule
