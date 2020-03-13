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

import datetime
import numpy

from gnocchi.rest.aggregates.api import Grouper
from gnocchi.tests import base
from unittest import mock


RETRIEVE_RESOURCES_HISTORY = \
    "gnocchi.rest.aggregates.api.Grouper.retrieve_resources_history"
API_AGGREGATE = \
    "gnocchi.rest.aggregates.api.AggregatesController._get_measures_by_name"


class Resource(object):
    def __init__(self, id, flavor_name, host, display_name,
                 revision_start=datetime.datetime(2020, 3, 10, 10, 0, 0, 0)):
        self.id = id
        self.flavor_name = flavor_name
        self.host = host
        self.display_name = display_name
        self.revision_start = revision_start
        self.revision_end = None


class ResourceHistory(object):
    def __init__(self, resources):
        self.resources = resources
        self.history = list(resources)

    def update_resource(self, date, update, id):
        resource = list(filter(lambda r: r.id == id, self.resources))[0]
        new_resource = Resource(resource.id, resource.flavor_name,
                                resource.host, resource.display_name, date)

        for k, v in update.items():
            new_resource.__setattr__(k, v)
        self.history.append(new_resource)

    def get_history_as_dict(self):
        self.history.sort(key=lambda r: r.revision_start)
        ids = set(map(lambda h: h.id, self.history))
        for id in ids:
            resources_with_id = [h for h in self.history if h.id == id]
            for i, resource in enumerate(resources_with_id[:-1]):
                resource.revision_end = resources_with_id[i + 1].revision_start

        return list(map(lambda x: x.__dict__, self.history))


class TestScenario(object):

    @mock.patch(RETRIEVE_RESOURCES_HISTORY)
    @mock.patch(API_AGGREGATE)
    def __init__(self, mock_measure, mock_history, test_input,
                 expected_output):
        self.test_input = test_input
        self.expected_output = expected_output
        self.result = None
        self.grouper = Grouper(test_input['test_group_by'],
                               test_input['test_start'],
                               test_input['test_end'],
                               {'operations': 'operation'},
                               None, None, None, None, None,
                               None, None)
        history = self.create_test_scenario()
        mock_history.side_effect = history.get_history_as_dict
        mock_measure.side_effect = get_metric
        self.execute()

    def execute(self):
        self.result = self.grouper.get_grouped_measures()

    def validate_scenario(self):
        all_groups = list(map(lambda o: o['group'], self.expected_output))
        all_response_groups = list(map(lambda r: r['group'], self.result))
        assert all_groups == all_response_groups
        for r in self.result:
            for out in self.expected_output:
                if out['group'] == r['group']:
                    for date, val in out['measures'].items():
                        aggregated = r['measures']['measures']['aggregated']
                        assert len(aggregated) == len(out['measures'])
                        for dat, gran, value in aggregated:
                            if str(dat) == date:
                                assert val == value

    def create_test_scenario(self):
        scenario = self.test_input
        resource_history = ResourceHistory(scenario['resource'])
        for update in scenario['updates']:
            resource_history.update_resource(update['event_time'],
                                             update['event_update'],
                                             update['id'])

        return resource_history


def get_metric(*args, **kwargs):
    start = args[3]
    end = args[4]
    if not end:
        end = numpy.datetime64('2020-03-10T12:00:00Z')
    ts = (start - numpy.datetime64(
        '1970-01-01T00:00:00Z')) / numpy.timedelta64(1, 's')
    current = numpy.datetime64(
        datetime.datetime.utcfromtimestamp(ts - (ts % 3600)))
    to_return = []
    while current < end:
        to_return.append((
            current,
            numpy.timedelta64(3600000000000, 'ns'),
            100
        ))
        current += numpy.timedelta64(3600, 's')

    return {'measures': {'aggregated': to_return}}


class TestGroupMeasuresWithHistory(base.BaseTestCase):

    def test_group_measures_changing_over_two_hours_with_more_than_one_resource(self):
        test_input = {
            'test_start': datetime.datetime(2020, 3, 10, 10, 0, 0, 0),
            'test_end': datetime.datetime(2020, 3, 10, 12, 0, 0, 0),
            'test_group_by': ['flavor_name', 'id'],
            'resource': [Resource(id=1, flavor_name="1gb-mem", host="192.168.0.1",
                                  display_name="My_VM",
                                  revision_start=datetime.datetime(
                                      2020, 3, 10, 9, 0, 0, 0)),
                         Resource(id=2, flavor_name="1gb-mem", host="192.168.0.1",
                                  display_name="My_VM",
                                  revision_start=datetime.datetime(
                                      2020, 3, 10, 8, 0, 0, 0))
                         ],
            'updates': [
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 30, 0, 0),
                    'event_update': {'flavor_name': "2gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 11, 30, 0, 0),
                    'event_update': {'flavor_name': "1gb-mem"}
                },
                {
                    'id': 2,
                    'event_time': datetime.datetime(2020, 3, 10, 11, 0, 0, 0),
                    'event_update': {'flavor_name': "2gb-mem"}
                },
            ]
        }
        expected_output = [
            {
                'group': {'flavor_name': '1gb-mem', 'id': 1},
                'measures': {
                    '2020-03-10T10:00:00.000000': 50,
                    '2020-03-10T11:00:00.000000': 50
                }
            },
            {
                'group': {'flavor_name': '1gb-mem', 'id': 2},
                'measures': {
                    '2020-03-10T10:00:00.000000': 100
                }
            },
            {
                'group': {'flavor_name': '2gb-mem', 'id': 1},
                'measures': {
                    '2020-03-10T10:00:00.000000': 50,
                    '2020-03-10T11:00:00.000000': 50
                }
            },
            {
                'group': {'flavor_name': '2gb-mem', 'id': 2},
                'measures': {
                    '2020-03-10T11:00:00.000000': 100
                }
            },
        ]
        scenario = TestScenario(test_input=test_input,
                                expected_output=expected_output)
        scenario.validate_scenario()

    def test_group_measures_changing_over_two_hours(self):
        test_input = {
            'test_start': datetime.datetime(2020, 3, 10, 10, 0, 0, 0),
            'test_end': datetime.datetime(2020, 3, 10, 12, 0, 0, 0),
            'test_group_by': ['flavor_name'],
            'resource': [Resource(id=1, flavor_name="1gb-mem", host="192.168.0.1",
                                  display_name="My_VM")],
            'updates': [
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 30, 0, 0),
                    'event_update': {'flavor_name': "2gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 11, 30, 0, 0),
                    'event_update': {'flavor_name': "1gb-mem"}
                }
            ]
        }
        expected_output = [
            {
                'group': {'flavor_name': '1gb-mem'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 50,
                    '2020-03-10T11:00:00.000000': 50
                }
            },
            {
                'group': {'flavor_name': '2gb-mem'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 50,
                    '2020-03-10T11:00:00.000000': 50
                }
            }
        ]
        scenario = TestScenario(test_input=test_input,
                                expected_output=expected_output)
        scenario.validate_scenario()

    def test_group_measures_changing_flavors_in_a_hour(self):
        test_input = {
            'test_start': datetime.datetime(2020, 3, 10, 10, 0, 0, 0),
            'test_end': datetime.datetime(2020, 3, 10, 12, 0, 0, 0),
            'test_group_by': ['flavor_name'],
            'resource': [Resource(id=1, flavor_name="1gb-mem", host="192.168.0.1",
                                  display_name="My_VM")],
            'updates': [
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 30, 0, 0),
                    'event_update': {'flavor_name': "2gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 45, 0, 0),
                    'event_update': {'flavor_name': "1gb-mem"}
                }
            ]
        }
        expected_output = [
            {
                'group': {'flavor_name': '1gb-mem'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 75,
                    '2020-03-10T11:00:00.000000': 100
                }
            },
            {
                'group': {'flavor_name': '2gb-mem'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 25
                }
            }
        ]
        scenario = TestScenario(test_input=test_input,
                                expected_output=expected_output)
        scenario.validate_scenario()

    def test_group_measures_many_flavors_changes_in_a_hour(self):
        test_input = {
            'test_start': datetime.datetime(2020, 3, 10, 10, 0, 0, 0),
            'test_end': datetime.datetime(2020, 3, 10, 12, 0, 0, 0),
            'test_group_by': ['flavor_name'],
            'resource': [Resource(id=1, flavor_name="1gb-mem", host="192.168.0.1",
                                  display_name="My_VM")],
            'updates': [
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 10, 0, 0),
                    'event_update': {'flavor_name': "2gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 20, 0, 0),
                    'event_update': {'flavor_name': "3gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 30, 0, 0),
                    'event_update': {'flavor_name': "4gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 40, 0, 0),
                    'event_update': {'flavor_name': "1gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 45, 0, 0),
                    'event_update': {'flavor_name': "2gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 55, 0, 0),
                    'event_update': {'flavor_name': "3gb-mem"}
                }
            ]
        }
        expected_output = [
            {
                'group': {'flavor_name': '1gb-mem'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 24.999999999999996
                }
            },
            {
                'group': {'flavor_name': '2gb-mem'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 33.33333333333333
                }
            },
            {
                'group': {'flavor_name': '3gb-mem'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 24.999999999999996,
                    '2020-03-10T11:00:00.000000': 100,
                }
            },
            {
                'group': {'flavor_name': '4gb-mem'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 16.666666666666686
                }
            }
        ]
        scenario = TestScenario(test_input=test_input,
                                expected_output=expected_output)
        scenario.validate_scenario()

    def test_group_measures_multiple_metadata_changed(self):
        test_input = {
            'test_start': datetime.datetime(2020, 3, 10, 10, 0, 0, 0),
            'test_end': datetime.datetime(2020, 3, 10, 12, 0, 0, 0),
            'test_group_by': ['flavor_name', 'display_name'],
            'resource': [Resource(id=1, flavor_name="1gb-mem", host="192.168.0.1",
                                  display_name="My_VM")],
            'updates': [
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 30, 0, 0),
                    'event_update': {'flavor_name': "2gb-mem"}
                },
                {
                    'id': 1,
                    'event_time': datetime.datetime(2020, 3, 10, 10, 45, 0, 0),
                    'event_update': {'display_name': "Not_My_VM",
                                     'flavor_name': "2gb-mem"}
                }
            ]
        }
        expected_output = [
            {
                'group': {'flavor_name': '1gb-mem',
                          'display_name': 'My_VM'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 50
                }
            },
            {
                'group': {'flavor_name': '2gb-mem',
                          'display_name': 'My_VM'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 25
                }
            },
            {
                'group': {'flavor_name': '2gb-mem',
                          'display_name': 'Not_My_VM'},
                'measures': {
                    '2020-03-10T10:00:00.000000': 25,
                    '2020-03-10T11:00:00.000000': 100,
                }
            }
        ]
        scenario = TestScenario(test_input=test_input,
                                expected_output=expected_output)
        scenario.validate_scenario()
