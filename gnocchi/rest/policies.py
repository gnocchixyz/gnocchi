# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


from oslo_config import cfg
from oslo_policy import policy

ADMIN = "role:admin"
ADMIN_OR_SERVICE = "role:admin or role:service"
CREATOR = 'user:%(creator)s or project_id:%(created_by_project_id)s'
ADMIN_OR_CREATOR = 'role:admin or %s' % CREATOR
RESOURCE_OWNER = "project_id:%(project_id)s"
METRIC_OWNER = "project_id:%(resource.project_id)s"
UNPROTECTED = ""

RULE_ADMIN_OR_SERVICE = "rule:admin_or_service"
RULE_ADMIN_OR_CREATOR = "rule:admin_or_creator"
RULE_ADMIN_OR_SERVICE_OR_CREATOR = "rule:admin_or_service or rule:creator"
RULE_ADMIN_OR_CREATOR_OR_RESOURCE_OWNER = \
    "rule:admin_or_creator or rule:resource_owner"
RULE_ADMIN_OR_SERVICE_OR_CREATOR_OR_RESOURCE_OWNER = \
    "rule:admin_or_service or rule:creator or rule:resource_owner"
RULE_ADMIN_OR_CREATOR_OR_METRIC_OWNER = \
    "rule:admin_or_creator or rule:metric_owner"
RULE_ADMIN_OR_SERVICE_OR_CREATOR_OR_METRIC_OWNER = \
    "rule:admin_or_service or rule:creator or rule:metric_owner"

rules = [
    policy.RuleDefault(
        name="context_is_admin",
        check_str=ADMIN
    ),
    policy.RuleDefault(
        name="admin_or_creator",
        check_str=ADMIN_OR_CREATOR,
    ),
    policy.RuleDefault(
        name="admin_or_service",
        check_str=ADMIN_OR_SERVICE,
    ),
    policy.RuleDefault(
        name="creator",
        check_str=CREATOR,
    ),
    policy.RuleDefault(
        name="resource_owner",
        check_str=RESOURCE_OWNER
    ),
    policy.RuleDefault(
        name="metric_owner",
        check_str=METRIC_OWNER
    )
]

status_rules = [
    policy.DocumentedRuleDefault(
        name="get status",
        check_str=ADMIN,
        description='Get status of Gnocchi service.',
        operations=[
            {
                'path': '/v1/status',
                'method': 'GET'
            }
        ]
    )
]

resource_rules = [
    policy.DocumentedRuleDefault(
        name="create resource",
        check_str=UNPROTECTED,
        description='Create a new resource.',
        operations=[
            {
                'path': '/v1/resource/{resource_type}',
                'method': 'POST'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="get resource",
        check_str=RULE_ADMIN_OR_CREATOR_OR_RESOURCE_OWNER,
        description='Get a resource.',
        operations=[
            {
                'path': '/v1/resource/{resource_type}/{resource_id}',
                'method': 'GET'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="update resource",
        check_str=RULE_ADMIN_OR_SERVICE_OR_CREATOR,
        scope_types=['system', 'domain', 'project'],
        description='Update a resource.',
        operations=[
            {
                'path': '/v1/resource/{resource_type}/{resource_id}',
                'method': 'PATCH'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="delete resource",
        check_str=RULE_ADMIN_OR_CREATOR,
        description='Delete a resource.',
        operations=[
            {
                'path': '/v1/resource/{resource_type}/{resource_id}',
                'method': 'DELETE'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="delete resources",
        check_str=RULE_ADMIN_OR_CREATOR,
        description='Delete multiple resources.',
        operations=[
            {
                'path': '/v1/resource/{resource_type}',
                'method': 'DELETE'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="list resource",
        check_str=RULE_ADMIN_OR_CREATOR_OR_RESOURCE_OWNER,
        description='List all resources.',
        operations=[
            {
                'path': '/v1/resource/{resource_type}',
                'method': 'GET'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="search resource",
        check_str=RULE_ADMIN_OR_SERVICE_OR_CREATOR_OR_RESOURCE_OWNER,
        scope_types=['system', 'domain', 'project'],
        description='Search resources.',
        operations=[
            {
                'path': '/v1/search/resources/{resource_type}',
                'method': 'POST'
            }
        ]
    )
]

resource_type_rules = [
    policy.DocumentedRuleDefault(
        name="create resource type",
        check_str=RULE_ADMIN_OR_SERVICE,
        scope_types=['system', 'domain', 'project'],
        description='Create a new resource type.',
        operations=[
            {
                'path': '/v1/resource_type',
                'method': 'POST'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="get resource type",
        check_str=UNPROTECTED,
        description='Get a resource type.',
        operations=[
            {
                'path': '/v1/resource_type/{resource_type}',
                'method': 'GET'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="update resource type",
        check_str=RULE_ADMIN_OR_SERVICE,
        scope_types=['system', 'domain', 'project'],
        description='Update a resource type.',
        operations=[
            {
                'path': '/v1/resource_type/{resource_type}',
                'method': 'PATCH'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="delete resource type",
        check_str=ADMIN,
        description='Delete a resource type.',
        operations=[
            {
                'path': '/v1/resource_type/{resource_type}',
                'method': 'DELETE'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="list resource type",
        check_str=UNPROTECTED,
        description='List all resource types.',
        operations=[
            {
                'path': '/v1/resource_type',
                'method': 'GET'
            }
        ]
    )
]

archive_policy_rules = [
    policy.DocumentedRuleDefault(
        name="create archive policy",
        check_str=RULE_ADMIN_OR_SERVICE,
        scope_types=['system', 'domain', 'project'],
        description='Create a new archive policy',
        operations=[
            {
                'path': '/v1/archive_policy',
                'method': 'POST'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="get archive policy",
        check_str=UNPROTECTED,
        description='Get an archive policy',
        operations=[
            {
                'path': '/v1/archive_policy/{archive_policy}',
                'method': 'GET'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="update archive policy",
        check_str=ADMIN,
        description='Update an archive policy',
        operations=[
            {
                'path': '/v1/archive_policy/{archive_policy}',
                'method': 'PATCH'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="delete archive policy",
        check_str=ADMIN,
        description='Delete an archive policy',
        operations=[
            {
                'path': '/v1/archive_policy/{archive_policy}',
                'method': 'DELETE'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="list archive policy",
        check_str=UNPROTECTED,
        description='List all archive policies',
        operations=[
            {
                'path': '/v1/archive_policy',
                'method': 'GET'
            }
        ]
    )
]

archive_policy_rule_rules = [
    policy.DocumentedRuleDefault(
        name="create archive policy rule",
        check_str=ADMIN,
        description='Create a new archive policy rule',
        operations=[
            {
                'path': '/v1/archive_policy_rule',
                'method': 'POST'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="get archive policy rule",
        check_str=UNPROTECTED,
        description='Get an archive policy rule',
        operations=[
            {
                'path': '/v1/archive_policy_rule/{archive_policy_rule}',
                'method': 'GET'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="update archive policy rule",
        check_str=ADMIN,
        description='Update an archive policy rule',
        operations=[
            {
                'path': '/v1/archive_policy_rule/{archive_policy_rule}',
                'method': 'PATCH'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="delete archive policy rule",
        check_str=ADMIN,
        description='Delete an archive policy rule',
        operations=[
            {
                'path': '/v1/archive_policy_rule/{archive_policy_rule}',
                'method': 'DELETE'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="list archive policy rule",
        check_str=UNPROTECTED,
        description='List all archive policy rules',
        operations=[
            {
                'path': '/v1/archive_policy_rule',
                'method': 'GET'
            }
        ]
    )
]

metric_rules = [
    policy.DocumentedRuleDefault(
        name="create metric",
        check_str=UNPROTECTED,
        description='Create a new metric',
        operations=[
            {
                'path': '/v1/metric',
                'method': 'POST'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="get metric",
        check_str=RULE_ADMIN_OR_SERVICE_OR_CREATOR_OR_METRIC_OWNER,
        scope_types=['system', 'domain', 'project'],
        description='Get a metric',
        operations=[
            {
                'path': '/v1/metric/{metric}',
                'method': 'GET'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="delete metric",
        check_str=RULE_ADMIN_OR_CREATOR,
        description='Delete a metric',
        operations=[
            {
                'path': '/v1/metric/{metric}',
                'method': 'DELETE'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="list metric",
        check_str=RULE_ADMIN_OR_CREATOR_OR_METRIC_OWNER,
        description='List all metrics',
        operations=[
            {
                'path': '/v1/metric',
                'method': 'GET'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="search metric",
        check_str=RULE_ADMIN_OR_CREATOR_OR_METRIC_OWNER,
        description='Search metrics',
        operations=[
            {
                'path': '/v1/search/metric',
                'method': 'POST'
            }
        ]
    )
]

measure_rules = [
    policy.DocumentedRuleDefault(
        name="post measures",
        check_str=RULE_ADMIN_OR_SERVICE_OR_CREATOR,
        scope_types=['system', 'domain', 'project'],
        description='Post measures',
        operations=[
            {
                'path': '/v1/metric/{metric}/measures',
                'method': 'POST'
            },
            {
                'path': '/v1/batch/metrics/measures',
                'method': 'POST'
            },
            {
                'path': '/v1/batch/resources/metrics/measures',
                'method': 'POST'
            }
        ]
    ),
    policy.DocumentedRuleDefault(
        name="get measures",
        check_str=RULE_ADMIN_OR_SERVICE_OR_CREATOR_OR_METRIC_OWNER,
        scope_types=['system', 'domain', 'project'],
        description='Get measures',
        operations=[
            {
                'path': '/v1/metric/{metric}/measures',
                'method': 'GET'
            }
        ]
    )
]


def list_rules():
    return rules + status_rules \
        + resource_rules + resource_type_rules \
        + archive_policy_rules + archive_policy_rule_rules \
        + metric_rules + measure_rules


def init(conf):
    policy_enforcer = policy.Enforcer(conf)
    policy_enforcer.register_defaults(list_rules())
    return policy_enforcer


def get_enforcer():
    # This method is used by oslopolicy CLI scripts in order to generate policy
    # files from overrides on disk and defaults in code.
    cfg.CONF([], project='gnocchi')
    return init(cfg.CONF)
