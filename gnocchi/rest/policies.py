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


from oslo_policy import policy

RULE_ADMIN = "role:admin"
RULE_ADMIN_OR_CREATOR = \
    'role:admin or user:%(creator)s or project_id:%(created_by_project_id)s'
RULE_RESOURCE_OWNER = "project_id:%(project_id)s"
RULE_METRIC_OWNER = "project_id:%(resource.project_id)s"
RULE_UNPROTECTED = ""

ADMIN_OR_CREATOR = "rule:admin_or_creator"
ADMIN_OR_CREATOR_OR_RESOURCE_OWNER = \
    "rule:admin_or_creator or rule:resource_owner"
ADMIN_OR_CREATOR_OR_METRIC_OWNER = "rule:admin_or_creator or rule:metric_owner"

rules = [
    policy.RuleDefault(
        name="context_is_admin",
        check_str=RULE_ADMIN
    ),
    policy.RuleDefault(
        name="admin_or_creator",
        check_str=RULE_ADMIN_OR_CREATOR
    ),
    policy.RuleDefault(
        name="resource_owner",
        check_str=RULE_RESOURCE_OWNER
    ),
    policy.RuleDefault(
        name="metric_owner",
        check_str=RULE_METRIC_OWNER
    )
]

resource_rules = [
    policy.DocumentedRuleDefault(
        name="create resource",
        check_str=RULE_UNPROTECTED,
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
        check_str=ADMIN_OR_CREATOR_OR_RESOURCE_OWNER,
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
        check_str=ADMIN_OR_CREATOR,
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
        check_str=ADMIN_OR_CREATOR,
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
        check_str=ADMIN_OR_CREATOR,
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
        check_str=ADMIN_OR_CREATOR_OR_RESOURCE_OWNER,
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
        check_str=ADMIN_OR_CREATOR_OR_RESOURCE_OWNER,
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
        check_str=RULE_ADMIN,
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
        check_str=RULE_UNPROTECTED,
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
        check_str=RULE_ADMIN,
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
        check_str=RULE_ADMIN,
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
        check_str=RULE_UNPROTECTED,
        description='List all resource types.',
        operations=[
            {
                'path': '/v1/resource_type',
                'method': 'GET'
            }
        ]
    )
]


def list_rules():
    return rules + resource_rules + resource_type_rules
