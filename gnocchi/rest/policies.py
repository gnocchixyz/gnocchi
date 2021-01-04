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


def list_rules():
    return rules
