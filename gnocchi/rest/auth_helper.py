# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
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
import webob

from gnocchi import rest


class KeystoneAuthHelper(object):
    @staticmethod
    def get_resource_policy_filter(rule, resource_type):
        try:
            # Check if the policy allows the user to list any resource
            rest.enforce(rule, {
                "resource_type": resource_type,
            })
        except webob.exc.HTTPForbidden:
            user, project = rest.get_user_and_project()
            policy_filter = []
            try:
                # Check if the policy allows the user to list resources linked
                # to their project
                rest.enforce(rule, {
                    "resource_type": resource_type,
                    "project_id": project,
                })
            except webob.exc.HTTPForbidden:
                pass
            else:
                policy_filter.append({"=": {"project_id": project}})
            try:
                # Check if the policy allows the user to list resources linked
                # to their created_by_project
                rest.enforce(rule, {
                    "resource_type": resource_type,
                    "created_by_project_id": project,
                })
            except webob.exc.HTTPForbidden:
                pass
            else:
                policy_filter.append({"=": {"created_by_project_id": project}})

            if not policy_filter:
                # We need to have at least one policy filter in place
                rest.abort(403, "Insufficient privileges")

            return {"or": policy_filter}


NoAuthHelper = KeystoneAuthHelper
