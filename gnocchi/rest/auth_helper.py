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
import werkzeug.http

from gnocchi import rest


class KeystoneAuthHelper(object):
    @staticmethod
    def get_current_user(headers):
        # FIXME(jd) should have domain but should not break existing :(
        user_id = headers.get("X-User-Id", "")
        project_id = headers.get("X-Project-Id", "")
        return user_id + ":" + project_id

    @staticmethod
    def get_auth_info(headers):
        user_id = headers.get("X-User-Id")
        project_id = headers.get("X-Project-Id")
        return {
            "user": (user_id or "") + ":" + (project_id or ""),
            "user_id": user_id,
            "project_id": project_id,
            'domain_id': headers.get("X-Domain-Id"),
            'roles': headers.get("X-Roles", "").split(","),
        }

    @staticmethod
    def get_resource_policy_filter(headers, rule, resource_type):
        try:
            # Check if the policy allows the user to list any resource
            rest.enforce(rule, {
                "resource_type": resource_type,
            })
        except webob.exc.HTTPForbidden:
            policy_filter = []
            project_id = headers.get("X-Project-Id")

            try:
                # Check if the policy allows the user to list resources linked
                # to their project
                rest.enforce(rule, {
                    "resource_type": resource_type,
                    "project_id": project_id,
                })
            except webob.exc.HTTPForbidden:
                pass
            else:
                policy_filter.append({"=": {"project_id": project_id}})

            try:
                # Check if the policy allows the user to list resources linked
                # to their created_by_project
                rest.enforce(rule, {
                    "resource_type": resource_type,
                    "created_by_project_id": project_id,
                })
            except webob.exc.HTTPForbidden:
                pass
            else:
                if project_id:
                    policy_filter.append(
                        {"like": {"creator": "%:" + project_id}})
                else:
                    policy_filter.append({"=": {"creator": None}})

            if not policy_filter:
                # We need to have at least one policy filter in place
                rest.abort(403, "Insufficient privileges")

            return {"or": policy_filter}


class NoAuthHelper(KeystoneAuthHelper):
    @staticmethod
    def get_current_user(headers):
        # FIXME(jd) Should be a single header
        user_id = headers.get("X-User-Id")
        project_id = headers.get("X-Project-Id")
        if user_id:
            if project_id:
                return user_id + ":" + project_id
            return user_id
        if project_id:
            return project_id
        rest.abort(401, "Unable to determine current user")


class BasicAuthHelper(object):
    @staticmethod
    def get_current_user(headers):
        auth = werkzeug.http.parse_authorization_header(
            headers.get("Authorization"))
        if auth is None:
            rest.abort(401)
        return auth.username

    def get_auth_info(self, headers):
        user = self.get_current_user(headers)
        roles = []
        if user == "admin":
            roles.append("admin")
        return {
            "user": user,
            "roles": roles
        }

    @staticmethod
    def get_resource_policy_filter(headers, rule, resource_type):
        return None
