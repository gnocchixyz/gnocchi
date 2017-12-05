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

from gnocchi.rest import api


class KeystoneAuthHelper(object):
    @staticmethod
    def get_current_user(request):
        # FIXME(jd) should have domain but should not break existing :(
        user_id = request.headers.get("X-User-Id", "")
        project_id = request.headers.get("X-Project-Id", "")
        return user_id + ":" + project_id

    @staticmethod
    def get_auth_info(request):
        user_id = request.headers.get("X-User-Id")
        project_id = request.headers.get("X-Project-Id")
        return {
            "user": (user_id or "") + ":" + (project_id or ""),
            "user_id": user_id,
            "project_id": project_id,
            'domain_id': request.headers.get("X-Domain-Id"),
            'roles': request.headers.get("X-Roles", "").split(","),
        }

    @staticmethod
    def get_resource_policy_filter(request, rule, resource_type, prefix=None):
        try:
            # Check if the policy allows the user to list any resource
            api.enforce(rule, {
                "resource_type": resource_type,
            })
        except webob.exc.HTTPForbidden:
            policy_filter = []
            project_id = request.headers.get("X-Project-Id")
            target = {}
            if prefix:
                resource = target[prefix] = {}
            else:
                resource = target

            resource["resource_type"] = resource_type
            resource["project_id"] = project_id
            try:
                # Check if the policy allows the user to list resources linked
                # to their project
                api.enforce(rule, target)
            except webob.exc.HTTPForbidden:
                pass
            else:
                policy_filter.append({"=": {"project_id": project_id}})

            del resource["project_id"]
            resource["created_by_project_id"] = project_id
            try:
                # Check if the policy allows the user to list resources linked
                # to their created_by_project
                api.enforce(rule, target)
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
                api.abort(403, "Insufficient privileges")

            return {"or": policy_filter}

    @staticmethod
    def get_metric_policy_filter(request, rule):
        try:
            # Check if the policy allows the user to list any metric
            api.enforce(rule, {})
        except webob.exc.HTTPForbidden:
            policy_filter = []
            project_id = request.headers.get("X-Project-Id")
            try:
                # Check if the policy allows the user to list metrics linked
                # to their created_by_project
                api.enforce(rule, {
                    "created_by_project_id": project_id,
                })
            except webob.exc.HTTPForbidden:
                pass
            else:
                policy_filter.append(
                    {"like": {"creator": "%:" + project_id}})

            if not policy_filter:
                # We need to have at least one policy filter in place
                api.abort(403, "Insufficient privileges")

            return {"or": policy_filter}


class BasicAuthHelper(object):
    @staticmethod
    def get_current_user(request):
        auth = werkzeug.http.parse_authorization_header(
            request.headers.get("Authorization"))
        if auth is None:
            api.abort(401)
        return auth.username

    def get_auth_info(self, request):
        user = self.get_current_user(request)
        roles = []
        if user == "admin":
            roles.append("admin")
        return {
            "user": user,
            "roles": roles
        }

    @staticmethod
    def get_resource_policy_filter(request, rule, resource_type, prefix=None):
        return None

    @staticmethod
    def get_metric_policy_filter(request, rule):
        return None


class RemoteUserAuthHelper(object):
    @staticmethod
    def get_current_user(request):
        user = request.remote_user
        if user is None:
            api.abort(401)
        return user.decode('iso-8859-1')

    def get_auth_info(self, request):
        user = self.get_current_user(request)
        roles = []
        if user == "admin":
            roles.append("admin")
        return {
            "user": user,
            "roles": roles
        }

    @staticmethod
    def get_resource_policy_filter(request, rule, resource_type, prefix=None):
        return None

    @staticmethod
    def get_metric_policy_filter(request, rule):
        return None
