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
import daiquiri

import webob
import werkzeug.http

from gnocchi import indexer
from gnocchi.rest import api


LOG = daiquiri.getLogger(__name__)


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
    def enforce_resource_policy(request,
                                rule,
                                resource_id,
                                resource,
                                prefix=None):
        auth_info = KeystoneAuthHelper.get_auth_info(request)
        project_id = auth_info["project_id"]
        user_id = auth_info["user_id"]

        try:
            LOG.debug(("Checking if user %s:%s is allowed to access any "
                       "resource in any project under policy rule [%s]."),
                      user_id,
                      project_id,
                      rule)
            api.enforce(rule, {})

        except webob.exc.HTTPForbidden:
            LOG.debug(("User %s:%s is NOT allowed to access any resource "
                       "in any project under policy rule [%s]."),
                      user_id,
                      project_id,
                      rule)

            policy_matched = False

            target = {}
            if prefix:
                target_resource = target[prefix] = {}
            else:
                target_resource = target

            target_resource["project_id"] = project_id
            try:
                LOG.debug(("Checking if user %s:%s is allowed to access "
                           "resources within their project under policy rule "
                           "[%s]."),
                          user_id,
                          project_id,
                          rule)
                api.enforce(rule, target)
            except webob.exc.HTTPForbidden:
                LOG.debug(("User %s:%s is NOT allowed to access resources "
                           "within their project under policy rule [%s]."),
                          user_id,
                          project_id,
                          rule)
            else:
                LOG.debug(("User %s:%s is allowed to access resources within "
                           "their project under policy rule [%s]."),
                          user_id,
                          project_id,
                          rule)
                policy_matched = True
                if resource.project_id == project_id:
                    LOG.debug(("User %s:%s is authenticated under the "
                               "project the resource belongs to. Allowing "
                               "access to resource %s."),
                              user_id,
                              project_id,
                              resource_id)
                    return
                else:
                    LOG.debug(("User %s:%s is NOT authenticated under the "
                               "project the resource belongs to."),
                              user_id,
                              project_id)

            resource_creator_user_id, _, resource_creator_project_id = (
                resource.creator.partition(":"))

            del target_resource["project_id"]
            target_resource["created_by_project_id"] = project_id
            try:
                LOG.debug(("Checking if user %s:%s is allowed to access "
                           "resources if they are part of the project that "
                           "created the resource under policy rule [%s]."),
                          user_id,
                          project_id,
                          rule)
                api.enforce(rule, target)
            except webob.exc.HTTPForbidden:
                LOG.debug(("User %s:%s is NOT allowed to access resources "
                           "if they are part of the project that created "
                           "the resource under policy rule [%s]."),
                          user_id,
                          project_id,
                          rule)
            else:
                LOG.debug(("User %s:%s is allowed to access resources if "
                           "they are part of the project that created the "
                           "resource under policy rule [%s]."),
                          user_id,
                          project_id,
                          rule)
                policy_matched = True
                if resource_creator_project_id == project_id:
                    LOG.debug(("User %s:%s is authenticated under the "
                               "project that created the resource. Allowing "
                               "access to resource %s."),
                              user_id,
                              project_id,
                              resource_id)
                    return
                else:
                    LOG.debug(("User %s:%s is NOT authenticated under the "
                               "project that created the resource."),
                              user_id,
                              project_id)

            del target_resource["created_by_project_id"]
            target_resource["creator"] = user_id
            try:
                LOG.debug(("Checking if user %s:%s is allowed to access "
                           "resources they created under policy rule [%s]."),
                          user_id,
                          project_id,
                          rule)
                api.enforce(rule, target)
            except webob.exc.HTTPForbidden:
                LOG.debug(("User %s:%s is NOT allowed to access resources "
                           "they created under policy rule [%s]."),
                          user_id,
                          project_id,
                          rule)
            else:
                LOG.debug(("User %s:%s is allowed to access resources they "
                           "created under policy rule [%s]."),
                          user_id,
                          project_id,
                          rule)
                policy_matched = True
                if resource_creator_user_id == user_id:
                    LOG.debug(("User %s:%s created the resource. Allowing "
                               "access to resource %s."),
                              user_id,
                              project_id,
                              resource_id)
                    return
                else:
                    LOG.debug("User %s:%s did NOT create the resource.",
                              user_id,
                              project_id)

            # If at least one of the policies matched but the user should not
            # be allowed access to the resource, return 404 Not Found to
            # prevent the user from enumerating the existence of the resource.
            if policy_matched:
                LOG.debug(("User %s:%s allowed access to the endpoint, but "
                           "denied access to the resource under policy rule "
                           "[%s]. Forbidding access to resource %s."),
                          user_id,
                          project_id,
                          rule,
                          resource_id)
                api.abort(404, str(indexer.NoSuchResource(resource_id)))

            # None of the above policies matched, return 403 Forbidden.
            LOG.debug(("No policy matches for user %s:%s under policy rule "
                       "[%s]. Forbidding access to the endpoint."),
                      user_id,
                      project_id,
                      rule)
            api.abort(403)

        else:
            LOG.debug(("User %s:%s is allowed to access any resource in any "
                       "project under policy rule [%s]. Allowing access "
                       "to resource %s."),
                      user_id,
                      project_id,
                      rule,
                      resource_id)

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
    def get_metric_policy_filter(request,
                                 rule,
                                 allow_resource_project_id=False):
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
                LOG.debug(("Policy rule for [%s] does not allow "
                           "users to access metrics they created."),
                          rule)
            else:
                policy_filter.append(
                    {"like": {"creator": "%:" + project_id}})

            resource_project_id_allowed = False
            try:
                # Check if the policy allows the user to list metrics linked
                # to their project via a resource
                api.enforce(rule, {"resource": {"project_id": project_id}})
            except webob.exc.HTTPForbidden:
                LOG.debug(("Policy rule for [%s] does not allow "
                           "users to access resource metrics "
                           "linked to their project."),
                          rule)
            else:
                if allow_resource_project_id:
                    resource_project_id_allowed = True

            # NOTE(callumdickinson): If allow_resource_project_id is enabled
            # and the policy filter is empty, allow an empty policy filter
            # to be returned for this case ONLY.
            # The caller is expected to use get_resource_policy_filter
            # to perform filtering by resource to ensure the client
            # only gets metrics for resources they are allowed to access.
            if resource_project_id_allowed and not policy_filter:
                return {}

            if not policy_filter:
                # We need to have at least one policy filter in place
                api.abort(403, "Insufficient privileges")

            return {"or": policy_filter}


class BasicAuthHelper(object):
    @staticmethod
    def get_current_user(request):
        hdr = request.headers.get("Authorization")
        LOG.debug("Processing basic auth request [%s]. Found "
                  "Authorization header [%s].", request, hdr)
        auth_hdr = (hdr.decode('utf-8') if isinstance(hdr, bytes)
                    else hdr)

        try:
            auth = werkzeug.http.parse_authorization_header(auth_hdr)
        except AttributeError:
            auth = werkzeug.datastructures.Authorization.from_header(auth_hdr)

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
            "roles": roles,
            "system": 'all',
        }

    @staticmethod
    def enforce_resource_policy(request,
                                rule,
                                resource_id,
                                resource,
                                prefix=None):
        pass

    @staticmethod
    def get_resource_policy_filter(request, rule, resource_type, prefix=None):
        return None

    @staticmethod
    def get_metric_policy_filter(request,
                                 rule,
                                 allow_resource_project_id=False):
        return None


class RemoteUserAuthHelper(object):
    @staticmethod
    def get_current_user(request):
        user = request.remote_user
        LOG.debug("Processing remote user authentication for request [%s]. "
                  "The remote user found is [%s].", request, user)
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
            "roles": roles,
            "system": 'all',
        }

    @staticmethod
    def enforce_resource_policy(request,
                                rule,
                                resource_id,
                                resource,
                                prefix=None):
        pass

    @staticmethod
    def get_resource_policy_filter(request, rule, resource_type, prefix=None):
        return None

    @staticmethod
    def get_metric_policy_filter(request,
                                 rule,
                                 allow_resource_project_id=False):
        return None
