#
# Copyright 2015-2017 Red Hat. All Rights Reserved.
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

import warnings

import sqlalchemy.exc

# NOTE(sileht) We create and recreate ton of sqlalchemy db
# Because we play with the declartive object list at runtime
# for resource type. In tests sqlalchemy complains we override
# the list again and again. So we filter them in tests
warnings.filterwarnings("ignore",
                        "This declarative base already contains a "
                        "class with the same class name",
                        sqlalchemy.exc.SAWarning)
