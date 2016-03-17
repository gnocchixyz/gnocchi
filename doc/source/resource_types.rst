================
 Resource Types
================

Gnocchi offers different resource types to manage your resources. Each resource
type has strongly typed attributes. All resource types are subtypes of the
`generic` type.

Immutable attributes are attributes that cannot be modified after the resource
has been created.


generic
=======

+------------+----------------+-----------+
| Attribute  | Type           | Immutable |
+============+================+===========+
| user_id    | UUID           | Yes       |
+------------+----------------+-----------+
| project_id | UUID           | Yes       |
+------------+----------------+-----------+
| started_at | Timestamp      | Yes       |
+------------+----------------+-----------+
| ended_at   | Timestamp      | No        |
+------------+----------------+-----------+
| type       | String         | Yes       |
+------------+----------------+-----------+
| metrics    | {String: UUID} | No        |
+------------+----------------+-----------+


ceph_account
============

No specific attributes.


identity
========

No specific attributes.


image
=====

+------------------+---------+-----------+
| Attribute        | Type    | Immutable |
+==================+=========+===========+
| name             | String  | No        |
+------------------+---------+-----------+
| container_format | String  | No        |
+------------------+---------+-----------+
| disk_format      | String  | No        |
+------------------+---------+-----------+


instance
========

+--------------+---------+-----------+
| Attribute    | Type    | Immutable |
+==============+=========+===========+
| flavor_id    | String  | No        |
+--------------+---------+-----------+
| image_ref    | String  | No        |
+--------------+---------+-----------+
| host         | String  | No        |
+--------------+---------+-----------+
| display_name | String  | No        |
+--------------+---------+-----------+
| server_group | String  | No        |
+--------------+---------+-----------+


ipmi
====

No specific attributes.


network
=======

No specific attributes.


stack
=====

No specific attributes.


swift_account
=============

No specific attributes.


volume
======

+--------------+---------+-----------+
| Attribute    | Type    | Immutable |
+==============+=========+===========+
| display_name | String  | No        |
+--------------+---------+-----------+


host
====

+--------------+---------+-----------+
| Attribute    | Type    | Immutable |
+==============+=========+===========+
| host_name    | String  | No        |
+--------------+---------+-----------+


host_disk
=========

+--------------+---------+-----------+
| Attribute    | Type    | Immutable |
+==============+=========+===========+
| host_name    | String  | No        |
+--------------+---------+-----------+
| device_name  | String  | No        |
+------------------------------------+


host_network_interface
======================

+--------------+---------+-----------+
| Attribute    | Type    | Immutable |
+==============+=========+===========+
| host_name    | String  | No        |
+--------------+---------+-----------+
| device_name  | String  | No        |
+------------------------------------+
