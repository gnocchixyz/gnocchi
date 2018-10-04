# -*- coding:Utf-8 -*-
from __future__ import unicode_literals

from collections import OrderedDict
import six


def get_ceph_health_status(driver):
    """
    Return ceph status.
    Include ceph stats.
    """
    response = OrderedDict([
        ('name', driver.__class__.__name__)
    ])
    try:
        stats = driver.rados.get_cluster_stats()
    except Exception as e:
        response['is_available'] = False
        response['error'] = six.text_type(e)
    else:
        response['is_available'] = True
        response['stats'] = stats
    return response


def get_file_health_status(driver):
    """
    Return file status.
    """
    return OrderedDict([
        ('name', driver.__class__.__name__),
        ('is_available', True)
    ])


def get_redis_health_status(driver):
    """
    Return redis status.
    Include redis info.
    """
    response = OrderedDict([
        ('name', driver.__class__.__name__)
    ])
    try:
        info = driver._client.info()
    except Exception as e:
        response['is_available'] = False
        response['error'] = six.text_type(e)
    else:
        response['is_available'] = True
        response['info'] = info
    return response


def get_s3_health_status(driver):
    """
    Return s3 status.
    """
    response = OrderedDict([
        ('name', driver.__class__.__name__)
    ])
    try:
        driver.s3.list_objects_v2(
            Bucket=driver._bucket_name_measures, Prefix='/')
    except Exception as e:
        response['is_available'] = False
        response['error'] = six.text_type(e)
    else:
        response['is_available'] = True
    return response


def get_sqlalchemy_health_status(driver):
    """
    Return sqlalchemy status.
    """
    response = OrderedDict([
        ('name', driver.__class__.__name__)
    ])
    try:
        with driver.facade.independent_reader() as session:
            session.execute('SELECT 1')
    except Exception as e:
        response['is_available'] = False
        response['error'] = six.text_type(e)
    else:
        response['is_available'] = True
    return response


def get_swift_health_status(driver):
    """
    Return swift status.
    Include swift account info.
    """
    response = OrderedDict([
        ('name', driver.__class__.__name__)
    ])
    try:
        info = driver.swift.head_account()
    except Exception as e:
        response['is_available'] = False
        response['error'] = six.text_type(e)
    else:
        response['is_available'] = True
        response['info'] = info
    return response
