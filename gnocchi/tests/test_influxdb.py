# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat, Inc.
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
import numpy
import pyparsing

from gnocchi.rest import influxdb
from gnocchi.tests import base


class TestInfluxDBLineProtocol(base.BaseTestCase):
    def test_line_protocol_parser_ok(self):
        lines = (
            ('cpu,cpu=cpu2,host=abydos usage_system=11.1,usage_idle=73.2,usage_nice=0,usage_irq=0,usage_user=15.7,usage_softirq=0,usage_steal=0,usage_guest=0,usage_guest_nice=0,usage_iowait=0 1510150170000000000',  # noqa
             ['cpu',
              {'host': 'abydos',
               'cpu': 'cpu2'},
              {'usage_guest': 0.0,
               'usage_nice': 0.0,
               'usage_steal': 0.0,
               'usage_iowait': 0.0,
               'usage_user': 15.7,
               'usage_idle': 73.2,
               'usage_softirq': 0.0,
               'usage_guest_nice': 0.0,
               'usage_irq': 0.0,
               'usage_system': 11.1},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('cpu,cpu=cpu-total,host=abydos usage_idle=79.2198049512378,usage_nice=0,usage_iowait=0,usage_steal=0,usage_guest=0,usage_guest_nice=0,usage_system=9.202300575143786,usage_irq=0,usage_softirq=0,usage_user=11.577894473618404 1510150170000000000',  # noqa
             ['cpu',
              {'cpu': 'cpu-total',
               'host': 'abydos'},
              {'usage_guest': 0.0,
               'usage_guest_nice': 0.0,
               'usage_idle': 79.2198049512378,
               'usage_iowait': 0.0,
               'usage_irq': 0.0,
               'usage_nice': 0.0,
               'usage_softirq': 0.0,
               'usage_steal': 0.0,
               'usage_system': 9.202300575143786,
               'usage_user': 11.577894473618404},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('diskio,name=disk0,host=abydos io_time=11020501i,iops_in_progress=0i,read_bytes=413847966208i,read_time=9816308i,write_time=1204193i,weighted_io_time=0i,reads=33523907i,writes=7321123i,write_bytes=141510539264i 1510150170000000000',  # noqa
             ['diskio',
              {'host': 'abydos',
               'name': 'disk0'},
              {'io_time': 11020501,
               'iops_in_progress': 0,
               'read_bytes': 413847966208,
               'read_time': 9816308,
               'reads': 33523907,
               'weighted_io_time': 0,
               'write_bytes': 141510539264,
               'write_time': 1204193,
               'writes': 7321123},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('disk,path=/,device=disk1s1,fstype=apfs,host=abydos total=250140434432i,free=28950695936i,used=216213557248i,used_percent=88.19130621205531,inodes_total=9223372036854775807i,inodes_free=9223372036850748963i,inodes_used=4026844i 1510150170000000000',  # noqa
             ['disk',
              {'device': 'disk1s1', 'fstype': 'apfs',
               'host': 'abydos', 'path': '/'},
              {'free': 28950695936,
               'inodes_free': 9223372036850748963,
               'inodes_total': 9223372036854775807,
               'inodes_used': 4026844,
               'total': 250140434432,
               'used': 216213557248,
               'used_percent': 88.19130621205531},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('mem,host=abydos free=16195584i,available_percent=24.886322021484375,used=6452215808i,cached=0i,buffered=0i,active=2122153984i,inactive=2121523200i,used_percent=75.11367797851562,total=8589934592i,available=2137718784i 1510150170000000000',  # noqa
             ['mem',
              {'host': 'abydos'},
              {'active': 2122153984,
               'available': 2137718784,
               'available_percent': 24.886322021484375,
               'buffered': 0,
               'cached': 0,
               'free': 16195584,
               'inactive': 2121523200,
               'total': 8589934592,
               'used': 6452215808,
               'used_percent': 75.11367797851562},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('disk,path=/private/var/vm,device=disk1s4,fstype=apfs,host=abydos inodes_total=9223372036854775807i,inodes_free=9223372036854775803i,inodes_used=4i,total=250140434432i,free=28950695936i,used=4296265728i,used_percent=12.922280752806417 1510150170000000000',  # noqa
             ['disk',
              {'device': 'disk1s4',
               'fstype': 'apfs',
               'host': 'abydos',
               'path': '/private/var/vm'},
              {'free': 28950695936,
               'inodes_free': 9223372036854775803,
               'inodes_total': 9223372036854775807,
               'inodes_used': 4,
               'total': 250140434432,
               'used': 4296265728,
               'used_percent': 12.922280752806417},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('swap,host=abydos used=2689073152i,free=532152320i,used_percent=83.47981770833334,total=3221225472i 1510150170000000000',  # noqa
             ['swap',
              {'host': 'abydos'},
              {'free': 532152320,
               'total': 3221225472,
               'used': 2689073152,
               'used_percent': 83.47981770833334},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('swap,host=abydos in=0i,out=0i 1510150170000000000',
             ['swap',
              {'host': 'abydos'},
              {'in': 0, 'out': 0},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('processes,host=abydos stopped=0i,running=2i,sleeping=379i,total=382i,unknown=0i,idle=0i,blocked=1i,zombies=0i 1510150170000000000',  # noqa
             ['processes',
              {'host': 'abydos'},
              {'blocked': 1,
               'idle': 0,
               'running': 2,
               'sleeping': 379,
               'stopped': 0,
               'total': 382,
               'unknown': 0,
               'zombies': 0},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('system,host=abydos load5=3.02,load15=3.31,n_users=1i,n_cpus=4i,load1=2.18 1510150170000000000',  # noqa
             ['system',
              {'host': 'abydos'},
              {'load1': 2.18,
               'load15': 3.31,
               'load5': 3.02,
               'n_cpus': 4,
               'n_users': 1},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('system,host=abydos uptime=337369i,uptime_format="3 days, 21:42" 1510150170000000000',  # noqa
             ['system',
              {'host': 'abydos'},
              {'uptime': 337369, 'uptime_format': '3 days, 21:42'},
              numpy.datetime64('2017-11-08T14:09:30.000000000')]),
            ('notag up=1 123234',
             ['notag',
              {},
              {'up': 1.0},
              numpy.datetime64('1970-01-01T00:00:00.000123234')]),
            ('notag up=3 ', ['notag', {}, {'up': 3.0}, None]),
        )
        for line, result in lines:
            parsed = list(influxdb.line_protocol.parseString(line))
            self.assertEqual(result, parsed)

    def test_line_protocol_parser_fail(self):
        lines = (
            "measurement, field=1",
            "measurement, field=1 123",
            "measurement,tag=value 123",
            "measurement,tag=value , 123",
            "measurement,tag=value 123",
            ",tag=value 123",
            "foobar,tag=value field=string 123",
        )
        for line in lines:
            self.assertRaises(pyparsing.ParseException,
                              influxdb.line_protocol.parseString,
                              line)

    def test_query_parser_ok(self):
        lines = (
            "CREATE DATABASE foobar;",
            "CREATE DATABASE foobar  ;",
            "CREATE DATABASE foobar  ;;;",
            "CrEaTe   Database foobar",
            "create Database    foobar",
        )
        for line in lines:
            parsed = list(influxdb.query_parser.parseString(line))[0]
            self.assertEqual("foobar", parsed)

    def test_query_parser_fail(self):
        lines = (
            "SELECT",
            "hey yo foobar;",
            "help database foobar;",
            "something weird",
            "create stuff foobar",
        )
        for line in lines:
            self.assertRaises(pyparsing.ParseException,
                              influxdb.query_parser.parseString,
                              line)
