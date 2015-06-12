#!/usr/bin/env python
#
# Copyright (c) 2014 eNovance
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Tools to analyse the result of multiple call of duration_perf_test.py:
#
#   $ clients=10
#   $ parallel --progress -j $clients python duration_perf_test.py \
#       --result myresults/client{} ::: $(seq 0 $clients)
#   $ python duration_perf_analyse.py myresults
#    * get_measures:
#                  Time
#    count  1000.000000
#    mean      0.032090
#    std       0.028287
#    ...
#


import argparse
import os

import pandas


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('result',
                        help=('Path of the results of perf_tool.py.'),
                        default='result')

    data = {
        'get_measures': [],
        'write_measures': [],
        'write_metric': [],
    }
    args = parser.parse_args()
    for root, dirs, files in os.walk(args.result):
        for name in files:
            for method in data:
                if name.endswith('_%s.csv' % method):
                    datum = data[method]
                    filepath = os.path.join(root, name)
                    datum.append(pandas.read_csv(filepath))
                    cname = name.replace('_%s.csv' % method, '')
                    datum[-1].rename(columns={'Duration': cname}, inplace=True)

    for method in data:
        merged = pandas.DataFrame(columns=['Index', 'Duration'])
        append = pandas.DataFrame(columns=['Duration'])
        for datum in data[method]:
            datum.dropna(axis=1, inplace=True)
            datum.drop('Count', axis=1, inplace=True)
            merged = merged.merge(datum, on='Index')
            cname = datum.columns.values[1]
            datum.rename(columns={cname: 'Duration'}, inplace=True)
            append = append.append(datum.drop('Index', axis=1))
        merged.to_csv(os.path.join(args.result, '%s_merged.csv' % method),
                      index=False)
        print("* %s:" % method)
        print(append.describe())
        print("")

if __name__ == '__main__':
    main()
