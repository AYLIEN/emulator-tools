#!/usr/bin/env python

# Copyright 2018, Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tool for connecting to Cloud Bigtable Emulator and run some basic operations.
#     http://www.apache.org/licenses/LICENSE-2.0

"""

import argparse
import datetime

from google.cloud import bigtable
from google.cloud.bigtable import column_family


def create_table(project_id, instance_id, table_id):
    ''' Create a Bigtable table

    :type project_id: str
    :param project_id: Project id of the client.

    :type instance_id: str
    :param instance_id: Instance of the client.

    :type table_id: str
    :param table_id: Table id to create table.
    '''

    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    # Check whether table exists in an instance.
    # Create table if it does not exists.
    print('Checking if table {} exists...'.format(table_id))
    if table.exists():
        print('Table {} already exists.'.format(table_id))
    else:
        print('Creating the {} table.'.format(table_id))
        table.create()
        print('Created table {}.'.format(table_id))

    return client, instance, table


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('command',
                        help='create. \
                        Operation to perform on table.')
    parser.add_argument(
        '--table',
        help='Cloud Bigtable Table name.',
        default='Hello-Bigtable')

    parser.add_argument('project_id',
                        help='Your Cloud Platform project ID.')
    parser.add_argument(
        'instance_id',
        help='ID of the Cloud Bigtable instance to connect to.')

    args = parser.parse_args()

    if args.command.lower() == 'create':
        create_table(args.project_id, args.instance_id, args.table)
    else:
        print('Command should be either create.\n Use argument -h,\
               --help to show help and exit.')
