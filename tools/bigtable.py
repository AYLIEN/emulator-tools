#!/usr/bin/env python

"""Tool for connecting to Cloud Bigtable Emulator and run some basic operations."""

import argparse
import datetime
import json
import sys
import struct

from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable.row_filters import RowKeyRegexFilter

def create_table(project_id, instance_id, table_id, data):
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


    body = json.loads(data.read())
    column_families = { family['name']: None for family in body['column_families'] }

    # Check whether table exists in an instance.
    # Create table if it does not exists.
    print('Checking if table {} exists...'.format(table_id))
    if table.exists():
        print('Table {} already exists.'.format(table_id))
    else:
        print('Creating the {} table.'.format(table_id))
        table.create(column_families=column_families)
        print('Created table {}.'.format(table_id))

    return client, instance, table

def list_tables(project_id, instance_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)

    tables = instance.list_tables()
    print('Listing tables in current project...')
    if tables != []:
        for tbl in tables:
            print(tbl.table_id)

def write(project_id, instance_id, table_id, schema_file, data):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    schema = json.loads(schema_file.read())

    body = json.loads(data.read())

    for row_body in body['rows']:
        row_key = row_body['rowkey']
        row = table.direct_row(row_key)

        for col in row_body['columns']:
            family_id, column_id = col['key'].split(':')
            if 'timestamp' in col:
                timestamp = datetime.datetime.utcfromtimestamp(col['timestamp'])
            else:
                timestamp = datetime.datetime.utcnow()

            schema_family = next((x for x in schema['column_families'] if x['name'] == family_id), None)
            schema_column = next((x for x in schema_family['columns'] if x['key'] == column_id), None)

            schema_type = schema_column['type']

            value = col['value']
            if schema_type == 'long':
                value = struct.pack(">q", value)
            elif schema_type == 'double':
                value = struct.pack(">d", value)
            elif schema_type == 'list_double':
                value = struct.pack(f'>{len(value)}d', *value)
            else:
                value = value.encode('utf-8')

            row.set_cell(family_id, column_id, value, timestamp=timestamp)

        row.commit()

        print('Successfully wrote row {}.'.format(row_key))

def read(project_id, instance_id, table_id, schema_file, json_output=False, limit=None, rowkey=None):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    schema = json.loads(schema_file.read())

    filters = None
    if rowkey is not None:
        filters = RowKeyRegexFilter(rowkey)

    rows = table.read_rows(limit=limit, filter_=filters)

    if json_output:
        output = []

    for row in rows:
        if json_output:
            columns = []

            for family, cells in row.cells.items():
                for key, col in cells.items():
                    col = col[0]

                    schema_family = next((x for x in schema['column_families'] if x['name'] == family), None)
                    schema_column = next((x for x in schema_family['columns'] if x['key'] == key.decode('utf-8')), None)
                    schema_type = schema_column['type']
                    if schema_type == 'long':
                        decoded_value = struct.unpack(">q", col.value)[0]
                    elif schema_type == 'double':
                        decoded_value = struct.unpack(">d", col.value)[0]
                    elif schema_type == 'list_double':
                        encoding = ">{}d".format(int(len(col.value) / 8))
                        decoded_value = struct.unpack(encoding, col.value)
                    else:
                        decoded_value = col.value.decode('utf-8')

                    columns.append({
                        'key': '{}:{}'.format(family, key.decode()),
                        'value': decoded_value,
                        'timestamp': col.timestamp.timestamp()
                    })

            output.append({
                'rowkey': row.row_key.decode(),
                'columns': columns
            })
        else:
            print('Row',row.row_key, row.to_dict())

    if json_output:
        print(json.dumps({
            'rows': output
        }))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('project_id',
                        help='Your Cloud Platform project ID.')
    parser.add_argument(
        'instance_id',
        help='ID of the Cloud Bigtable instance to connect to.')

    subparsers = parser.add_subparsers(dest='command')

    list_tables_parser = subparsers.add_parser('list-tables', help=list_tables.__doc__)

    create_table_parser = subparsers.add_parser('create-table', help=create_table.__doc__)

    create_table_parser.add_argument(
        'table',
        help='Cloud Bigtable Table name.',
        default='test')

    read_table_parser = subparsers.add_parser('read', help=read.__doc__)

    read_table_parser.add_argument(
        'table',
        help='Cloud Bigtable Table name.',
        default='test')

    read_table_parser.add_argument(
        'schema',
        type=argparse.FileType('r'),
        help='The table schema file address. See tables directory.')

    read_table_parser.add_argument(
        '--json',
        help='JSON output format for read',
        action='store_true',
        default=False)

    read_table_parser.add_argument(
        '--limit',
        help='limit number of outputs for read command. default is unlimited.',
        type=int,
        default=None)

    read_table_parser.add_argument(
        '--rowkey',
        help='get a specific rowkey',
        default=None)

    write_table_parser = subparsers.add_parser('write', help=write.__doc__)

    write_table_parser.add_argument(
        'table',
        help='Cloud Bigtable Table name.',
        default='test')

    write_table_parser.add_argument(
        'schema',
        type=argparse.FileType('r'),
        help='The table schema file address. See tables directory.')

    create_table_parser.add_argument('data', default=sys.stdin, type=argparse.FileType('r'), nargs='?')
    write_table_parser.add_argument('data', default=sys.stdin, type=argparse.FileType('r'), nargs='?')


    args = parser.parse_args()

    if args.command.lower() == 'create-table':
        create_table(args.project_id, args.instance_id, args.table, args.data)
    elif args.command.lower() == 'list-tables':
        list_tables(args.project_id, args.instance_id)
    elif args.command.lower() == 'write':
        write(args.project_id, args.instance_id, args.table, args.schema, args.data)
    elif args.command.lower() == 'read':
        read(args.project_id, args.instance_id, args.table, args.schema, args.json, args.limit, args.rowkey)
    else:
        print('Command should be either create-table list-tables read or write.\n Use argument -h,\
               --help to show help and exit.')
