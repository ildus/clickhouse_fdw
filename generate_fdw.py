#!/usr/bin/env python3
#coding: utf-8

import json
import sys
import requests
import argparse

parser = argparse.ArgumentParser(description='generate PostgreSQL definitions from ClickHouse tables')
parser.add_argument('tables', metavar='N', type=str, nargs='+', help='tables')
parser.add_argument('-H', '--host', type=str, dest='host', default='localhost',
        help='specify remote host')
parser.add_argument('-P', '--port', type=str, dest='port', default='8123',
        help='specify remote port')
parser.add_argument('-D', '--database', type=str, dest='database', default='default',
        help='specify remote database')
parser.add_argument('-S', '--server_name', type=str, dest='server_name', default='remote',
        help='default server name (for CREATE SERVER command)')

types_map = {
    'Int8': 'smallint',
    'UInt8': 'smallint',
    'Int16': 'smallint',
    'UInt16': 'integer',
    'Int32': 'integer',
    'UInt32': 'bigint',
    'Int64': 'bigint',
    'UInt64': 'bigint', # overflow risk
    'Float32': 'real',
    'Float64': 'double precision',
    'Decimal': 'numeric',
    'Boolean': 'boolean',
    'String': 'text',
    'Date': 'date',
    'DateTime': 'timestamp',
    'UUID': 'text',
    'FixedString': 'text',
    'Enum8': 'text',
    'Enum16': 'text'
}


def make_query(host, port, query):
    resp = requests.get("http://%s:%s" % (host, port), params={'query': query})
    if resp.status_code == 200:
        return resp.content.decode('utf8')

    raise Exception(resp.content.decode('utf8'))


def get_definition(host, port, name):
    query ="describe table %s format JSON" %  (name)
    return json.loads(make_query(host, port, query))


def get_tables(host, port, database):
    query ="select name from system.tables where database = '%s' format JSON;" % database
    return json.loads(make_query(host, port, query))


if __name__ == '__main__':
    args = parser.parse_args()
    database = args.database

    server_name = args.server_name
    sql = 'CREATE EXTENSION clickhouse_fdw;\n'
    sql += """CREATE SERVER IF NOT EXISTS %s
        FOREIGN DATA WRAPPER clickhouse_fdw
        OPTIONS (dbname '%s', host '%s', port '%s');\n""" \
        % (server_name, database, args.host, args.port)
    sql += "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER SERVER %s;\n" % server_name

    tables = args.tables
    if len(tables) == 1 and tables[0] == '*':
        data = get_tables(args.host, args.port, database)
        tables = []
        for row in data['data']:
            tables.append(row['name'])

    for table_name in tables:
        add_new_line = False

        definition = get_definition(args.host, args.port, '%s.%s' % (database, table_name))
        sql += '\nCREATE FOREIGN TABLE %s (\n' % table_name
        for row in definition['data']:
            try:
                if add_new_line:
                    sql += ',\n'
                else:
                    add_new_line = True

                is_nullable = False
                is_array = False
                remote_type = row['type']

                while '(' in remote_type:
                    if remote_type.startswith('Decimal'):
                        remote_type = 'Numeric' + remote_type.replace('Decimal', '')

                        # when there is only one number then it's scale in ClickHouse
                        # but for PostgreSQL it's precision
                        assert(',' in remote_type)
                        break
                    if remote_type.startswith('FixedString'):
                        remote_type = 'varchar' + remote_type.replace('FixedString', '')
                        break
                    elif remote_type.startswith('Array'):
                        is_array = True
                    elif remote_type.startswith('Nullable'):
                        is_nullable = True
                    elif remote_type.startswith('LowCardinality'):
                        pass

                    remote_type = remote_type.split('(', 1)[1][:-1]

                if '(' in remote_type:
                    # complex enough
                    sql += '\t%s %s' % (row['name'], remote_type.upper())
                else:
                    sql += '\t%s %s' % (row['name'], types_map[remote_type].upper())

                if is_array:
                    sql += '[]'

                if not is_nullable:
                    sql += ' NOT NULL'

            except:
                print(row)
                raise

        sql += "\n) SERVER %s OPTIONS (table_name '%s');\n" % (server_name, table_name)

    print(sql)
