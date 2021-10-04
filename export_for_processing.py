#!/usr/bin/python3

"""
We aim to replace a lot of old Perl scripts that have been used for many years.
Rather than have one set of scripts for each table in each schema, we will
parameterize by schema and table.

In general, the schema will be either UNICORE or less commonly CATS. For the
business records refreshed each quarter, the schema will be either UNICORE_P
or UNICORE_P2, and then the user will need to use the
--owner argument.
"""

import argparse
import configparser
import os.path

import afl.dbconnections

DBNAME = 'pdb5'
TSTRING = type('string')


def export_for_ace(config_pathname, export_type, owner=None):
    """ Export a table in fixed format suitable for ACE, creating also a .fmt
    file for it. The value of pathname_stem will have '.dat' appended for the
    data file and '.fmt' appended for the format file. """

    def get_owner_table_name_and_pathname_stem(config_pathname, export_type,
                                               owner):
        """ We need three things here:
        a. the name of the table or view to be used
        b. stem of the pathname for the data and format files.
        c. (possibly) the owner of the table.
        """

        config = configparser.ConfigParser()
        config.read(config_pathname)
        for section_name in 'master', export_type:
            if section_name not in config.sections():
                print(f'Section {export_type} not found in {config_pathname}!',
                      'Exiting.')
                exit()
        table_name = config[export_type]['table_read']
        filename_stem = config[export_type]['filename_stem']
        data_directory = config['master']['data_directory']
        if owner is None:
            owner = config[export_type]['owner']

        return owner, table_name, os.path.join(data_directory, filename_stem)

    def get_connection():
        """ return a cx_Oracle.connection. """

        return afl.dbconnections.connect('unicore_helper')

    def get_column_details(cur, owner, table_name):
        """ We need the field names and widths. """

        cur.execute("""SELECT column_name, data_length
        FROM all_tab_columns
        WHERE owner = :owner
          AND table_name = :table_name
        ORDER BY column_id""", [owner, table_name])

        return cur.fetchall()

    def make_format_string(column_details):
        """ Because Python's struct.pack does not support space-padding
        in the manner of Perl's pack, we build a format string in the
        "{:<99}{:98}..." style. """

        return ''.join(['{:<' + str(cd[1]) + '}' for cd in column_details])

    def write_format_file(column_details, format_pathname):
        """ Create the file that FirstLogic wants to describe the
        file layout. """

        with open(format_pathname, 'w') as ofh:
            for name, length in column_details:
                ofh.write(f'{name}, {length}, C\n')
            ofh.write('EOR, 1, C\n')

    def process_row(row_in):
        """ Turn Nones to blanks, strip vertical white space. """

        def _per_column(value):
            """ fix each """

            if value is None:
                return ''
            if type(value) != TSTRING:
                return value
            if '\r' in str(value) or '\n' in value:
                return ' '.join(value.splitlines())

            return value

        return [_per_column(column) for column in row_in]

    owner, table_name, pathname_stem = get_owner_table_name_and_pathname_stem(
        config_pathname, export_type, owner)
    data_pathname = pathname_stem + '.dat'
    format_pathname = pathname_stem + '.fmt'
    conn = get_connection()
    cur = conn.cursor()
    column_details = get_column_details(cur, owner, table_name)
    format_string = make_format_string(column_details)
    column_string = ', '.join([cd[0] for cd in column_details])
#    print(format_string)
    cur.execute(f"""SELECT {column_string}  FROM {owner}.{table_name}""")
    with open(data_pathname, 'w', encoding='latin1', errors='replace') as ofh:
        for row in cur:
            row_out = process_row(row)
            ofh.write(format_string.format(*row_out) + '\n')
    write_format_file(column_details, format_pathname)


parser = argparse.ArgumentParser(
    'script to export data for ACE (and other FirstLogic processing.')
parser.add_argument('config_pathname',
                    help='provides mappings for the export type')
parser.add_argument('export_type',
                    help='type of data to be exported: determines table '
                    + 'and filename components')
parser.add_argument('--owner',
                    help='table owner, in case it varies')
args = parser.parse_args()
export_for_ace(args.config_pathname, args.export_type, args.owner)
