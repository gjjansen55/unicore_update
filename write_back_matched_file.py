#!/usr/bin/python3

"""
A script to obviate (we hope) the need for multiple SQL*Loader control files
to load up the match results.

This varies somewhat from export_from_ace.py in that the destination table is
always in username's schema.
"""

import argparse
import configparser
import os.path

import cx_Oracle

DBNAME = 'pdb5'


def write_back_matched_file(config_pathname, data_type, owner=None):
    """ write the data back to the database. """

    def get_connection(username):
        """ log in, turn autocommit on. """

        password = input(f'password for {username}: ')
        conn = cx_Oracle.connect(username, password, DBNAME)
        conn.autocommit = True

        return conn

    def get_owner_table_name_and_pathname_stem(config_pathname, owner):
        """ Read the pieces we need out of the configuration file. """
        """ We need two things here:
        a. the name of the table or view to be used
        b. stem of the pathname for the data and format files.
        """

        config = configparser.ConfigParser()
        config.read(config_pathname)
        for section_name in 'master', data_type:
            if section_name not in config.sections():
                print(f'Section {section_name} not found in',
                      f'{config_pathname}! Exiting.')
                exit()
        table_name = config[data_type]['table_written']
        filename_stem = config[data_type]['filename_stem']
        data_directory = config['master']['data_directory']
        if owner is None:
            owner = config[data_type]['owner']

        return owner, table_name, os.path.join(data_directory, filename_stem)

    def make_processor(format_path):
        """ We need to extract our locations from the fmt file. """

        items_wanted = set(['MATCH_LEVEL', 'DUNS_NBR', 'ID'])
        start, length = 0, 0
        offsets = {}
        with open(format_path, 'r') as ifh:
            for line in ifh:
                name, slength = [f.strip() for f in line.split(',')[0:2]]
                length = int(slength)
                end = start + length
                if name in items_wanted:
                    offsets[name] = (start, end)
                start = end
        for name in items_wanted:
            if name not in offsets:
                print(f'item {name} not found, exiting')
                exit()

        def _inner(line):
            """ return a dictionary giving offsets """

            retval = {}
            for name in offsets:
                start, end = offsets[name]
                retval[name] = line[start:end].strip()

            return retval

        return _inner

      
    def update_table(cur, table_name, data_pathname, line_processor):
        """ read through and apply. """

        stmt_text = f"""UPDATE {table_name}
        SET MATCH_LEVEL = :MATCH_LEVEL, 
            DUNS_NBR = :DUNS_NBR 
        WHERE ID = :ID"""

        buffer = []
        rows = 0
        with open(data_pathname, 'r', encoding='latin1') as ifh:
            for line in ifh:
                buffer.append(line_processor(line))
                rows += 1
                if rows % 1000 == 0:
                    cur.executemany(stmt_text, buffer)
                    buffer = []
        if len(buffer):
            cur.executemany(stmt_text, buffer)

    owner, table_name, pathname_stem = get_owner_table_name_and_pathname_stem(
        config_pathname, owner)
    conn = get_connection(owner)
    cur = conn.cursor()
    line_processor = make_processor(pathname_stem + '.fmt')
    update_table(cur, table_name, pathname_stem + '.dat', line_processor)


parser = argparse.ArgumentParser('script to load up UNICORE match information')
parser.add_argument('config_pathname',
                    help='pathname of an ini file with details for loading')
parser.add_argument('data_type',
                    help='contracts, OSHA, etc.')
parser.add_argument('--owner',
                    help='owner of the table, if not specified in config.')
args = parser.parse_args()
write_back_matched_file(args.config_pathname, args.data_type, args.owner)
