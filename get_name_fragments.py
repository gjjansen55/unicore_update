#!/usr/bin/python3


"""
A rewrite of the old Perl version of get_name_fragments.pl

In light of what I see in the files, we need to be more efficient at
  a. Stripping organization descriptions off the tail end of a name
  ('CO', 'LTD', 'GMBH')
  b. Stripping articles off the front of a name ('AN', 'THE', 'LE').
  c. Stripping useless singletons where they occur.

  The rules therefore are
  a. The original form of a name is always preserved: if someone wants to
     search for "THE COMPANY STORE" or "AMERICAN PSYCHOLOGICAL ASSOCIATION",
     that string will be there. But thereafter
  b. We strip articles from the front.
  c. We strip organization descriptions from the end.
  d. We do discard useless one-word tokens. So "CHURCH" and "FAMILY" do not
     appear with a Duns number; but either of "FAMILY CHURCH" or
     "CHURCH FAMILY" will.
  e. We also discard useless two-word tokens now.

"""

import argparse
import logging
import os.path
import time

import afl.dbconnections

REPORTING_INTERVAL = 500000

ARTICLES = set(['AN', 'THE', 'LE', 'LES'])

ORGANIZATION_TYPES = set(['A', 'ASSOCIACAO',
                          'ASSOCIATION', 'CO', 'CO.,LTD.', 'COMPANY', 'CORP',
                          'CORPORATION',
                          'FIRMA', 'GMBH',
                          'INC', 'INC.', 'INCORPORATED',
                          'LTD', 'LTD.', 'LLC', 'LTDA',
                          'O', 'OOO',
                          'SP',
                          'W',
                          'Z'])

UNWANTED_AFFIXES = ['AND', 'OF', 'THE']


def make_fragmenter(stopwords):
    """ Given a list of stopwords, return a function to return a list
    of useful substrings of the name.
    """

    def _inner(name):
        """ We discard articles from the left, organization types from
        the right, then begin to create our substrings. We discard only
        single-word substrings that fall into the list of stopwords.
        Probably we could pick something by removing digraphs also, but
        that is for another day.
        The except branch is to catch cases where we run out of words,
        perhaps if the 1960s band 'The Association' were in the list.

        Some non-obvious points about names:
        fvt is 'first valid token'
        lvt is 'last valid token'
        UNWANTED_AFFIXES holds words that may not appear at the beginning
        or end of a string."""

        tokens = [t.strip(' .,') for t in name.split(' ')
                  if t.strip(' .,') != '']

        variants = set([name])
        try:
            fvt = 0
            while fvt < len(tokens) and tokens[fvt] in ARTICLES:
                fvt += 1
            if fvt == len(tokens):
                return variants
            lvt = len(tokens) - 1
            while lvt >= 0 and tokens[lvt] in ORGANIZATION_TYPES:
                lvt -= 1
            for i in range(fvt, lvt + 1):
                if tokens[i] in UNWANTED_AFFIXES:
                    continue
                for j in range(i, lvt+1):
                    if tokens[j] in UNWANTED_AFFIXES:
                        continue
                    fragment = ' '.join(tokens[i:j+1])
                    if fragment not in stopwords:
                        variants.add(fragment)
        except IndexError:
            print('failed to tokenize "%s"' % name)
            pass

        return variants

    return _inner


def _make_handler(directory_name):
    """ set up the file handles that we will write to. """

    _handles = {}
    _handles['1'] = open(os.path.join(directory_name, '1.txt'), 'w')
    _handles['MV'] = open(os.path.join(directory_name, 'MV.txt'), 'w')
    for i in range(65,91):
        ltr = chr(i)
        _handles[ltr] = open(os.path.join(directory_name, '%s.txt' % ltr), 'w')

    def _inner(fragment, duns_nbr):
        """ pick out the appropriate handle, write an item. """

        initial = fragment[0]
        if initial in _handles:
            handle = _handles[initial]
        elif initial < 'A':
            handle = _handles['1']
        else:
            handle = _handles['MV']
        handle.write('%s\t%s\n' % (fragment, duns_nbr))

    return _inner

def get_stopwords(conn):
    """ Return a set of what we consider to be the words not useful
    in themselves in executing a query.
    """

    cur = conn.cursor()
    cur.execute("""SELECT word
    FROM unicore.business_stopwords""")

    return set([row[0] for row in cur.fetchall()])


def get_name_fragments(username, output_directory):
    """ username is either unicore_p or unicore_p2
    output_directory is the directory in which tab-delimited files
    will be written."""


    logging.basicConfig(level=logging.DEBUG)
    conn = afl.dbconnections.connect('unicore_helper')
    stopwords = get_stopwords(conn)
    fragmenter = make_fragmenter(stopwords)
    rows, fragments_written = 0, 0
    handler = _make_handler(output_directory)
    cur = conn.cursor()
    cur.execute(f"""SELECT duns_nbr, business_name, secondary_name
        FROM {username}.business_common""")
    while True:
        row = cur.fetchone()
        if row is None:
            break
        duns_nbr, business_name, secondary_name = row
        seen = set([])
        for name in [business_name, secondary_name]:
            if name is not None:
                fragments = fragmenter(name)
                for fragment in fragments:
                    if fragment not in seen:
                        handler(fragment, duns_nbr)
                        seen.add(fragment)
                        fragments_written += 1
        rows += 1
        if rows % REPORTING_INTERVAL == 0:
            logging.info('have read %d records and written %d fragments',
                         rows, fragments_written)
            time.sleep(30)


parser = argparse.ArgumentParser(
    'a newer script for generating name fragments')
parser.add_argument('username',
                    help='the schema for which we are running this')
parser.add_argument('output_directory',
                    help='directory to which files will be written')
args = parser.parse_args()
get_name_fragments(args.username, args.output_directory)
