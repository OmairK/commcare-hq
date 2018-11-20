"""Run SQL concurrently on partition databases

SQL statement templates may use the `{chunk_size}` placeholder, which
will be replaced with the value of the --chunk-size=N command argument.
"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals
import sys
import time
import traceback

import attr
import gevent
from django.core.management.base import BaseCommand
from django.db import connections
from six.moves import input

from corehq.sql_db.util import get_db_aliases_for_partitioned_query

MULTI_DB = 'Executing on ALL (%s) databases in parallel. Continue?'


class Command(BaseCommand):
    help = """Run SQL concurrently on partition databases."""

    def add_arguments(self, parser):
        parser.add_argument('name', choices=list(TEMPLATES), help="SQL statement name.")
        parser.add_argument('-d', '--dbname', help='Django DB alias to run on')
        parser.add_argument('--chunk-size', type=int, default=1000,
            help="Maximum number of records to move at once.")

    def handle(self, name, dbname, chunk_size, **options):
        template = TEMPLATES[name]
        sql = template.format(chunk_size=chunk_size)
        run = getattr(template, "run", run_once)
        dbnames = get_db_aliases_for_partitioned_query()
        if dbname or len(dbnames) == 1:
            run(sql, dbname or dbnames[0])
        elif not confirm(MULTI_DB % len(dbnames)):
            sys.exit('abort')
        else:
            greenlets = []
            for dbname in dbnames:
                g = gevent.spawn(run, sql, dbname)
                greenlets.append(g)

            gevent.joinall(greenlets)
            try:
                for job in greenlets:
                    job.get()
            except Exception:
                traceback.print_exc()


def confirm(msg):
    return input(msg + "\n(y/N) ").lower() == 'y'


def run_once(sql, dbname):
    """Run sql statement once on database

    This is the default run mode for statements
    """
    print("running on %s database" % dbname)
    with connections[dbname].cursor() as cursor:
        cursor.execute(sql)


@attr.s
class RunUntilZero(object):
    """SQL statement to be run repeatedly

    ...until the first column of of the first returned row is zero.
    """

    sql = attr.ib()

    def format(self, **kw):
        return self.sql.format(**kw)

    @staticmethod
    def run(sql, dbname):
        next_update = 0
        total = 0
        with connections[dbname].cursor() as cursor:
            while True:
                cursor.execute(sql)
                rows = cursor.fetchmany(2)
                assert len(rows) == 1 and len(rows[0]) == 1, \
                    "expected 1 row with 1 column, got %r" % (rows,)
                moved = rows[0][0]
                if not moved:
                    break
                total += moved
                now = time.time()
                if now > next_update:
                    print("{}: processed {} items".format(dbname, total))
                    next_update = now + 5
        print("{} final: processed {} items".format(dbname, total))


# see https://github.com/dimagi/commcare-hq/pull/21631
BLOBMETA_KEY_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS form_processor_xformattachmentsql_blobmeta_key
ON public.form_processor_xformattachmentsql (((
    CASE
        WHEN blob_bucket = '' THEN '' -- empty bucket -> blob_id is the key
        ELSE COALESCE(blob_bucket, 'form/' || attachment_id) || '/'
    END || blob_id
)::varchar(255)))
"""


TEMPLATES = {
    "blobmeta_key": BLOBMETA_KEY_SQL,
}
