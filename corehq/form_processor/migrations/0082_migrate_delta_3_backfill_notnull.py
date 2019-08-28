
from django.db import migrations

from corehq.sql_db.operations import RawSQLMigration


migrator = RawSQLMigration(('corehq', 'form_processor', 'migrations', 'sql_templates'))


class Migration(migrations.Migration):

    dependencies = [
        ('form_processor', '0081_migrate_delta_2_create_trigger'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrator.get_migration('migrate_delta_3_backfill_notnull.sql'),
            ]
        ),
    ]
