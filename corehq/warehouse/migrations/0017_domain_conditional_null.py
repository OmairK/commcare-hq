# Generated by Django 1.11.8 on 2018-01-24 09:34

from django.db import migrations, models




class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0016_readd_batch_key'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='userdim',
            name='domain',
        ),
        migrations.AlterField(
            model_name='userstagingtable',
            name='domain',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
        migrations.RunSQL(
            """
            ALTER TABLE warehouse_userstagingtable
            ADD CONSTRAINT domain_conditional_null CHECK
            ((doc_type = 'WebUser' and domain IS NULL) OR (doc_type = 'CommCareUser' and domain is NOT NULL))
            """,
            reverse_sql="""
            ALTER TABLE warehouse_userstagingtable
            DROP CONSTRAINT IF EXISTS domain_conditional_null;
            """
        ),
    ]