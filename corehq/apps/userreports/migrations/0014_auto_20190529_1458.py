# Generated by Django 1.11.20 on 2019-05-29 14:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('userreports', '0013_reportcomparisondiff_reportcomparisonexception_reportcomparisontiming'),
    ]

    operations = [
        migrations.AddField(
            model_name='reportcomparisondiff',
            name='notes',
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name='reportcomparisonexception',
            name='notes',
            field=models.TextField(blank=True),
        ),
    ]
