# Generated by Django 1.11.13 on 2018-06-20 17:03

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('warehouse', '0028_nullable_user_dim'),
    ]

    operations = [
        migrations.AlterField(
            model_name='userdim',
            name='first_name',
            field=models.TextField(null=True),
        ),
        migrations.AlterField(
            model_name='userdim',
            name='last_name',
            field=models.TextField(null=True),
        ),
        migrations.AlterField(
            model_name='userstagingtable',
            name='first_name',
            field=models.TextField(null=True),
        ),
        migrations.AlterField(
            model_name='userstagingtable',
            name='last_name',
            field=models.TextField(null=True),
        ),
    ]