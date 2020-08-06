
from django.db import migrations
import django.db.models.deletion
import tenant_schemas.fields
import tenant_schemas.models


class Migration(migrations.Migration):

    dependencies = [
        ('txdadmin', '0009_auto_20200219_1118'),
        ('contenttypes', '0002_remove_content_type_name'),
        ('nsync', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='externalkeymapping',
            name='tenant',
            field=tenant_schemas.fields.RLSForeignKey(blank=True, default=tenant_schemas.models.get_tenant, on_delete=django.db.models.deletion.PROTECT, to='txdadmin.Client', to_field='schema_name'),
        ),
        migrations.AlterUniqueTogether(
            name='externalkeymapping',
            unique_together={('external_system', 'external_key', 'content_type', 'tenant')},
        ),
        migrations.AlterIndexTogether(
            name='externalkeymapping',
            index_together={('external_system', 'external_key', 'content_type', 'tenant')},
        ),
    ]
