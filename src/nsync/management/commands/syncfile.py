from django.core.management.base import BaseCommand, CommandError
import os
import csv
import ctypes as ct

from .utils import ExternalSystemHelper, ModelFinder, CsvActionFactory
from nsync.policies import BasicSyncPolicy, TransactionSyncPolicy


class Command(BaseCommand):
    help = 'Synchonise model info from one file'

    def add_arguments(self, parser):
        # Mandatory
        parser.add_argument(
            'ext_system_name',
            help='The name of the external system to use for storing '
                 'sync information in relation to')
        parser.add_argument(
            'app_label',
            default=None,
            help='The name of the application the model is part of')
        parser.add_argument(
            'model_name',
            help='The name of the model to synchronise to')
        parser.add_argument(
            'file_name',
            help='The file to synchronise from')

        # Optional
        parser.add_argument(
            '--create_external_system',
            type=bool,
            default=True,
            help='The name of the external system to use for storing '
                 'sync information in relation to')
        parser.add_argument(
            '--as_transaction',
            type=bool,
            default=False,
            help='Wrap all of the actions in a DB transaction Default:True')
        parser.add_argument(
            '--rel_by_external_key',
            type=bool,
            default=False,
            help='If related field is referenced by external key'
        )
        parser.add_argument(
            '--rel_by_external_key_excluded',
            action='append',
            default=[],
            help='Name of tables for which related fields are not referenced by external key. '
                 'These are exceptions for rel_by_external_key'
        )
        parser.add_argument(
            '--use_bulk',
            type=bool,
            default=False,
            help='Controls whether operations should be performed in bulk. '
                 'By default, an object\'s save() method is called for each row in a data set. '
                 'When bulk is enabled, objects are saved using bulk operations'
        )
        parser.add_argument(
            '--force_init_instance',
            type=bool,
            default=False,
            help='If True, this parameter will prevent from checking the database for existing instances. '
                 'Enabling this parameter is a performance improvement if data is guaranteed to contain '
                 'new instances only'
        )

    def handle(self, *args, **options):
        external_system = ExternalSystemHelper.find(
            options['ext_system_name'], options['create_external_system'])
        model = ModelFinder.find(options['app_label'], options['model_name'])

        filename = options['file_name']
        if not os.path.exists(filename):
            raise CommandError("Filename '{}' not found".format(filename))

        with open(filename) as f:
            # TODO - Review - This indirection is only due to issues in
            # getting the mocks in the tests to work
            SyncFileAction.sync(external_system,
                                model,
                                f,
                                use_transaction=options['as_transaction'],
                                rel_by_external_key=options['rel_by_external_key'],
                                rel_by_external_key_excluded=options['rel_by_external_key_excluded'],
                                use_bulk=options['use_bulk'],
                                force_init_instance=options['force_init_instance']
                                )


class SyncFileAction:
    @staticmethod
    def sync(external_system, model, file, use_transaction=True,
             rel_by_external_key=False, rel_by_external_key_excluded=False,
             use_bulk=False, force_init_instance=False):
        # Increase csv field size limit, set the limit using an artifice
        csv.field_size_limit(int(ct.c_ulong(-1).value // 2))
        reader = csv.DictReader(file)
        builder = CsvActionFactory(model, external_system=external_system,
                                   rel_by_external_key=rel_by_external_key,
                                   rel_by_external_key_excluded=rel_by_external_key_excluded,
                                   force_init_instance=force_init_instance)

        def get_actions():
            for d in reader:
                yield builder.from_dict(d)

        policy = BasicSyncPolicy(get_actions(), use_bulk=use_bulk, model=model)

        if use_transaction:
            policy = TransactionSyncPolicy(policy)

        policy.execute()
