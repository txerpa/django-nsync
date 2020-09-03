import inspect
from itertools import tee

from django.contrib.contenttypes.models import ContentType
from django.db import transaction, connection
from django.core.exceptions import FieldDoesNotExist
from django.conf import settings
from .actions import ReferenceActionMixin
from .models import ExternalKeyMapping, ExternalSystem
from django.db.models.fields import DateField
from django.db.models import signals, Max
from .management.commands.utils import temp_disconnect_signal, get_signal_info
import logging


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class AutoNowMixin:
    disabled_auto_now_date_fields = None

    def __init__(self, *args, **kwargs):
        self.disabled_auto_now_date_fields = list()

    def turn_off_auto_now(self):
        """
        Disable auto_now/auto_now_add of date fields in order to be able to save original date
        instead of the current date
        """
        for field_name in self.header:
            try:
                field = self.model._meta.get_field(field_name)
            except FieldDoesNotExist:
                continue
            if isinstance(field, DateField) and (field.auto_now or field.auto_now_add):
                field.auto_now = False
                field.auto_now_add = False
                self.disabled_auto_now_date_fields.append(field)

    def turn_on_auto_now(self):
        """
        Enable back on disabled auto_now/auto_now_add for date fields
        :return:
        """
        for disabled_auto_now_field in self.disabled_auto_now_date_fields:
            disabled_auto_now_field.auto_now = True
            disabled_auto_now_field.auto_now_add = True


class BasicSyncPolicy(AutoNowMixin):
    """ A synchronisation policy that simply executes each action in order. """

    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'

    action = None
    model = None

    header = []

    num_of_model_actions = 0
    num_of_executed_actions = 0

    def __init__(self, actions, model=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.actions = actions
        self.model = model

    def execute(self, actions=None):
        # When bulk oper is used signals are disabled by default, so check if signals have to be disabled
        # in the case bulk is not used
        if settings.SYNC_SIGNAL_DISCONNECT:
            model_name = self.model._meta.model_name
            post_save_receiver, post_save_dispatch = get_signal_info('post_save', model_name)
            post_save_kwargs = {
                'signal': signals.post_save,
                'receiver': post_save_receiver,
                'sender': self.model,
                'dispatch_uid': post_save_dispatch
            }
            post_delete_receiver, post_delte_dispatch = get_signal_info('post_delete', model_name)
            post_delete_kwargs = {
                'signal': signals.post_delete,
                'receiver': post_delete_receiver,
                'sender': self.model,
                'dispatch_uid': post_delte_dispatch
            }
            with temp_disconnect_signal(**post_save_kwargs), temp_disconnect_signal(**post_delete_kwargs):
                self.execute_actions(actions)
        else:
            self.execute_actions(actions)

        self.turn_on_auto_now()
        if self.num_of_model_actions != self.num_of_executed_actions:
            logger.warning(f'Actions are skipped! {self.num_of_model_actions - self.num_of_executed_actions} '
                           f'actions are not executed for the model {self.model._meta.db_table}')

    def execute_actions(self, actions):
        actions = actions or self.actions
        for row_actions in actions:
            row_action_types = []
            for action in row_actions:
                if action.type not in row_action_types:
                    row_action_types.append(action.type)
                if action.type != self.DELETE and not self.header:
                    self.header = action.fields.keys()
                    self.turn_off_auto_now()
                self.execute_row_action(action)
            self.num_of_model_actions += len(row_action_types)

    def execute_row_action(self, action):
        instance = action.execute(use_bulk=False)
        if instance and instance.pk:
            self.num_of_executed_actions += 1


class BulkSyncPolicy(AutoNowMixin):
    """ A synchronisation policy that executes actions in batch. """

    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'

    action = None
    model = None

    def __init__(self, actions, model, batch_size=500, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.actions = actions
        self.batch_size = 500 if not isinstance(batch_size, int) else batch_size
        self.model = model

        self.header = []

        self.num_of_model_actions = 0
        self.num_of_executed_actions = 0

        # lists to hold model instances in memory when bulk operations are enabled
        self.create_instances = list()
        self.create_ref_instances = list()
        self.update_instances = list()
        self.update_ref_instances = list()
        self.delete_instances = list()

    def execute(self, actions=None):
        # When bulk oper is used signals are disabled by default, so check if signals have to be disabled
        # in the case bulk is not used
        model_name = self.model._meta.model_name
        post_save_receiver, post_save_dispatch = get_signal_info('post_save', model_name)
        post_save_kwargs = {
            'signal': signals.post_save,
            'receiver': post_save_receiver,
            'sender': self.model,
            'dispatch_uid': post_save_dispatch
        }
        post_delete_receiver, post_delte_dispatch = get_signal_info('post_delete', model_name)
        post_delete_kwargs = {
            'signal': signals.post_delete,
            'receiver': post_delete_receiver,
            'sender': self.model,
            'dispatch_uid': post_delte_dispatch
        }

        with temp_disconnect_signal(**post_save_kwargs), temp_disconnect_signal(**post_delete_kwargs):
            self.execute_actions(actions)

        # bulk persist any instances which are still pending
        self.bulk_create_with_ref()
        self.bulk_update_with_ref()
        self.bulk_delete()

        self.turn_on_auto_now()

        if self.num_of_model_actions != self.num_of_executed_actions:
            logger.warning(f'Actions are skipped! {self.num_of_model_actions - self.num_of_executed_actions} '
                           f'actions are not executed for the model {self.model._meta.db_table}')

    def execute_actions(self, actions):
        actions = actions or self.actions
        for row_actions in actions:
            row_action_types = []
            for action in row_actions:
                if action.type not in row_action_types:
                    row_action_types.append(action.type)
                if action.type != self.DELETE and not self.header:
                    self.header = action.fields.keys()
                    self.turn_off_auto_now()
                self.execute_row_action(action)
            self.num_of_model_actions += len(row_action_types)

    def execute_row_action(self, action):
        instance = action.execute(use_bulk=True)
        if instance:
            # Note: It is important to keep this order of bulk action execution -> create-update-delete
            if action.type == self.CREATE and not action.obj_already_created:
                self.append_for_bulk(action, instance, self.create_instances, self.create_ref_instances)
                if self.has_batch_size(self.create_instances):
                    self.bulk_create_with_ref()
            elif action.type == self.UPDATE:
                self.append_for_bulk(action, instance, self.update_instances, self.update_ref_instances)
                if self.has_batch_size(self.update_instances):
                    self.bulk_update_with_ref()
            elif action.type == self.DELETE:
                self.delete_instances.append(instance)
                if self.has_batch_size(self.delete_instances):
                    self.bulk_delete()

    def append_for_bulk(self, action, instance, instances, ref_instances):
        instances.append(instance)
        if isinstance(action, ReferenceActionMixin):
            ref_instances.append(action.external_mapping_obj)

    def has_batch_size(self, instances):
        return len(instances) == self.batch_size

    def bulk_create_with_ref(self):
        created_instances = self.bulk_create(self.model, self.create_instances)
        self.num_of_executed_actions += len(created_instances)
        if self.create_ref_instances:
            self.map_to_external_objects(created_instances, self.create_ref_instances)
            self.bulk_create(ExternalKeyMapping, self.create_ref_instances)

    def bulk_update_with_ref(self):
        updated_instances = self.bulk_update(self.model, self.update_instances, self.header)
        self.num_of_executed_actions += len(updated_instances)
        if self.update_ref_instances:
            self.map_to_external_objects(updated_instances, self.update_ref_instances)
            self.bulk_update(ExternalKeyMapping, self.update_ref_instances, ['object_id'])

    def map_to_external_objects(self, instances, ext_mapp_instances):
        # Note: Limit to map instances to external obj is to have all objects as externaly mappable
        assert len(instances) == len(ext_mapp_instances)
        for instance, ext_mapp_instance in zip(instances, ext_mapp_instances):
            ReferenceActionMixin.map_to_external_object(ext_mapp_instance, instance)

    def bulk_create(self, model, instances):
        """
        Creates objects by calling ``bulk_create``.
        Note:
            - The model’s save() method will not be called, and the pre_save and post_save signals will not be sent.
            - If the model’s primary key is an AutoField it does not retrieve and set the primary key attribute,
            as save() does, unless the database backend supports it (currently PostgreSQL).
            - It does not work with many-to-many relationships.
        """
        objs = []
        try:
            if len(instances) > 0:
                objs = model.objects.bulk_create(instances, batch_size=self.batch_size)
        except Exception as e:
            logger.exception(e)
        finally:
            instances.clear()
            return objs

    def bulk_update(self, model, instances, fields):
        """
        Updates objects by calling ``bulk_update``.
        Note:
            - You cannot update the model’s primary key.
            - Each model’s save() method isn’t called, and the pre_save and post_save signals aren’t sent.
            - Updating fields defined on multi-table inheritance ancestors will incur an extra query per ancestor.
            - If objs contains duplicates, only the first one is updated
        """
        updated_instances = []
        try:
            if len(instances) > 0:
                model.objects.bulk_update(instances, fields, batch_size=self.batch_size)
                updated_instances = instances
        except Exception as e:
            logger.exception(e)
        finally:
            instances.clear()
            return updated_instances

    def bulk_delete(self):
        """
        Deletes objects by filtering on a list of instances to be deleted,
        then calling ``delete()`` on the entire queryset.
        Note:
            - Bulk delete does not call any delete() methods on your models.
            - It does, however, emit the pre_delete and post_delete signals for all deleted objects
            (including cascaded deletions) and so all the objects that are to be deleted are pulled into memory
        """
        try:
            if len(self.delete_instances) > 0:
                delete_ids = [o.pk for o in self.delete_instances]
                self.model.objects.filter(pk__in=delete_ids).delete()
                self.num_of_executed_actions += len(delete_ids)
        except Exception as e:
            logger.exception(e)
        finally:
            self.delete_instances.clear()


class MPTTBulkSyncPolicy(BulkSyncPolicy):
    """ A synchronisation policy that executes actions in batch. """

    pk_offset = None
    # external_to_internal_map = dict()

    def __init__(self, actions, model, batch_size=500, *args, **kwargs):
        super().__init__(actions, model, batch_size=batch_size, *args, **kwargs)

    def execute(self, actions=None):
        if self.pk_offset is None:
            self.compute_pk_offset()

        actions = actions or self.actions
        if inspect.isgenerator(actions):
            # Duplicate the original actions generator
            actions_1, actions_2 = tee(actions)
        else:
            actions_1, actions_2 = actions, actions

        # TODO: review this two lines... must be passed in instead of getting this way?
        external_system = ExternalSystem.objects.first()
        dct = ContentType.objects.get(model=self.model._meta.model_name, app_label=self.model._meta.app_label)

        # Map all pks
        mapped_objects = list()
        mapped_external_keys = set()
        n = 0
        for row_actions in actions_1:
            for action in row_actions:
                # TODO: This not works as expected but I don't know why... :S
                # assert action.type != 'create', f"{self.__class__.__name__} can only be used with \"create\" actions"

                external_pk = action.external_key
                if external_pk in mapped_external_keys:
                    continue

                n += 1
                new_internal_pk = self.pk_offset + n

                external_obj = ExternalKeyMapping()
                external_obj.object_id = new_internal_pk
                external_obj.content_type = dct
                external_obj.external_key = external_pk
                external_obj.external_system = external_system  # TODO: Only exists one?

                mapped_external_keys.add(external_pk)
                mapped_objects.append(external_obj)

                action.force_init_instace = False
                # action.external_mapping_obj = external_obj

        ExternalKeyMapping.objects.bulk_create(mapped_objects)

        super().execute(actions=actions_2)

        # Update table's pk sequence on postgres with last inserted id
        self.update_table_pk_sequence(self.model._meta.db_table)

    def bulk_create_with_ref(self):
        created_instances = self.bulk_create(self.model, self.create_instances)
        self.num_of_executed_actions += len(created_instances)

    def bulk_update_with_ref(self):
        updated_instances = self.bulk_update(self.model, self.update_instances, self.header)
        self.num_of_executed_actions += len(updated_instances)

    def compute_pk_offset(self):
        res = self.model.objects.aggregate(max_id=Max('pk'))
        self.pk_offset = res.get('max_id') or 0

    def update_table_pk_sequence(self, table_name):
        with transaction.atomic(), connection.cursor() as cursor:
            cursor.execute(
                f"""
                BEGIN;
                -- protect against concurrent inserts while you update the counter
                LOCK TABLE {table_name} IN EXCLUSIVE MODE;
                -- Update the sequence
                SELECT setval('{table_name}_id_seq', COALESCE((SELECT MAX(id)+1 FROM {table_name}), 1), false);
                COMMIT;
                """
            )
            logger.info(f" > Table pk sequence updated: {table_name}")


class TransactionSyncPolicy:
    """
    A synchronisation policy that wraps other sync policies in a database
    transaction.

    This allows the changes from all of the actions to occur in an atomic
    fashion. The limit to the number of transactions is database dependent
    but is usually quite large (i.e. like 2^32).
    """
    def __init__(self, policy):
        self.policy = policy

    def execute(self):
        with transaction.atomic():
            self.policy.execute()


class OrderedSyncPolicy(BasicSyncPolicy):
    """
    A synchronisation policy that performs the actions in a controlled order.

    This policy filters the list of actions and executes all of the create
    actions, then all of the update actions and finally all of the delete
    actions. This is to ensure that the whole list of actions behaves more
    predictably.

    For example, if there are create actions and forced delete actions for
    the same object in the list, then the net result of the state of the
    objects will depend on which action is performed first. If the order is
    'create' then 'delete', the object will be created and then deleted. If
    the order is 'delete' then 'create', the delete action will fail and
    then the object will be created. This policy avoids this situation by
    performing the different types in order.

    This also helps with referential updates, where an update action might be
    earlier in the list than the action to create the referred to object.
    """

    def execute(self):
        for filter_by in [self.CREATE, self.UPDATE, self.DELETE]:
            filtered_actions = filter(lambda a: a.type == filter_by,
                                      self.actions)
            super(OrderedSyncPolicy, self).execute(filtered_actions)
