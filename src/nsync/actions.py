from django import VERSION
from django.core.exceptions import (
    MultipleObjectsReturned,
    ObjectDoesNotExist,
    FieldDoesNotExist)
from django.contrib.contenttypes.fields import ContentType, GenericForeignKey
from django.contrib.postgres.fields import JSONField
from django.db.models import Model
from django.db.models.query_utils import Q
from .models import ExternalKeyMapping
from collections import defaultdict
import logging
import ast
from .logging import StyleAdapter

"""
NSync actions for updating Django models

This module contains the available actions for performing synchronisations.
These include the basic Create / Update / Delete for model object, as well
as the actions for managing the ExternalKeyMapping objects, to record the
identification keys used by external systems for internal objects.

It is recommended to use the ActionFactory.build() method to create actions
from raw input.
"""

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # http://pieces.openpolitics.com/2012/04/python-logging-best-practices/
logger = StyleAdapter(logger)


def set_value_to_remote(object, attribute, value):
    target_attr = getattr(object, attribute)
    if object._meta.get_field(attribute).one_to_one:
        if VERSION[0] == 1 and VERSION[1] < 9:
            target_attr = value
        else:
            target_attr.set(value)
    else:
        target_attr.add(value)


class DissimilarActionTypesError(Exception):

    def __init__(self, action_type1, action_type2, field_name, model_name):
        self.action_types = [action_type1, action_type2]
        self.field_name = field_name
        self.model_name = model_name

    def __str__(self):
        return 'Dissimilar action types[{}] for many-to-many field {} on model {}'.format(
            ','.join(self.action_types),
            self.field_name,
            self.model_name)


class UnknownActionType(Exception):

    def __init__(self, action_type, field_name, model_name):
        self.action_type = action_type
        self.field_name = field_name
        self.model_name = model_name

    def __str__(self):
        return 'Unknown action type[{}] for many-to-many field {} on model {}'.format(
            self.action_type,
            self.field_name,
            self.model_name)


class ObjectSelector:
    OPERATORS = set(['|', '&', '~'])

    def __init__(self, match_on, available_fields):
        for field_name in match_on:
            if field_name in self.OPERATORS:
                continue

            if field_name not in available_fields:
                raise ValueError(
                    'field_name({}) must be in fields({})'.format(
                        field_name, available_fields))

        self.match_on = match_on
        self.fields = available_fields

    def __str__(self):
        return str(self.match_on)

    def get_by(self):
        def build_selector(match):
            return Q(**{match: self.fields[match]})

        # if no operators present, then just AND all of the match_ons
        if len(self.OPERATORS.intersection(self.match_on)) == 0:
            match = self.match_on[0]
            q = build_selector(match)
            for match in self.match_on[1:]:
                q = q & build_selector(match)

            return q

        # process post-fix operator string
        stack = []
        for match in self.match_on:
            if match in self.OPERATORS:
                if match is '~':
                    if len(stack) < 1:
                        raise ValueError('Insufficient operands for operator:{}', match)

                    stack.append(~stack.pop())
                    continue

                if len(stack) < 2:
                    raise ValueError('Insufficient operands for operator:{}', match)

                # remove the operands from the stack in reverse order
                # (preserves left-to-right reading)
                operand2 = stack.pop()
                operand1 = stack.pop()

                if match == '|':
                    stack.append(operand1 | operand2)
                elif match == '&':
                    stack.append(operand1 & operand2)
                else:
                    pass
            else:
                stack.append(build_selector(match))

        if len(stack) != 1:
            raise ValueError('Insufficient operators, stack:{}', stack)

        return stack[0]


class ModelAction:
    """
    The base action, which performs makes no modifications to objects.

    This class consolidates the some of the validity checking and the logic
    for finding the target objects.
    """
    REFERRED_TO_DELIMITER = '=>'

    def __init__(self, model, match_on, fields={}, rel_by_external_key=False,
                 rel_by_external_key_excluded=False, external_system=None):
        """
        Create a base action.

        :param model:
        :param match_on:
        :param fields:
        :param rel_by_external_key
        :param rel_by_external_key_excluded
        :param external_system
        :return:
        """
        if model is None:
            raise ValueError('model cannot be None')
        if match_on:
            match_on = ObjectSelector(match_on, fields)

        self.model = model
        self.match_on = match_on
        self.fields = fields
        self.rel_by_external_key = rel_by_external_key
        self.rel_by_external_key_excluded = rel_by_external_key_excluded
        self.external_system = external_system

    def __str__(self):
        return '{} - Model:{} - MatchFields:{} - Fields:{}'.format(
            self.__class__.__name__,
            self.model.__name__,
            self.match_on,
            self.fields)

    @property
    def type(self):
        return ''

    def get_object(self):
        """Finds the object that matches the provided matching information"""
        return self.model.objects.get(self.match_on.get_by())

    def get_object_id_by_external_key(self, external_key, content_type):
        try:
            external_key_mapp = ExternalKeyMapping.objects.get(
                external_system=self.external_system,
                external_key=external_key,
                content_type=content_type
            )
            return external_key_mapp.object_id
        except ExternalKeyMapping.DoesNotExist as e:
            logger.warning('External mapping issue - {} Error: {}. External key {}', str(self), e, external_key)
            raise

    def execute(self, use_bulk=False):
        """Does nothing"""
        pass

    def update_from_fields(self, object, force=False):
        """
        Update the provided object with the fields.

        This is implemented in a consolidated place, as both Create and
        Update style actions require the functionality.

        :param object: the object to update
        :param force (bool): (Optional) Whether the update should only
        affect 'empty' fields. Default: False
        :return:
        """
        # we need to support referential attributes, so look for them
        # as we iterate and store them for later

        # We store the referential attributes as a dict of dicts, this way
        # filtering against many fields is possible
        referential_attributes = defaultdict(dict)
        generic_fks = []
        generic_fk_objs = dict()
        if self.rel_by_external_key:
            # Find all generic foreign key and it's fk field to be able to map these fields.
            generic_fks = [field for field in object._meta.get_fields() if isinstance(field, GenericForeignKey)]
        for attribute, value in self.fields.items():
            if self.REFERRED_TO_DELIMITER in attribute and value != '':
                ref_attr = attribute.split(self.REFERRED_TO_DELIMITER)
                referential_attributes[ref_attr[0]][ref_attr[1]] = value
            else:
                if not force:
                    current_value = getattr(object, attribute, None)
                    if not (current_value is None or current_value is ''):
                        continue
                try:
                    field = object._meta.get_field(attribute)
                    if field.null:
                        value = None if value == '' else value
                    if field.related_model and value and self.is_rel_model_mapped(field.related_model):
                        content_type = ContentType.objects.get_for_model(field.related_model)
                        value = self.get_object_id_by_external_key(value, content_type)
                    if isinstance(field, JSONField) and value:
                        try:
                            value = ast.literal_eval(value)
                        except (ValueError, SyntaxError):
                            pass
                except FieldDoesNotExist:
                    pass

                if self.rel_by_external_key and attribute in [generic_fk.fk_field for generic_fk in generic_fks]:
                    generic_fk_objs[attribute] = value
                setattr(object, attribute, value)

        for attribute, get_by in referential_attributes.items():
            try:
                field = object._meta.get_field(attribute)
                # For migration advice of the get_field_by_name() call see [1]
                # [1]: https://docs.djangoproject.com/en/1.9/ref/models/meta/#migrating-old-meta-api

                if field.related_model:
                    if field.concrete:
                        own_attribute = field.name
                        get_current_value = getattr
                        set_value = setattr
                    else:
                        own_attribute = field.get_accessor_name()

                        def get_value_from_remote(object, attribute, default):
                            try:
                                return getattr(object, attribute).get()
                            except:
                                return default

                        get_current_value = get_value_from_remote
                        set_value = set_value_to_remote

                    if not force:
                        current_value = get_current_value(object, own_attribute, None)
                        if current_value is not None:
                            continue

                    try:
                        if field.many_to_many:
                            action_type = None
                            get_by_exact = {}
                            for k, v in get_by.items():
                                if action_type is None:
                                    action_type = k[0]
                                elif action_type != k[0]:
                                    raise DissimilarActionTypesError(
                                        action_type, k[0], field.verbose_name,
                                        object.__class__.__name__)
                                get_by_exact[k[1:]] = v

                            if action_type not in '+-=':
                                raise UnknownActionType(action_type,
                                                        field.verbose_name,
                                                        object.__class__.__name__)

                            target = field.related_model.objects.get(**get_by_exact)

                            if action_type is '+':
                                getattr(object, own_attribute).add(target)
                            elif action_type is '-':
                                getattr(object, own_attribute).remove(target)
                            elif action_type is '=':
                                attr = getattr(object, own_attribute)
                                # Django 1.9 impl  => getattr(object, own_attribute).set([target])
                                attr.clear()
                                for t in set([target]):
                                    attr.add(t)

                        else:
                            target = field.related_model.objects.get(**get_by)
                            set_value(object, own_attribute, target)

                    except ObjectDoesNotExist as e:
                        logger.warning(
                            'Could not find {} with {} for {}[{}].{}',
                            field.related_model.__name__,
                            get_by,
                            object.__class__.__name__,
                            object,
                            field.verbose_name)
                    except MultipleObjectsReturned as e:
                        logger.warning(
                            'Found multiple {} objects with {} for {}[{}].{}',
                            field.related_model.__name__,
                            get_by,
                            object.__class__.__name__,
                            object,
                            field.verbose_name)
            except FieldDoesNotExist as e:
                logger.warning('Attibute "{}" does not exist on {}[{}]',
                               attribute,
                               object.__class__.__name__,
                               object)

            except DissimilarActionTypesError as e:
                logger.warning('{}', e)

            except UnknownActionType as e:
                logger.warning('{}', e)

        for gfk_obj_attr, value in generic_fk_objs.items():
            # Map object id of generic foreign key
            # First find name of the content type field for generic foreign key
            content_type_field_name = None
            content_type_field_value = None
            for generic_fk in generic_fks:
                if generic_fk.fk_field == gfk_obj_attr:
                    content_type_field_name = generic_fk.ct_field
            # Get value of content type field
            if content_type_field_name:
                content_type_column_name = object._meta.get_field(content_type_field_name).column
                content_type_field_value = getattr(object, content_type_column_name)
            if content_type_field_value:
                # Finally get mapped value of object id
                value = self.get_object_id_by_external_key(value, content_type_field_value)
            else:
                raise ValueError(f'Content type cannot be found for Generic foreign key with object '
                                 f'\'{attribute}\' for model {object._meta.label}')
            setattr(object, gfk_obj_attr, value)

    def is_rel_model_mapped(self, related_model):
        return self.rel_by_external_key and related_model._meta.db_table not in self.rel_by_external_key_excluded


class CreateModelAction(ModelAction):
    """
    Action to create a model object if it does not exist.

    Note, this will not create another object if a matching one is
    found, nor will it update a matched object.
    """

    def __init__(self, *args, **kwargs):
        self.force_init_instace = kwargs.pop('force_init_instance', None)
        self.obj_already_created = False
        super(CreateModelAction, self).__init__(*args, **kwargs)

    def execute(self, use_bulk=False):
        if self.match_on and not self.force_init_instace:
            try:
                obj = self.get_object()
                self.obj_already_created = True
                return obj
            except ObjectDoesNotExist as e:
                pass
            except MultipleObjectsReturned as e:
                logger.warning('Mulitple objects found - {} Error:{}', str(self), e)
                return None

        obj = self.model()
        # NB: Create uses force to override defaults
        try:
            self.update_from_fields(obj, True)
        except ExternalKeyMapping.DoesNotExist:
            return None
        if not use_bulk:
            # Check if there is parents and if they are already created
            if obj._meta.parents and \
                all(getattr(obj, field.attname) is not None for parent, field in obj._meta.parents.items()):
                # This is fix for multi-table inheritance problem and that model cannot be saved without explicitly
                # setting parent data for the given model
                # The way to associate existing parent with child is telling save_base not to save any parent models
                obj.save_base(raw=True)
            else:
                # TODO: depending of configuration, call save of the instance or
                # skip it and call save on the django model
                Model.save(obj)
                # obj.save()
        return obj

    @property
    def type(self):
        return 'create'


class ReferenceActionMixin:

    def __init__(self, *args, **kwargs):
        super(ReferenceActionMixin, self).__init__(*args, **kwargs)
        self.external_mapping_obj = None

    def get_or_init_ext_mapp(self, force_init=False):
        mapping_inst = ExternalKeyMapping(
            external_system=self.external_system,
            external_key=self.external_key,
            content_type=ContentType.objects.get_for_model(self.model)
        )
        if force_init:
            return mapping_inst

        try:
            mapping = ExternalKeyMapping.objects.get(
                external_system=self.external_system,
                external_key=self.external_key,
                content_type=ContentType.objects.get_for_model(self.model),
            )
        except ExternalKeyMapping.DoesNotExist:
            mapping = mapping_inst

        return mapping

    @staticmethod
    def map_to_external_object(external_mapping_obj, model_obj):
        external_mapping_obj.content_object = model_obj
        external_mapping_obj.object_id = model_obj.id


class CreateModelWithReferenceAction(ReferenceActionMixin, CreateModelAction):
    """
    Action to create a model object if it does not exist, and to create or
    update an external reference to the object.
    """

    def __init__(self, external_system, model, external_key, match_on, **kwargs):
        """

        :param external_system (model object): The external system to create or
            update the reference for.
        :param external_key (str): The reference value from the external
            system (i.e. the 'id' that the external system uses to refer to the
            model object).
        :param model (class): See definition on super class
        :param match_on (list): See definition on super class
        """
        super(CreateModelWithReferenceAction, self).__init__(
            model, match_on, external_system=external_system, **kwargs
        )
        self.external_key = external_key

    def execute(self, use_bulk=False):
        self.external_mapping_obj = self.get_or_init_ext_mapp(self.force_init_instace)

        model_obj = self.external_mapping_obj.content_object
        if model_obj is None:
            model_obj = super(CreateModelWithReferenceAction, self).execute(use_bulk=use_bulk)
        if not use_bulk and model_obj and model_obj.pk and model_obj.pk != self.external_mapping_obj.object_id:
            self.map_to_external_object(self.external_mapping_obj, model_obj)
            self.external_mapping_obj.save()
        return model_obj


from django.db import IntegrityError, transaction


class UpdateModelAction(ModelAction):
    """
    Action to update the fields of a model object, but not create an
    object.
    """

    def __init__(self, model, match_on, force_update=False, **kwargs):
        """
        Create an Update action to be executed in the future.

        :param model (class): The model to update against
        :param match_on (list): A list of names of model attributes/fields
            to use to find the object to update. They must be a key in the
            provided fields.
        :param fields(dict): The set of fields to update, with the values to
            update them to.
        :param force_update(bool): (Optional) Whether the update should be
            forced or only affect 'empty' fields. Default:False
        :return: The updated object (if a matching object is found) or None.
        """
        super(UpdateModelAction, self).__init__(model, match_on, **kwargs)
        self.force_update = force_update

    @property
    def type(self):
        return 'update'

    def execute(self, use_bulk=False):
        try:
            obj = self.get_object()
            self.update_from_fields(obj, self.force_update)

            if not use_bulk:
                with transaction.atomic():
                    obj.save()

            return obj
        except ObjectDoesNotExist:
            return None
        except ExternalKeyMapping.DoesNotExist as e:
            return None
        except MultipleObjectsReturned as e:
            logger.warning('Mulitple objects found - {} Error:{}', str(self), e)
            return None
        except IntegrityError as e:
            logger.warning('Integrity issue - {} Error:{}', str(self), e)
            return None


class UpdateModelWithReferenceAction(ReferenceActionMixin, UpdateModelAction):
    """
    Action to create a model object if it does not exist, and to create or
    update an external reference to the object.
    """

    def __init__(self, external_system, model, external_key, match_on, **kwargs):
        """

        :param external_system (model object): The external system to create or
            update the reference for.
        :param external_key (str): The reference value from the external
            system (i.e. the 'id' that the external system uses to refer to the
            model object).

        :param model (class): See definition on super class
        :param match_on (list): See definition on super class
        """
        super(UpdateModelWithReferenceAction, self).__init__(
            model, match_on, external_system=external_system, **kwargs)
        self.external_key = external_key

    def execute(self, use_bulk=False):
        self.external_mapping_obj = self.get_or_init_ext_mapp()

        linked_object = self.external_mapping_obj.content_object

        matched_object = None
        try:
            matched_object = self.get_object()
        except ObjectDoesNotExist:
            pass
        except MultipleObjectsReturned as e:
            logger.warning('Mulitple objects found - {} Error:{}', str(self), e)
            return None

        # If both matched and linked objects exist but are different,
        # get rid of the matched one
        if matched_object and linked_object and (matched_object !=
                                                 linked_object):
            matched_object.delete()

        # Choose the most appropriate object to update
        if linked_object:
            model_obj = linked_object
        elif matched_object:
            model_obj = matched_object
        else:
            # No object to update
            return None

        if model_obj:
            try:
                self.update_from_fields(model_obj, self.force_update)
            except ExternalKeyMapping.DoesNotExist:
                return None
            if not use_bulk:
                try:
                    with transaction.atomic():
                        model_obj.save()
                except IntegrityError as e:
                    logger.warning('Integrity issue - {} Error:{}', str(self), e)
                    return None

        if model_obj and not use_bulk and model_obj.pk != self.external_mapping_obj.object_id:
            self.map_to_external_object(self.external_mapping_obj, model_obj)
            self.external_mapping_obj.save()
        return model_obj


class DeleteIfOnlyReferenceModelAction(ModelAction):
    """
    This action only deletes the pointed to object if the key mapping
    corresponding to 'this' external key it the only one

    I.e. if there are two references from different external systems to the
    same object, then the object will not be deleted.
    """

    def __init__(self, external_system, external_key, delete_action):
        self.delete_action = delete_action
        self.external_key = external_key
        self.external_system = external_system

    @property
    def type(self):
        return self.delete_action.type

    def execute(self, use_bulk=False):
        try:
            if self.delete_action.match_on:
                obj = self.delete_action.get_object()
                # Note: Check if obj will be deleted if there is another external system with ref on that object
                key_mapping = ExternalKeyMapping.objects.get(
                    object_id=obj.pk,
                    content_type=ContentType.objects.get_for_model(
                        self.delete_action.model),
                    external_key=self.external_key)
                if key_mapping.external_system == self.external_system:
                    return self.delete_action.execute(obj=obj, use_bulk=use_bulk)
                else:
                    # The key mapping is not 'this' systems key mapping
                    pass
            else:
                # Find object by external key and external system
                key_mapping = ExternalKeyMapping.objects.get(
                    content_type=ContentType.objects.get_for_model(
                        self.delete_action.model),
                    external_key=self.external_key,
                    external_system=self.external_system
                )
                obj = key_mapping.content_object
                if obj:
                    return self.delete_action.execute(obj=obj, use_bulk=use_bulk)

        except MultipleObjectsReturned:
            # There are multiple key mappings or multiple target objects, we shouldn't delete the object
            return
        except ObjectDoesNotExist:
            return


class DeleteModelAction(ModelAction):

    @property
    def type(self):
        return 'delete'

    def execute(self, obj=None, use_bulk=False):
        """Forcibly delete any objects found by the
        ModelAction.get_object() method."""
        try:
            obj = obj or self.get_object()
            if not use_bulk:
                # TODO: depending of configuration, call delete of the instance or skip it
                # and call delete of the django model
                Model.delete(obj)
                # inst.delete()
                # obj.delete()
            return obj
        except ObjectDoesNotExist:
            pass
        except MultipleObjectsReturned as e:
            logger.warning('Mulitple objects found - {} Error:{}', str(self), e)
            return None


class DeleteExternalReferenceAction:
    """
    A model action to remove the ExternalKeyMapping object for a model object.
    """

    def __init__(self, model, external_system, external_key):
        self.model = model
        self.external_system = external_system
        self.external_key = external_key

    @property
    def type(self):
        return 'delete'

    def execute(self, use_bulk=False):
        """
        Deletes all ExternalKeyMapping objects that match the provided external
        system and external key.
        :return: Nothing
        """
        ExternalKeyMapping.objects.filter(
            external_system=self.external_system,
            external_key=self.external_key,
            content_type=ContentType.objects.get_for_model(self.model)).delete()


class ActionFactory:
    """
    A factory for producing the most appropriate (set of) ModelAction objects.

    The factory takes care of creating the correct actions in the instances
    where it is a little complicated. In particular, when there are unforced
    delete actions.

    In the case of unforced delete actions, the builder will create a
    DeleteIfOnlyReferenceModelAction. This action will only delete the
    underlying model if there is a single link to the object to be deleted
    AND it is a link from the same system.

     Example 1:

       1. Starting State
       -----------------
       ExtSys 1 - Mapping 1 (Id: 123) --+
                                        |
                                        v
                            Model Object (Person: John)
                                        ^
                                        |
       ExtSys 2 - Mapping 1 (Id: AABB) -+


       2. DeleteIfOnlyReferenceModelAction(ExtSys 2, AABB, DeleteAction(John))
       -----------------------------------------------------------------------
        Although there was a 'delete John' action, it was not performed
        because there is another system with a link to John.

     Example 2:

       1. Starting State
       -----------------
       ExtSys 1 - Mapping 1 (Id: 123) --+
                                        |
                                        v
                            Model Object (Person: John)

       2. DeleteIfOnlyReferenceModelAction(ExtSys 2, AABB, DeleteAction(John))
       -----------------------------------------------------------------------
        Although there was only a single reference, it is not for ExtSys 2,
        hence the delete is not performed.

    The builder will also include a DeleteExternalReferenceAction if the
    provided action is 'externally mappable'. These will always be executed
    and will ensure that the reference objects will be removed by their
    respective sync systems (and that if they all work correctly the last
    one will be able to delete the object).
    """

    def __init__(self, model, external_system=None, rel_by_external_key=False,
                 rel_by_external_key_excluded=False, force_init_instance=False):
        """
        Create an actions factory for a given Django Model.

        :param model: The model to use for the actions
        :param external_system: (Optional) The external system object to
            create links against
        :param rel_by_external_key: (Optional) If related field is referenced by external key
        :param rel_by_external_key_excluded: (Optional) Relations that are excpeted from rel_by_external_key
        :param force_init_instance: (Optional) Whether to prevent from checking the database for existing instances
        :return: A new actions factory
        """
        self.model = model
        self.external_system = external_system
        self.rel_by_external_key = rel_by_external_key
        self.rel_by_external_key_excluded = rel_by_external_key_excluded
        self.force_init_instance = force_init_instance

    def is_externally_mappable(self, external_key):
        """
        Check if the an 'external system mapping' could be created for the
        provided key.
        :param external_key:
        :return:
        """
        if self.external_system is None:
            return False

        if external_key is None:
            return False

        if not isinstance(external_key, str):
            return False

        return external_key.strip() is not ''

    def build(self, sync_actions, match_on, external_system_key,
              fields):
        """
        Builds the list of actions to satisfy the provided information.

        This includes correctly building any actions required to keep the
        external system references correctly up to date.

        :param sync_actions:
        :param match_on:
        :param external_system_key:
        :param fields:
        :return:
        """
        actions = []
        action_kwargs = dict(
            fields=fields,
            rel_by_external_key=self.rel_by_external_key,
            rel_by_external_key_excluded = self.rel_by_external_key_excluded
        )
        if sync_actions.is_impotent():
            actions.append(ModelAction(self.model, match_on, **action_kwargs))

        if sync_actions.delete:
            action = DeleteModelAction(self.model, match_on, fields=fields)
            if self.is_externally_mappable(external_system_key):
                if not sync_actions.force:
                    action = DeleteIfOnlyReferenceModelAction(
                        self.external_system, external_system_key, action)
                actions.append(action)
                actions.append(DeleteExternalReferenceAction(
                    self.model, self.external_system, external_system_key))
            elif sync_actions.force:
                actions.append(action)

        if sync_actions.create:
            if self.is_externally_mappable(external_system_key):
                action = CreateModelWithReferenceAction(
                    self.external_system,
                    self.model,
                    external_system_key,
                    match_on,
                    force_init_instance=self.force_init_instance, **action_kwargs
                )
            else:
                action = CreateModelAction(self.model, match_on,
                                           force_init_instance=self.force_init_instance,
                                           external_system=self.external_system, **action_kwargs)
            actions.append(action)
        if sync_actions.update:
            if self.is_externally_mappable(external_system_key):
                action = UpdateModelWithReferenceAction(
                    self.external_system,
                    self.model,
                    external_system_key,
                    match_on,
                    force_update=sync_actions.force,
                    **action_kwargs)
            else:
                action = UpdateModelAction(self.model, match_on,
                                           force_update=sync_actions.force,
                                           external_system=self.external_system,
                                           **action_kwargs
                                           )

            actions.append(action)

        return actions


class SyncActions:
    """
    A holder object for the actions that can be requested against a model
    object concurrently.
    """

    def __init__(self, create=False, update=False, delete=False, force=False):
        if delete and create:
            raise ValueError("Cannot delete AND create")
        if delete and update:
            raise ValueError("Cannot delete AND update")

        self.create = create
        self.update = update
        self.delete = delete
        self.force = force

    def __str__(self):
        return "SyncActions {}{}{}{}".format(
            'c' if self.create else '',
            'u' if self.update else '',
            'd' if self.delete else '',
            '*' if self.force else '')

    def is_impotent(self):
        return not (self.create or self.update or self.delete)

