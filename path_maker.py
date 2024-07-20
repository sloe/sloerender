import logging
import os

import json5

LOGGER = logging.getLogger('path_maker')
LOGGER.setLevel(level=logging.DEBUG)


class PathMaker:
    def __init__(self, env_filepath):
        with open(env_filepath, "r", encoding='utf-8') as file:
            self.env = json5.load(file)
        LOGGER.info(f"Loaded {len(self.env)} environment values from {env_filepath}")

    def get_event(self, event_name=None):
        if event_name:
            return event_name
        return self.env['event']

    def get_division(self, division_name=None):
        if division_name:
            return division_name
        return self.env['division']

    def get_variant(self, variant_name=None):
        if variant_name:
            return variant_name
        return self.env['variant']

    def set_default_event(self, event_name):
        self.env['event'] = event_name

    def set_default_division(self, division_name):
        self.env['division'] = division_name

    def set_default_variant(self, variant_name):
        self.env['variant'] = variant_name

    def project_name(self):
        project_elements = [
            self.get_event(),
            self.get_division()
        ]
        if self.get_variant():
            project_elements.append(self.get_variant())

        return "-".join(project_elements)

    def defs_name(self, event_name=None, division_name=None, variant_name=None):
        defs_elements = [
            'defs',
            self.get_event(event_name),
            self.get_division(division_name)
        ]
        if self.get_variant(variant_name):
            defs_elements.append(self.get_variant(variant_name))

        return "-".join(defs_elements)

    def formatters_defs_dir(self, event_name=None, division_name=None, variant_name=None):
        defs_name = self.defs_name(event_name=event_name, division_name=division_name, variant_name=variant_name)
        return os.path.join(self.env['smr_root'], 'formatters', self.get_event(event_name), defs_name)

    def final_dir(self, event_name=None, division_name=None, variant_name=None):
        return os.path.join(self.env['smr_root'], 'final', self.get_event(event_name), self.get_division(division_name))

    def smr_scratch_prores_dir(self, event_name=None, division_name=None, variant_name=None):
        return os.path.join(self.env['smr_scratch_prores'], self.get_event(event_name),
                            self.get_division(division_name))

    def order_path(self, event_name=None, division_name=None, variant_name=None):
        return os.path.join(self.formatters_defs_dir(event_name, division_name, variant_name), 'order.json5')

    def item_path(self, name, event_name=None, division_name=None, variant_name=None):
        return os.path.join(self.formatters_defs_dir(event_name, division_name, variant_name), f'{name}.json5')

    def item_ae_project_path(self, item_ae_project_path):
        return os.path.join(self.env['smr_root'], item_ae_project_path)

    def final_path(self, name, event_name=None, division_name=None, variant_name=None, mkdir=False):
        variant = self.get_variant(variant_name)
        if self.env['variant_has_suffix'] and variant:
            final_path = os.path.join(self.final_dir(event_name, division_name), f'{name}-{variant}.mp4')
        else:
            final_path = os.path.join(self.final_dir(event_name, division_name), f'{name}.mp4')

        if mkdir:
            os.makedirs(os.path.dirname(final_path), exist_ok=True)

        return final_path

    def prores_path(self, name, event_name=None, division_name=None, variant_name=None, mkdir=False):
        variant = self.get_variant(variant_name)
        if variant:
            prores_path = os.path.join(self.smr_scratch_prores_dir(event_name, division_name),
                                       f'{name}-{variant} prores.mov')
        else:
            prores_path = os.path.join(self.smr_scratch_prores_dir(event_name, division_name), f'{name} prores.mov')

        if mkdir:
            os.makedirs(os.path.dirname(prores_path), exist_ok=True)

        return prores_path
