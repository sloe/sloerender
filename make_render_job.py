# python 3.12
import argparse
import logging
from datetime import datetime
from pathlib import Path

import json5

import item_params

LOGGER = logging.getLogger('make_render_job')
logging.basicConfig(level=logging.INFO)
USAGE_DESCRIPTION = '''
This script is used to create a JSON5 file from command line parameters.
Example of usage:
python your_script.py --ae_project /path/to/project.aep --ae_comp "comp name" --name "My item"
'''


class CommandParamsHandler:
    def __init__(self):
        self.parser = self.initialize_parser()
        LOGGER.debug('Argument parser initialized')

    @staticmethod
    def initialize_parser():
        parser = argparse.ArgumentParser(description=USAGE_DESCRIPTION)
        parser.add_argument('--ae_comp', required=True, type=str,
                            help='Name of a composition in the After Effects project')
        parser.add_argument('--ae_project', required=True, type=str,
                            help='Filepath to an After Effects project')
        parser.add_argument('--name', required=False, type=str,
                            help='Name of the item')
        parser.add_argument('--output-json5', required=False, type=str,
                            help='Name of the output json5 file', default=None)
        parser.add_argument('--creation-timestamp', required=False, type=str,
                            help='Time of item file creation')
        parser.add_argument('--ae-project-timestamp', required=False, type=str,
                            help='Time of After Effects project modification')
        return parser

    def generate_and_write_item_params(self):
        options = self.parser.parse_args()
        LOGGER.debug(f'Arguments parsed: {options}')
        if not options.ae_project_timestamp:
            options.ae_project_timestamp = datetime.fromtimestamp(Path(options.ae_project).stat().st_mtime).isoformat()
        if not options.creation_timestamp:
            options.creation_timestamp = datetime.now().isoformat()
        if not options.name:
            options.name = options.ae_comp
        if not options.output_json5:
            options.output_json5 = f"{options.name}-render.json5"
            LOGGER.info(f'--output-json5 is not passed, setting it to {options.output_json5} based on name')

        item = item_params.ItemParams(
            ae_project=options.ae_project,
            ae_comp=options.ae_comp,
            name=options.name,
            creation_timestamp=options.creation_timestamp,
            ae_project_timestamp=options.ae_project_timestamp
        )

        self.write_item_params_to_file(options.output_json5, item)

    def write_item_params_to_file(self, filepath, item):
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(json5.dumps(item.dict(), indent=2))
        LOGGER.info(f'Render item details written to the file: {filepath}')


if __name__ == '__main__':
    handler = CommandParamsHandler()
    handler.generate_and_write_item_params()
