import argparse
import logging

import cascading_params
import item_params
import rendering_job

LOGGER = logging.getLogger('render')
logging.basicConfig(level=logging.DEBUG)


class Render:
    def __init__(self):
        self.render_params_base = cascading_params.CascadingParams("render_params_base")
        self.options = None

    def do_render(self, item_filepath):
        render_params = cascading_params.CascadingParams("render_job")
        render_params.add_to_end('render_params_base', self.render_params_base)
        item_dict = render_params.load_json5_params('item', item_filepath)
        item = item_params.ItemParams(**item_dict)
        render_params.add_to_end('item', item)
        render_job = rendering_job.RenderingJob(job_name=item.name, params=render_params)
        render_job.execute()

    def do_work(self):
        LOGGER.info("Beginning render")
        for item in self.options.items:
            self.do_render(item)

    def parse_args(self):
        parser = argparse.ArgumentParser(description='Slow Motion Rowing renderer.')

        parser.add_argument('items',
                            nargs='+',
                            help='One or more names of item definition files')
        parser.add_argument('--render-params-base',
                            default='render_params_base.json5',
                            help='parameter file (json5) for rendering')
        parser.add_argument('--debug',
                            action='store_true',
                            help='include to set logging to debug')

        self.options = parser.parse_args()

        # Set logging level
        if self.options.debug:
            LOGGER.setLevel(logging.DEBUG)

        self.render_params_base.add_json5_to_end('render_params_base', self.options.render_params_base)


if __name__ == '__main__':
    LOGGER.setLevel(logging.INFO)
    app = Render()
    app.parse_args()
    app.do_work()
