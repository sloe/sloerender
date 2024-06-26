import argparse
import logging

import division_order
import item_params
import output_params
import path_maker
import render_job
import render_params
from trackers import Trackers

LOGGER = logging.getLogger('render')
logging.basicConfig(level=logging.DEBUG)


class Render:
    def __init__(self):
        self.options = None

    def do_work(self):
        LOGGER.info("Beginning render")
        order = division_order.DivisionOrder(self.path_maker.order_path())

        filtered_order = order.filter(self.options.include)
        for order_item in filtered_order:
            item_params_path = self.path_maker.item_path(order_item['name'])
            item_p = item_params.ItemParams.from_json5(item_params_path)
            output_p = output_params.OutputParams(destination_path=self.path_maker.final_path(order_item['name']))
            render_p = render_params.RenderParams.from_json5(self.options.render_params_base)
            render_job_p = render_job.RenderJobParams(item=item_p, output=output_p, render=render_p)

            LOGGER.info("Contacting trackers...")
            clearml_task = Trackers.clearml_task_init(
                project_name=self.path_maker.project_name(),
                task_name=f"Render {item_p.item_name}"
            )
            try:
                clearml_task.connect(render_job_p.dict())

                LOGGER.info(f"Starting job: Render {item_p.item_name}")
                job = render_job.RenderJob(self.path_maker, f"Render {item_p.item_name}", render_job_p)
                job.execute()
            finally:
                clearml_task.close()

    def prepare_env(self):
        self.path_maker = path_maker.PathMaker(self.options.env_filepath)
        if self.options.event:
            self.path_maker.set_default_event(self.options.event)
        if self.options.division:
            self.path_maker.set_default_division(self.options.division)
        if self.options.variant:
            self.path_maker.set_default_variant(self.options.variant)

        if self.path_maker.env['clearml_uri']:
            Trackers.init_clearml(self.path_maker.env['clearml_uri'])
        if self.path_maker.env['mlflow_uri']:
            Trackers.init_mlflow(self.path_maker.env['mlflow_uri'])

    def parse_args(self):
        parser = argparse.ArgumentParser(description='Slow Motion Rowing renderer.')

        parser.add_argument('--debug',
                            action='store_true',
                            help='include to set logging to debug')
        parser.add_argument('--division',
                            default=None,
                            help='Division name, e.g. divm1')

        parser.add_argument('--env-filepath',
                            default='sloerender_env.json5',
                            help='Path to the environment file (see sloerender_env_sample.json5')

        parser.add_argument('--event',
                            default=None,
                            help='Event name, e.g. mays2024')
        parser.add_argument('--include',
                            default='.*',
                            help='Filter regexp to select the names of items to be rendered')
        parser.add_argument('--render-params-base',
                            default='render_params_base.json5',
                            help='Parameter file (json5) for rendering')
        parser.add_argument('--variant',
                            default=None,
                            help='Variant name, e.g. nextgen')

        self.options = parser.parse_args()

        # Set logging level
        if self.options.debug:
            LOGGER.setLevel(logging.DEBUG)


if __name__ == '__main__':
    LOGGER.setLevel(logging.INFO)
    app = Render()
    app.parse_args()
    app.prepare_env()
    app.do_work()
