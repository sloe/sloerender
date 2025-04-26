import argparse
import logging
import os.path
import re
import socket
import traceback

import wakepy

import division_order
import file_scanner
import item_params
import output_params
import path_maker
import render_job
import render_params
from trackers import Trackers

LOGGER = logging.getLogger('render')
LOGGER.setLevel(level=logging.DEBUG)
logging.getLogger("wakepy").setLevel(logging.INFO)


class Render:
    def __init__(self):
        self.options = None

    def do_work(self):

        order = division_order.DivisionOrder(self.path_maker.order_path())

        filtered_order = order.filter(self.options.include)
        if self.options.reverse:
            filtered_order.reverse()

        LOGGER.info("Will potentially render:\n\n  %s\n\nStopping after: %s",
                    "\n  ".join(x['name'] for x in filtered_order),
                    self.options.stop_after
                    )

        for order_num, order_item in enumerate(filtered_order):
            if self.options.stop_after:
                if re.match(r'^[0-9]+$', self.options.stop_after):
                    if order_num >= int(self.options.stop_after):
                        LOGGER.info("Stopping after %d items due to --stop-after %s", order_num,
                                    self.options.stop_after)
                        break

            item_params_path = self.path_maker.item_path(order_item['name'])
            item_p = item_params.ItemParams.from_json5(item_params_path)
            output_p = output_params.OutputParams(destination_path=self.path_maker.final_path(order_item['name']))
            render_p = render_params.RenderParams.from_json5(self.options.render_params_base)
            render_job_p = render_job.RenderJobParams(item=item_p, output=output_p, render=render_p)

            final_path = self.path_maker.final_path(item_p.item_name, mkdir=True)
            final_scan = file_scanner.FileScanner(final_path, f"Output file prescan {item_p.item_name}",
                                                  render_job_p)
            final_scan_result = final_scan.scan_video()

            if final_scan_result['valid'] and not self.options.force_final and not self.options.force_prores:
                LOGGER.info("Skipping %s: %s", item_p.item_name, final_scan_result['message'])
            else:
                LOGGER.info("Contacting trackers...")
                clearml_task = Trackers.clearml_task_init(
                    auto_resource_monitoring=dict(report_frequency_sec=5.0),
                    enabled=self.path_maker.env['clearml_enabled'],
                    project_name=self.path_maker.env['project_prefix'] + self.path_maker.project_name(),
                    reuse_trackers=self.options.reuse_trackers,
                    task_name=f"{self.path_maker.env['run_prefix']}{item_p.item_name}",
                )
                mlflow_task = Trackers.mlflow_task_init(
                    enabled=self.path_maker.env['mlflow_enabled'],
                    project_name=self.path_maker.env['project_prefix'] + self.path_maker.project_name(),
                    reuse_trackers=self.options.reuse_trackers,
                    task_name=f"{self.path_maker.env['run_prefix']}{item_p.item_name}",
                )
                try:
                    clearml_task.connect(render_job_p.dict())
                    mlflow_task.connect(render_job_p.dict())
                    LOGGER.info(f"Starting job: Render {item_p.item_name}")
                    job = render_job.RenderJob(self.path_maker, f"{item_p.item_name}", render_job_p)
                    prores_scan_result, final_scan_result = job.execute(force_final=self.options.force_final,
                                                                        force_prores=self.options.force_prores)
                    if prores_scan_result:
                        clearml_task.connect(prores_scan_result, name="ProRes file")
                        mlflow_task.connect(prores_scan_result, name="ProRes file")
                    if final_scan_result:
                        clearml_task.connect(final_scan_result, name="Output file")
                        mlflow_task.connect(final_scan_result, name="Output file")
                except (Exception, KeyboardInterrupt) as exc:
                    clearml_task.mark_failed(status_message="".join(traceback.format_exception_only(exc)).strip(),
                                             force=True)
                    mlflow_task.mark_failed(status_message="".join(traceback.format_exception_only(exc)).strip(),
                                            force=True)
                    raise
                finally:
                    clearml_task.close()
                    mlflow_task.close()

            if self.options.stop_after:
                if re.search(self.options.stop_after, order_item['name']):
                    LOGGER.info("Stopping after %d items due to --stop-after %s matching %s", order_num,
                                self.options.stop_after, order_item['name'])
                    break

    def prepare_env(self):
        env_filepath = f"{socket.gethostname()}-{self.options.env_filepath}"
        if not os.path.isfile(env_filepath):
            env_filepath = self.options.env_filepath
        self.path_maker = path_maker.PathMaker(env_filepath)
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
        parser.add_argument('--force-final',
                            action='store_true',
                            help='Force generation of the final file even if it is already present')
        parser.add_argument('--force-prores',
                            action='store_true',
                            help='Force generation of the prores file even if it is already present')
        parser.add_argument('--include',
                            default='.*',
                            help='Filter regexp to select the names of items to be rendered')
        parser.add_argument('--render-params-base',
                            default='render_params_base.json5',
                            help='Parameter file (json5) for rendering')
        parser.add_argument('--reuse-trackers',
                            action='store_true',
                            help='Reuse tracker IDs to prevent creation of new experiments')
        parser.add_argument('--reverse',
                            action='store_true',
                            help='Process in reverse order')
        parser.add_argument('--stop-after',
                            help='Stop processing after this many (number) or the next name matches (regexp)')
        parser.add_argument('--variant',
                            default=None,
                            help='Variant name, e.g. nextgen')

        self.options = parser.parse_args()

        # Set logging level
        if self.options.debug:
            logging.setLevel(logging.DEBUG)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(name)s:%(levelname)s:%(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)
    with wakepy.keep.running(on_fail="warn"):
        app = Render()
        app.parse_args()
        app.prepare_env()
        app.do_work()
