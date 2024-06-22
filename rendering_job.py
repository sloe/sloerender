import logging
import os

import process_wrapper

LOGGER = logging.getLogger('cascading_params')
logging.basicConfig(level=logging.DEBUG)


class RenderingJob:
    def __init__(self, job_name, params):
        self.job_name = job_name
        self.params = params

    def do_aerender(self):
        aerender_path = os.path.join(
            self.params.ae_aerender_path.replace('{ae_major_version}', self.params.ae_major_version),
            'aerender.exe')
        LOGGER.info(self.params.ae_project)
        aerender_command = [
            aerender_path,
            '-verbose',
            '-project', self.params.ae_project,
            '-comp', self.params.ae_comp,
            '-output', self.params.ae_output,
            '-RStemplate', self.params.ae_render_settings_template,
            '-OMtemplate', self.params.ae_output_module_template
        ]

        if self.params.ae_mfr:
            aerender_command += [
                '-mfr', 'ON', str(self.params.ae_mfr_max_cpu_percent)
            ]

        if self.params.ae_play_sound:
            aerender_command += [
                '-sound', 'ON'
            ]

        aerender = process_wrapper.ProcessWrapper(aerender_command)
        aerender.run()

    def execute(self):
        self.do_aerender()
