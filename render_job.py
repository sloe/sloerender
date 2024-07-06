import datetime
import logging
import os
import re
import time

import psutil
from pydantic import BaseModel

import item_params
import output_params
import process_wrapper
import render_params
from trackers import Trackers

LOGGER = logging.getLogger('render_job')
logging.basicConfig(level=logging.DEBUG)


class RenderJobParams(BaseModel):
    item: item_params.ItemParams
    output: output_params.OutputParams
    render: render_params.RenderParams


class RenderJob:
    def __init__(self, path_maker, job_name, params):
        self.path_maker = path_maker
        self.job_name = job_name
        self.params = params

        self.ae_child_pids = None
        self.start_time = None
        self.last_frame_time = None
        self.frame_interval_moving_average = 0.0

    def do_aerender(self):
        aerender_path = os.path.join(
            self.params.render.ae.aerender_path.replace('{ae.major_version}', self.params.render.ae.major_version),
            'aerender.exe')

        prores_path = self.path_maker.prores_path(self.params.item.item_name, mkdir=True)
        LOGGER.info(self.params.item.ae_project)
        aerender_command = [
            aerender_path,
            '-v', 'ERRORS_AND_PROGRESS',
            '-project', self.path_maker.item_ae_project_path(self.params.item.ae_project),
            '-comp', self.params.item.ae_comp,
            '-output', prores_path,  # self.params.output.destination_path,
            '-RStemplate', self.params.render.ae.render_settings_template,
            '-OMtemplate', self.params.render.ae.output_module_template
        ]

        if self.params.render.ae.mfr:
            aerender_command += [
                '-mfr', 'ON', str(self.params.render.ae.mfr_max_cpu_percent)
            ]

        if self.params.render.ae.play_sound:
            aerender_command += [
                '-sound', 'ON'
            ]

        aerender = process_wrapper.ProcessWrapper(aerender_command)
        self.ae_child_pids = []
        aerender.run()
        self.start_time = time.monotonic()
        try:
            while aerender.is_alive():
                self.service_job(aerender)
                time.sleep(0.5)

        except (Exception, KeyboardInterrupt) as exp:
            aerender.kill(extra_pids=self.ae_child_pids)
            raise

        self.service_job(aerender)

        rc = aerender.get_return_code()
        if rc == 0:
            LOGGER.info(f"Successful: {self.job_name}")
        else:
            LOGGER.error(f"+++RETURN CODE {rc}: {self.job_name}")

    def handle_new_frame(self, seconds, time_str, frame_num):
        if self.last_frame_time is not None:
            elapsed_str = str(datetime.timedelta(seconds=int(time.monotonic() - self.start_time)))
            frame_interval = seconds - self.last_frame_time
            self.frame_interval_moving_average = 0.9 * self.frame_interval_moving_average + 0.1 * frame_interval
            Trackers.report_scalar("Render performance", "After Effects frame render time", frame_interval,
                                   frame_num)
            Trackers.report_scalar("Render performance", "After Effects frame render time moving average",
                                   self.frame_interval_moving_average, frame_num)
            if (frame_num % 100) == 0 or frame_num == 10:
                LOGGER.info("Elapsed %s frame %d avg: %.3fs/frame: %s",
                            elapsed_str,
                            frame_num,
                            self.frame_interval_moving_average,
                            self.job_name)
                for ae_pid in self.ae_child_pids:
                    try:
                        process = psutil.Process(ae_pid)
                        cpu_times = process.cpu_times()
                        cpu_user_str = str(datetime.timedelta(seconds=int(cpu_times.user)))
                        cpu_system_str = str(datetime.timedelta(seconds=int(cpu_times.system)))
                        LOGGER.info("CPU times of After Effects process with pid %s: user=%s, system=%s", ae_pid,
                                    cpu_user_str, cpu_system_str)
                    except (psutil.NoSuchProcess, ProcessLookupError):
                        LOGGER.info("After Effects process with PID %s no longer active", ae_pid)

        self.last_frame_time = seconds

    def service_job(self, aerender):

        while not aerender.output_queue.empty():
            stream_label, seconds, line = aerender.output_queue.get()
            if stream_label == 'EXC':
                raise line
            if stream_label == 'OUT':
                match = re.match(r'PROGRESS:\s+([0-9:.])+\s+\((\d+)\)', line)
                if match:
                    self.handle_new_frame(seconds=seconds, time_str=match.group(1), frame_num=int(match.group(2)))
                else:
                    LOGGER.info(line)
            else:
                LOGGER.info(f"{stream_label}: {line}")

        ae_pids = aerender.capture_child_pids(r'After\s*(Effects|FX)')
        for ae_pid in [x for x in ae_pids if x not in self.ae_child_pids]:
            LOGGER.info("Captured new After Effects process with PID %d", ae_pid)
            self.ae_child_pids.append(ae_pid)

    def execute(self):
        self.do_aerender()
