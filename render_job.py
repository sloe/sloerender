import datetime
import json
import logging
import os
import re
import time
from collections import defaultdict

import psutil
from pydantic import BaseModel

import file_scanner
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
        self.final_scan_result = None
        self.frame_interval_moving_average = 0.0
        self.hb_iteration = None
        self.json_capture_on = None
        self.json_data = None
        self.json_discard_on = None
        self.last_frame_time = None
        self.prores_scan_result = None
        self.start_time = None

    def do_aerender(self):
        aerender_dir = os.path.join(
            self.params.render.ae.aerender_dir.replace('{ae.major_version}', self.params.render.ae.major_version),
            'aerender.exe')

        prores_path = self.path_maker.prores_path(self.params.item.item_name, mkdir=True)
        project_path = self.path_maker.item_ae_project_path(self.params.item.ae_project)

        LOGGER.info(f"Launching aerender.exe to render %s from project %s comp %s",
                    prores_path, project_path, self.params.item.ae_comp)
        aerender_command = [
            aerender_dir,
            '-v', 'ERRORS_AND_PROGRESS',
            '-project', project_path,
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
                self.service_aerender_job(aerender)
                time.sleep(0.5)

        except (Exception, KeyboardInterrupt) as exp:
            aerender.kill(extra_pids=self.ae_child_pids)
            raise

        self.service_aerender_job(aerender)

        rc = aerender.get_return_code()
        if rc == 0:
            LOGGER.info(f"Successful: {self.job_name}")
        else:
            LOGGER.error(f"+++RETURN CODE {rc}: {self.job_name}")
            raise Exception(f"AE render process exited with rc={rc}")

    def handle_new_frame(self, seconds, time_str, frame_num):
        if self.last_frame_time is not None:
            elapsed_str = str(datetime.timedelta(seconds=int(time.monotonic() - self.start_time)))
            frame_interval = seconds - self.last_frame_time
            self.frame_interval_moving_average = 0.9 * self.frame_interval_moving_average + 0.1 * frame_interval
            Trackers.report_scalar("Render performance", "After Effects seconds per frame", frame_interval,
                                   frame_num)
            Trackers.report_scalar("Moving average render performance",
                                   "After Effects seconds per frame moving average",
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

    def service_aerender_job(self, aerender):

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

    def do_hbrender(self):

        prores_path = self.path_maker.prores_path(self.params.item.item_name)
        final_path = self.path_maker.final_path(self.params.item.item_name)
        LOGGER.info(f"Launching HandBrakeCLI to render %s from %s", final_path, prores_path)

        hb_path = os.path.join(self.params.render.hb.hb_dir, 'HandBrakeCLI.exe')

        hb_scan_command = [
            hb_path,
            '--ab', str(self.params.render.hb.audio_bitrate),
            '--enable-hw-decoding', 'nvdec',
            '--encoder', str(self.params.render.hb.encoder),
            '--encoder-preset', str(self.params.render.hb.encoder_preset),
            '--input', prores_path,
            '--json',
            '--optimize',
            '--output', final_path,
            '--turbo',
            '--vb', str(self.params.render.hb.bitrate),
        ]

        handbrakecli = process_wrapper.ProcessWrapper(hb_scan_command)
        self.json_capture_on = False
        self.json_discard_on = False
        self.hb_iteration = 0
        handbrakecli.run()
        self.start_time = time.monotonic()
        try:
            while handbrakecli.is_alive():
                self.service_handbrakecli_job(handbrakecli)
                time.sleep(0.5)

        except (Exception, KeyboardInterrupt) as exp:
            handbrakecli.kill()
            raise

        self.service_handbrakecli_job(handbrakecli)

        rc = handbrakecli.get_return_code()
        if rc == 0:
            LOGGER.info(f"Successful: {self.job_name}")

        else:
            LOGGER.error(f"+++RETURN CODE {rc}: {self.job_name}")
            raise Exception(f"HandBrakeCLI process exited with rc={rc}")

    def handle_handbrakecli_progress(self, progress):
        working = progress.get('Working', defaultdict(lambda: '<Unknown>'))
        if self.hb_iteration % 10 == 0:
            LOGGER.info("Rendered %.2f%%, average %.2f fps, current %.2f fps",
                        100 * working.get('Progress', 0),
                        working.get('RateAvg', 0),
                        working.get('Rate', 0))

        Trackers.report_scalar("Encoder performance", "Encoding frames per second",
                               working.get('Rate', 0),
                               self.hb_iteration)
        Trackers.report_scalar("Encoder performance",
                               "Encoding average frames per second",
                               working.get('RateAvg', 0),
                               self.hb_iteration)

        self.hb_iteration += 1

    def service_handbrakecli_job(self, handbrakecli):

        while not handbrakecli.output_queue.empty():
            stream_label, seconds, line = handbrakecli.output_queue.get()
            if stream_label == 'EXC':
                raise line
            if stream_label == 'OUT':
                if self.json_discard_on:
                    if line.startswith('}'):  # Use the absense of indent to determine the end
                        self.json_discard_on = False
                    # Don't log discarded lines
                else:
                    if self.json_capture_on:
                        self.json_data.append(line.rstrip())
                        if line.startswith('}'):  # Use the absense of indent to determine the end
                            self.json_capture_on = False
                            progress = json.loads("\n".join(self.json_data))
                            if progress['State'] == 'WORKING':
                                self.handle_handbrakecli_progress(progress)
                    else:
                        match = re.search(r'Progress: \{', line)
                        if match:
                            self.json_capture_on = True
                            self.json_data = ['{']
                        else:
                            LOGGER.info(line)
            else:
                LOGGER.info("%s", line.rstrip())

    def scan_prores(self):
        prores_path = self.path_maker.prores_path(self.params.item.item_name)
        prores_scan = file_scanner.FileScanner(prores_path, f"Scan {self.params.item.item_name}", self.params)
        return prores_scan.scan_video()

    def scan_final(self):
        final_path = self.path_maker.final_path(self.params.item.item_name)
        final_scan = file_scanner.FileScanner(final_path, f"Scan {self.params.item.item_name}", self.params)
        return final_scan.scan_video()

    def execute(self, force_final, force_prores):
        self.prores_scan_result = self.scan_prores()
        if not self.prores_scan_result['valid'] or force_prores:
            self.do_aerender()

        self.prores_scan_result = self.scan_prores()

        if not self.prores_scan_result['valid']:
            raise Exception(f"Failed to create valid ProRes file: {self.prores_scan_result['message']}")

        self.final_scan_result = self.scan_final()

        if not self.final_scan_result['valid'] or force_final:
            self.do_hbrender()

        self.final_scan_result = self.scan_final()

        if not self.final_scan_result['valid']:
            raise Exception(f"Failed to create valid final file: {self.final_scan_result['message']}")

        return self.prores_scan_result, self.final_scan_result
