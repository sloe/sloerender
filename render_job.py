import datetime
import json
import logging
import os
import queue
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
LOGGER.setLevel(level=logging.DEBUG)


class RenderJobParams(BaseModel):
    item: item_params.ItemParams
    output: output_params.OutputParams
    render: render_params.RenderParams


class RenderTimeout(Exception):
    pass


class RenderJob:
    def __init__(self, path_maker, job_name, params):
        self.AE_ACTIVITY_TIMEOUT = 300
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
        self.last_activity_time = None
        self.last_frame_time = None
        self.prores_scan_result = None
        self.start_time = None

    def extract_preferences(self):
        major, minor = map(int, self.params.render.ae.condensed_version.split("."))
        base_prefs_path = self.params.render.ae.ae_prefs_path.replace('{user}', os.getenv('USERNAME'))
        for test_minor in range(minor + 32, minor - 1, -1):
            test_version = f"{major}.{test_minor}"
            ae_prefs_path = base_prefs_path.replace('{version}', test_version)
            if os.path.isfile(ae_prefs_path):
                LOGGER.info(f"Using AE prefs file {ae_prefs_path} for version {test_version}")
                break
        else:
            raise Exception(
                f"Unable to find AE preferences file using {base_prefs_path} and {self.params.render.ae.condensed_version}")

        prefs = {}
        with open(ae_prefs_path, 'r') as f:
            current_section = None
            for line in f:
                if line.strip().startswith('['):
                    current_section = line.strip(' []"\n')
                elif current_section == "Concurrent Frame Rendering":
                    if '=' in line:
                        key, value = [x.strip().strip('"') for x in line.split('=')]
                        prefs[key] = value

        tags = {}
        enable_cfr = prefs.get("Enable Concurrent Frame Renders")
        num_concurrent_frames = prefs.get("Number of Concurrent Frame Renders")
        reserved_cpu_power = prefs.get("Reserved CPU Power")

        if reserved_cpu_power and int(reserved_cpu_power) > 10:
            LOGGER.warning("Bad value for Reserved CPU Power %d", reserved_cpu_power)

        if enable_cfr is None:
            tags[f"mfr=unknown"] = True
        elif int(enable_cfr):
            tags[f"mfr={int(num_concurrent_frames)}"] = True
        else:
            tags["mfr=off"] = True

        return tags

    def do_aerender(self):
        aerender_dir = os.path.join(
            self.params.render.ae.aerender_dir.replace('{ae.major_version}', self.params.render.ae.major_version),
            'aerender.exe')

        tags = self.extract_preferences()
        Trackers.add_tags(tags)

        prores_path = self.path_maker.prores_path(self.params.item.item_name, mkdir=True)
        project_path = self.path_maker.item_ae_project_path(self.params.item.ae_project)

        LOGGER.info(f"Launching aerender.exe to render %s from project %s comp %s",
                    prores_path, project_path, self.params.item.ae_comp)
        aerender_command = [
            aerender_dir,
            '-v', 'ERRORS_AND_PROGRESS',
            '-project', project_path,
            '-comp', self.params.item.ae_comp,
            '-mem_usage', str(self.params.render.ae.image_cache_percent), str(self.params.render.ae.max_mem_percent),
            '-output', prores_path,  # self.params.output.destination_path,
            '-RStemplate', self.params.render.ae.render_settings_template,
            '-OMtemplate', self.params.render.ae.output_module_template
        ]

        if self.params.render.ae.mfr:
            aerender_command += [
                '-mfr', 'ON', str(self.params.render.ae.mfr_max_cpu_percent)
            ]
        else:
            aerender_command += [
                '-mfr', 'OFF', '0'
            ]

        if self.params.render.ae.play_sound:
            aerender_command += [
                '-sound', 'ON'
            ]

        aerender = process_wrapper.ProcessWrapper(aerender_command)
        self.ae_child_pids = []
        aerender.run()
        self.start_time = time.monotonic()
        self.last_activity_time = time.monotonic()
        try:
            while aerender.is_alive():
                was_active = self.service_aerender_job(aerender)
                if was_active:
                    self.last_activity_time = time.monotonic()
                else:
                    current_time = time.monotonic()
                    seconds_since_activity = current_time - self.last_activity_time
                    if seconds_since_activity > self.AE_ACTIVITY_TIMEOUT - 30:
                        LOGGER.warning(" No progress for %d seconds.  Approaching watchdog timeout",
                                       seconds_since_activity)
                    if seconds_since_activity > self.AE_ACTIVITY_TIMEOUT:
                        LOGGER.error("+++ No progress for %d seconds, watchdog activated", seconds_since_activity)
                        raise RenderTimeout("AE timed out after %d seconds of inactivity" % seconds_since_activity)
                time.sleep(0.5)

        except (Exception, KeyboardInterrupt) as exp:
            aerender.kill(extra_pids=self.ae_child_pids)
            self.delete_on_failure(prores_path)
            raise

        self.service_aerender_job(aerender)

        rc = aerender.get_return_code()
        if rc == 0:
            LOGGER.info(f"Successful: {self.job_name}")
        else:
            LOGGER.error(f"+++RETURN CODE %s: %s", rc, self.job_name)
            self.delete_on_failure(prores_path)
            raise Exception(f"AE render process exited with rc={rc}")

    def delete_on_failure(self, output_path):
        if self.params.render.ae.delete_output_on_failure:
            if os.path.exists(output_path):
                for i in range(12):
                    try:
                        os.remove(output_path)
                        LOGGER.info(f"Deleted incomplete output file: {output_path}")
                        break
                    except PermissionError as exc:
                        LOGGER.warning(
                            f"Attempt to delete incomplete output file {output_path} failed, will retry: {exc}")
                        time.sleep(1)
                else:
                    raise Exception(f"Failed to delete incomplete output file: {output_path}")

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
                for i, ae_pid in enumerate(self.ae_child_pids):
                    try:
                        process = psutil.Process(ae_pid)
                        cpu_times = process.cpu_times()
                        cpu_user_str = str(datetime.timedelta(seconds=int(cpu_times.user)))
                        cpu_system_str = str(datetime.timedelta(seconds=int(cpu_times.system)))
                        LOGGER.info("CPU times of After Effects process with pid %s: user=%s, system=%s", ae_pid,
                                    cpu_user_str, cpu_system_str)
                        Trackers.report_scalar("CPU cumulative time",
                                               "After Effects User CPU seconds",
                                               cpu_times.user,
                                               frame_num)
                        Trackers.report_scalar("CPU cumulative time",
                                               "After Effects System CPU seconds",
                                               cpu_times.system,
                                               frame_num)
                    except (psutil.NoSuchProcess, ProcessLookupError):
                        LOGGER.info("After Effects process with PID %s no longer active", ae_pid)
                        del self.ae_child_pids[i]

        self.last_frame_time = seconds

    def service_aerender_job(self, aerender):
        is_active = False
        for _ in range(100):
            try:
                stream_label, seconds, line = aerender.output_queue.get(block=True, timeout=5.0)
            except queue.Empty:
                break

            if stream_label == 'EXC':
                raise line
            if stream_label == 'OUT':
                is_active = True
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

        return is_active

    def do_hbrender(self):

        prores_path = self.path_maker.prores_path(self.params.item.item_name)
        final_path = self.path_maker.final_path(self.params.item.item_name, mkdir=True)
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
            if self.params.render.hb.delete_intermediate_on_success:
                file_size_gb = os.path.getsize(prores_path) / (1024 * 1024 * 1024)
                os.remove(prores_path)
                LOGGER.info(f"Deleted intermediate file with size {file_size_gb:.2f}GB: {prores_path}")
        else:
            LOGGER.error(f"+++RETURN CODE %s: %s", rc, self.job_name)
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
            try:
                self.do_aerender()
            except RenderTimeout:
                LOGGER.error(f"Retrying due to render timeout")
                self.do_aerender()

        self.prores_scan_result = self.scan_prores()

        if not self.prores_scan_result['valid']:
            LOGGER.error(f"Failed to create valid ProRes file (retrying): {self.prores_scan_result['message']}")
            self.do_aerender()
            self.prores_scan_result = self.scan_prores()
            if not self.prores_scan_result['valid']:
                raise Exception(
                    f"Failed to create valid ProRes file (second attempt): {self.prores_scan_result['message']}")

        self.final_scan_result = self.scan_final()

        if not self.final_scan_result['valid'] or force_final:
            self.do_hbrender()

        self.final_scan_result = self.scan_final()

        if not self.final_scan_result['valid']:
            raise Exception(f"Failed to create valid final file: {self.final_scan_result['message']}")

        return self.prores_scan_result, self.final_scan_result
