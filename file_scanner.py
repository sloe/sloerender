import json
import logging
import os
import platform
import re
import time
from datetime import datetime

import process_wrapper

LOGGER = logging.getLogger('file_scanner')
logging.basicConfig(level=logging.DEBUG)


class FileScanner:
    def __init__(self, file_path, job_name, params):
        self.file_path = file_path
        self.job_name = job_name
        self.params = params

        self.file_data = {}
        self.json_capture_on = None
        self.json_data = None
        self.json_discard_on = None

    def scan_video(self):
        if not os.path.isfile(self.file_path):
            self.file_data.update(dict(
                valid=False,
                result='NO_FILE',
                message=f"File {self.file_path} does not exist"
            ))
            return self.file_data

        self.file_data['file'] = dict(
            atime=datetime.fromtimestamp(os.path.getatime(self.file_path)).isoformat(),
            ctime=datetime.fromtimestamp(os.path.getctime(self.file_path)).isoformat(),
            node=platform.node(),
            mtime=datetime.fromtimestamp(os.path.getmtime(self.file_path)).isoformat(),
            path=os.path.abspath(self.file_path),
            size=os.path.getsize(self.file_path)
        )
        hb_path = os.path.join(self.params.render.hb.hb_dir, 'HandBrakeCLI.exe')

        hb_scan_command = [
            hb_path,
            '--input', self.file_path,
            '--json',
            '--scan',
            '--title', '0',
        ]

        handbrakecli = process_wrapper.ProcessWrapper(hb_scan_command)
        self.json_capture_on = False
        self.json_discard_on = False
        handbrakecli.run()
        self.start_time = time.monotonic()
        try:
            while handbrakecli.is_alive():
                self.service_job(handbrakecli)
                time.sleep(0.5)

        except (Exception, KeyboardInterrupt) as exp:
            handbrakecli.kill()
            raise

        self.service_job(handbrakecli)

        rc = handbrakecli.get_return_code()
        if rc == 0:
            LOGGER.info(f"Successful: {self.job_name}")
            self.file_data.update(dict(
                valid=True,
                result='VALID',
                message=f"File {self.file_path} is present and valid"
            ))
        else:
            LOGGER.error(f"+++RETURN CODE %s: %s", rc, self.job_name)
            self.file_data.update(dict(
                valid=False,
                result='INVALID',
                message=f"File {self.file_path} is present but not valid"
            ))
        return self.file_data

    def service_job(self, handbrakecli):

        while not handbrakecli.output_queue.empty():
            stream_label, seconds, line = handbrakecli.output_queue.get()
            if stream_label == 'EXC':
                raise line  # line is an instance of Exception in this case

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
                            self.file_data['video'] = json.loads("\n".join(self.json_data))
                            # Rearrange data to look good in trackers
                            for i, title in enumerate(self.file_data['video']['TitleList']):
                                for j, audio_item in enumerate(title['AudioList']):
                                    title[f"Audio{j}"] = audio_item
                                del title['AudioList']
                                for j, chapter_item in enumerate(title['ChapterList']):
                                    title[f"Chapter{j}"] = chapter_item
                                del title['ChapterList']

                                self.file_data['video'][f"Title{i}"] = title

                            del self.file_data['video']['TitleList']
                    else:
                        match = re.search(r'JSON Title Set: \{', line)
                        if match:
                            self.json_capture_on = True
                            self.json_data = ['{']
                        else:
                            match = re.search(r'Progress: \{', line)
                            if match:
                                self.json_discard_on = True
                    if not self.json_discard_on:
                        LOGGER.debug(line)
            else:
                LOGGER.info("%s", line.rstrip())
