import logging
import queue
import subprocess
import threading
import time

import psutil

LOGGER = logging.getLogger('process_wrapper')
logging.basicConfig(level=logging.DEBUG)


class ProcessWrapper:
    def __init__(self, command):
        self.command = command
        self.process = None
        self.output_queue = queue.Queue()
        self.thread = threading.Thread(target=self.run_process)

    def output_reader(self, stream, stream_label):
        """Read output/errors in a separate thread and put it into a queue with a label."""
        try:
            for line in iter(stream.readline, b''):
                self.output_queue.put((stream_label, time.monotonic(), line.decode().rstrip('\n')))
            stream.close()
        except Exception as exc:
            self.output_queue.put(("EXC", time.monotonic(), exc))

    def run_process(self):
        self.process = subprocess.Popen(self.command, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)

        stdout_thread = threading.Thread(target=self.output_reader, args=(self.process.stdout, 'OUT'))
        stderr_thread = threading.Thread(target=self.output_reader, args=(self.process.stderr, 'ERR'))

        stdout_thread.start()
        stderr_thread.start()

        stdout_thread.join()
        stderr_thread.join()

    def kill(self):
        """Kill the running process"""
        if self.process:
            LOGGER.info("Killing process tree with pid: %s", self.process.pid)
            parent = psutil.Process(self.process.pid)
            for child in parent.children(recursive=True):
                try:
                    child.kill()
                    time.sleep(0.5)
                    LOGGER.info("Killed child process with pid %s", child.pid)
                except (psutil.NoSuchProcess, ProcessLookupError):
                    LOGGER.info("Child process with pid %s no longer active", child.pid)
            try:
                parent.kill()
                LOGGER.info("Killed parent process with pid %s", child.pid)
            except (psutil.NoSuchProcess, ProcessLookupError):
                LOGGER.info("Parent process with pid %s no longer active", child.pid)

    def run(self):
        LOGGER.info("Executing command: %s", " ".join(self.command))
        self.thread.start()

    def is_alive(self):
        return self.thread.is_alive()

    def get_return_code(self):
        if self.process:
            rc = self.process.returncode
            if rc is None:
                time.sleep(1.0)
                rc = self.process.returncode
            return rc
        else:
            return None
