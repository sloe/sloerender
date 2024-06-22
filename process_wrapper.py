import queue
import subprocess
import threading


class ProcessWrapper:
    def __init__(self, command):
        self.command = command
        self.process = None
        self.output_queue = queue.Queue()
        self.thread = threading.Thread(target=self.run_process)

    def output_reader(self, stream, stream_label):
        """Read output/errors in a separate thread and put it into a queue with a label."""
        for line in iter(stream.readline, b''):
            self.output_queue.put((stream_label, line))
        stream.close()

    def run_process(self):
        self.process = subprocess.Popen(self.command, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)

        stdout_thread = threading.Thread(target=self.output_reader, args=(self.process.stdout, 'OUT'))
        stderr_thread = threading.Thread(target=self.output_reader, args=(self.process.stderr, 'ERR'))

        stdout_thread.start()
        stderr_thread.start()

    def print_output(self):
        while not self.output_queue.empty():
            stream_label, line = self.output_queue.get()
            print(f'{stream_label}: {line.decode().strip()}')  # Decode bytes to string and print

    def run(self):
        self.thread.start()

    def is_alive(self):
        return self.thread.is_alive()
