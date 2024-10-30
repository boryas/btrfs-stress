import shlex
import subprocess

SENTINEL = "begin btrfs stress run"

class StressException(Exception):
    def __init__(self, failures):
        super().__init__(self, failures)
        self.failures = failures
    def display(self):
        for f in self.failures:
            f.display()

def fs_uuid(mount):
    cmd = f"findmnt -no UUID {mount}"
    uuid = subprocess.run(shlex.split(cmd), text=True,
                          capture_output=True)
    return uuid.stdout.strip()

def get_dev(mount) -> str:
    findmnt = subprocess.run(["findmnt", "-n", "-o", "SOURCE", mount],
                             text=True, capture_output=True)
    return findmnt.stdout.strip()

def write_sentinel():
    with open("/dev/kmsg", "w") as kmsg:
        kmsg.write(SENTINEL)
