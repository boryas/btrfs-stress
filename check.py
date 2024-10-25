import subprocess
import time

import conf

# Perform various checks that validate the stress run was successful
# - is it RO?
# - btrfs check
# - btrfs errors in dmesg
# - WARNINGs in dmesg
# - data integrity (should be handled by fsx)

SENTINEL = "begin btrfs stress run"

class StressFailure(Exception):
    pass

def check_ro(config):
    directory = config["global"]["directory"]
    ro = subprocess.run(["findmnt", "-n",  directory, "-O", "ro"],
                        text=True, capture_output=True)
    if ro.returncode == 0:
        raise StressFailure(f"fs went ro: {ro.stdout.strip()}")

def check_fsck(config):
    directory = config["global"]["directory"]
    btrfs = conf.get_btrfs_util(config)
    dev = get_dev(directory)

    umounted = False
    print(f"unmounting {dev} for btrfs check...")
    for _ in range(60):
        umount = subprocess.run(["umount", dev])
        if umount.returncode == 0:
            umounted = True
            print("unmounted!")
            break
        time.sleep(1)

    if umounted:
        cmd = [btrfs, "check", dev]
    else:
        cmd = [btrfs, "check", "--force", dev]
    print(f"run fsck: {cmd}")
    fsck = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    if umounted:
        print(f"remounting {dev} at {directory}")
        subprocess.run(["mount", dev, directory])

    if fsck.returncode != 0:
        with open("fsck.out", "w") as f:
            f.write(fsck.stdout)
        raise StressFailure(f"fsck failed: {fsck.returncode} results in fsck.out")

def check_dmesg(config):
    dmesg = subprocess.run(["dmesg"], text=True, capture_output=True).stdout
    pos = dmesg.rfind("begin btrfs stress run")
    dmesg = dmesg[pos:]
    if "WARNING" in dmesg:
        raise StressFailure("WARNING in dmesg")
    if "BTRFS warning" in dmesg:
        raise StressFailure("btrfs_warn in dmesg")
    if "BTRFS error" in dmesg:
        raise StressFailure("btrfs_err in dmesg")
    if "BTRFS critical" in dmesg:
        raise StressFailure("btrfs_crit in dmesg")
    if "BTRFS alert" in dmesg:
        raise StressFailure("btrfs_alert in dmesg")

def get_dev(mount) -> str:
    findmnt = subprocess.run(["findmnt", "-n", "-o", "SOURCE", mount],
                             text=True, capture_output=True)
    return findmnt.stdout.strip()

def write_sentinel():
    with open("/dev/kmsg", "w") as kmsg:
        kmsg.write(SENTINEL)

def check(config):
    check_ro(config)
    check_fsck(config)
    check_dmesg(config)
