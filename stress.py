#!/usr/bin/env python3

import configparser
import os
import subprocess

config = configparser.ConfigParser()

if __name__ == "__main__":
    config.read("btrfs-stress.conf")
    if "default" not in config:
        print("Invalid btrfs-stress.conf: no default section")
        exit(-22)
    fstests_path = config["default"]["fstests"]
    fsx_path = os.path.join(fstests_path, "ltp/fsx")
    print(f"run fsx: {fsx_path}")
    fsx_cmd = [fsx_path, "-x", "--duration=5", "/mnt/lol/foo.fsx"]
    fsx_p = subprocess.Popen(fsx_cmd, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = fsx_p.communicate()
    print(out)
