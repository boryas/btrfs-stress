#!/usr/bin/env python3

import multiprocessing
import os
import subprocess
import time
from typing import List

import check
import conf

def run_fsx_proc(config, proc) -> subprocess.Popen | None:
    fstests_path = config["global"]["fstests"]
    directory = config["global"]["directory"]
    fsx_path = os.path.join(fstests_path, "ltp/fsx")
    fsx_file = os.path.join(directory, f"stress-fsx.{proc}")
    fsx_cmd = [fsx_path, "-x", fsx_file]
    if conf.test_optional_config(config["global"], "dry_run"):
        print(f"dry_run: fsx: {fsx_cmd}")
        return None
    return subprocess.Popen(fsx_cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

def run_fsx_procs(config) -> List[subprocess.Popen]:
    if "fsx" not in config:
        return []
    fsx_procs = []
    fsx_section = config["fsx"]
    for proc in range(fsx_section["procs"]):
        p = run_fsx_proc(config, proc)
        if p:
            fsx_procs.append(p)
    return fsx_procs

def run_fsstress_proc(config) -> subprocess.Popen | None:
    if "fsstress" not in config:
        return None
    fstests_path = config["global"]["fstests"]
    directory = config["global"]["directory"]
    fsstress_path = os.path.join(fstests_path, "ltp/fsstress")
    fsstress_procs = config["fsstress"]["procs"]
    fsstress_dir = os.path.join(directory, "fsstress")
    fsstress_cmd = [fsstress_path, "-d", fsstress_dir,
                    "-w", "-n", "10000", "-p", f"{fsstress_procs}", "-l", "0"]
    if conf.test_optional_config(config["global"], "dry_run"):
        print(f"dry_run: fsstress: {fsstress_cmd}")
        return None
    return subprocess.Popen(fsstress_cmd,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

def balance_loop(config):
    mount = config["global"]["directory"]
    btrfs = conf.get_btrfs_util(config)
    cmd = [btrfs, "balance", "start", "-dusage=100", mount]
    print(f"run balance loop {cmd}")
    while True:
        subprocess.run(cmd, capture_output=True)
        time.sleep(1)

def run_balance_loop(config) -> multiprocessing.Process | None:
    if "btrfs" not in config:
        return None
    if not conf.test_optional_config(config["btrfs"], "balance"):
        return None
    p = multiprocessing.Process(target=balance_loop, args=[config])
    p.start()
    return p

def main():
    config = conf.get_config("conf.toml")
    conf.validate_config(config)

    check.write_sentinel()
    fsx_procs = run_fsx_procs(config)
    fsstress_proc = run_fsstress_proc(config)
    balance_proc = run_balance_loop(config)

    if not fsx_procs and not fsstress_proc:
        exit(0)

    try:
        duration = config["global"]["duration"]
        print(f"Sleeping {duration}...")
        time.sleep(duration)
    except KeyboardInterrupt:
        pass

    for fsx_proc in fsx_procs:
        print(f"kill fsx {fsx_proc.pid}")
        fsx_proc.kill()
    if fsstress_proc:
        print(f"kill fsstress {fsstress_proc.pid}")
        fsstress_proc.kill()
    if balance_proc:
        print(f"stop balance loop")
        balance_proc.terminate()

    for fsx_proc in fsx_procs:
        fsx_proc.wait()
    if fsstress_proc:
        fsstress_proc.wait()
    if balance_proc:
        balance_proc.join()

    check.check(config)

if __name__ == "__main__":
    main()
