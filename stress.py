#!/usr/bin/env python3

import multiprocessing
import os
import subprocess
import time
from typing import List

import check
import conf

# TODO
# make validators/stressors generic
# make a stream grep validator
# make a "stressor exited" validator
# make fsstress go really big, but not ENOSPC big
# run all validators (except fsck) each time and at the end (no early exit)
# real memory pressure? in a cgroup?

class StressFailure:
    def __init__(self, msg):
        self.msg = msg
    def display(self):
        print("\033[31m" + f"stress failure: {self.msg}" + "\033[0m")

class Validator:
    def check(self) -> StressFailure | None:
        raise NotImplementedError
    def stop(self):
        pass

class RoValidator(Validator):
    def __init__(self, mount):
        self.mount = mount

    def check(self):
        ro = subprocess.run(["findmnt", "-n",  self.mount, "-O", "ro"],
                            text=True, capture_output=True)
        if ro.returncode == 0:
            return StressFailure(f"{self.mount} went read-only")
        return None

class FsckValidator(Validator):
    def __init__(self, config):
        self.config = config
        self.mount = self.config["global"]["directory"]
        self.dev = check.get_dev(self.mount)

    def check(self):
        btrfs = conf.get_btrfs_util(self.config)
        umounted = False
        conf.dbg(self.config, f"unmounting {self.dev} for btrfs check...")
        umount_retries = conf.get_optional_config(self.config["global"], "umount_retries", 5)
        for i in range(umount_retries):
            umount = subprocess.run(["umount", self.dev], capture_output=True)
            if umount.returncode == 0:
                umounted = True
                conf.dbg(self.config, "unmounted!")
                break
            time.sleep(1 << (i % 3))

        if umounted:
            cmd = [btrfs, "check", self.dev]
        else:
            cmd = [btrfs, "check", "--force", self.dev]
        conf.dbg(self.config, f"run fsck: {cmd}")
        fsck = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        if umounted:
            conf.dbg(self.config, f"remounting {self.dev} at {self.mount}")
            subprocess.run(["mount", self.dev, self.mount])

        if fsck.returncode != 0:
            with open("fsck.out", "w") as f:
                f.write(fsck.stdout)
            return StressFailure(f"fsck failed: {fsck.returncode} (results in fsck.out)")

        return None

# Given a stream and some bad strings to look out for, read the stream
# and look for the needles.
#
# Note that currently this relies on making the stream O_NONBLOCK which
# is not compatible with a subprocess.PIPE and text=True
class StreamGrepValidator(Validator):
    def __init__(self, stream, needles):
        os.set_blocking(stream.fileno(), False)
        self.stream = stream
        self.needles = needles
        self.data = ""

    def check(self):
        data = str(self.stream.read())
        if not data:
            return None
        self.data += data
        for needle in self.needles:
            if needle in data:
                return StressFailure(f"found {needle} in stream")
                #TODO dump bad output to file
        return None

class ProcRunningValidator(Validator):
    def __init__(self, proc):
        self.proc = proc

    def check(self):
        if self.proc.poll():
            return StressFailure(f"proc {self.proc.args[0]}[{self.proc.pid}] exited ({self.proc.returncode})")

class MultiprocRunningValidator(Validator):
    def __init__(self, proc):
        self.proc = proc

    def check(self):
        if not self.proc.is_alive():
            return StressFailure(f"multiprocess {self.proc.args[0]}[{self.proc.pid}] exited ({self.proc.returncode})")

class CommitStatsValidator(Validator):
    def __init__(self, mount, thresh_ms):
        self.mount = mount
        self.sysfs_path = os.path.join("/sys/fs/btrfs/", check.fs_uuid(self.mount), "commit_stats")
        self.thresh_ms = thresh_ms

    def check(self):
        with open(self.sysfs_path, "r") as f:
            lines = f.read().split("\n")
            max_commit_ms_line = lines[2]
            max_commit_ms = int(max_commit_ms_line.split(" ")[1])
            if max_commit_ms > self.thresh_ms:
                return StressFailure(f"max_commit_ms {max_commit_ms} > threshold {self.thresh_ms}")

class Stressor:
    def run(self) -> bool:
        raise NotImplementedError
    def stop(self) -> None:
        raise NotImplementedError
    def validators(self) -> List[Validator]:
        raise NotImplementedError

class Fsx(Stressor):
    def __init__(self, config):
        self.config = config
        self.fsx_config = config["fsx"]
        self.nr_procs = self.fsx_config["procs"]
        # TODO can this be an @property?
        self.m_validators = []
        self.procs = []

    def run(self):
        fstests_path = self.config["global"]["fstests"]
        directory = conf.get_directory(self.config)
        fsx_path = os.path.join(fstests_path, "ltp/fsx")
        if conf.test_optional_config(self.config["global"], "dry_run"):
            print(f"dry_run: fsx: {fsx_cmd}")
            return False
        for proc_id in range(self.nr_procs):
            fsx_file = os.path.join(directory, f"stress-fsx.{proc_id}")
            fsx_cmd = [fsx_path, "-x", fsx_file]
            proc = subprocess.Popen(fsx_cmd,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            self.procs.append(proc)
            grep_validator = StreamGrepValidator(proc.stdout, ["READ BAD DATA"])
            self.m_validators.append(grep_validator)
            self.m_validators.append(ProcRunningValidator(proc))
        return True

    def stop(self):
        if not self.procs:
            return
        for proc in self.procs:
            conf.dbg(self.config, f"stop fsx pid {proc.pid}")
            proc.terminate()
            proc.wait()

    def validators(self):
        return self.m_validators

class Fsstress(Stressor):
    def __init__(self, config):
        self.config = config
        self.m_validators = []

    def run(self):
        self.proc = None
        fstests_path = self.config["global"]["fstests"]
        directory = conf.get_directory(self.config)
        fsstress_path = os.path.join(fstests_path, "ltp/fsstress")
        fsstress_procs = self.config["fsstress"]["procs"]
        fsstress_dir = os.path.join(directory, "fsstress")
        fsstress_cmd = [fsstress_path, "-d", fsstress_dir, "-w",
                        "-n", "100000", "-p", f"{fsstress_procs}",
                        "-l", "0"]
        if conf.test_optional_config(self.config["global"], "dry_run"):
            print(f"dry_run: fsstress: {fsstress_cmd}")
            return False
        self.proc = subprocess.Popen(fsstress_cmd,
                                     stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     text=True)
        self.m_validators.append(ProcRunningValidator(self.proc))
        return True

    def stop(self):
        if not self.proc:
            return
        conf.dbg(self.config, f"stop fsstress pid {self.proc.pid}")
        self.proc.terminate()
        self.proc.wait()

    def validators(self):
        return self.m_validators

# Not really a stressor per-se, but fits the execution/validation model perfectly
class Dmesg(Stressor):
    def __init__(self, config):
        self.config = config
        self.m_validators = []

    def run(self):
        if conf.test_optional_config(self.config["global"], "dry_run"):
            return False
        self.proc = subprocess.Popen(["dmesg", "-T", "-W"],
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
        bad_dmesg_lines = [
            "WARNING",
            "BTRFS warning",
            "BTRFS error",
            "BTRFS critical",
            "BTRFS alert"
        ]
        self.m_validators.append(StreamGrepValidator(self.proc.stdout, bad_dmesg_lines))
        self.m_validators.append(ProcRunningValidator(self.proc))
        return True

    def stop(self):
        if not self.proc:
            return
        conf.dbg(self.config, f"stop dmesg pid {self.proc.pid}")
        self.proc.terminate()
        self.proc.wait()

    def validators(self):
        return self.m_validators

class LoopStressor(Stressor):
    def __init__(self, config, stressor, delay):
        self.config = config
        self.stressor = stressor
        self.delay = delay
        self.m_validators = []

    def do_loop(self):
        while True:
            self.stressor.run()
            time.sleep(self.delay)

    def run(self):
        self.p = multiprocessing.Process(target=self.do_loop)
        self.p.start()
        self.m_validators.append(MultiprocRunningValidator(self.p))
        return True

    def stop(self):
        conf.dbg(self.config, f"stop loop stressor pid {self.p.pid}")
        self.p.terminate()
        self.p.join()

    def validators(self):
        return self.m_validators

class GatedStressor(Stressor):
    def __init__(self, config, stressor, gates):
        self.config = config
        self.stressor = stressor
        self.gates = gates
        self.running = False

    def test_gates(self):
        for gate in self.gates:
            spl = gate.split(":")
            conf.dbg(self.config, f"gated stressor: gate {gate} split {spl}")
            if len(spl) == 1:
                section = spl[0]
                if section not in self.config:
                    conf.dbg(self.config, f"skip gated stressor: {section}")
                    return False
            if len(spl) == 2:
                section, field = spl
                if not conf.test_optional_config(self.config[section], field):
                    conf.dbg(self.config, f"skip gated stressor: {section}:{field}")
                    return False
        return True

    def run(self):
        if not self.test_gates():
            return False
        self.running = self.stressor.run()

    def stop(self):
        if self.running:
            self.stressor.stop()

    def validators(self):
        return self.stressor.validators()

class CmdStressor(Stressor):
    def __init__(self, config, cmd):
        self.config = config
        self.cmd = cmd

    def run(self):
        p = subprocess.run(self.cmd, text=True, capture_output=True)
        conf.dbg(self.config, f"ran {self.cmd} {p.stdout} {p.returncode}")
    def stop(self):
        pass
    def validators(self):
        return []

class ReflinkStressor(Stressor):
    def __init__(self, config):
        self.config = config
    def run(self):
        return False
    def stop(self):
        pass
    def validators(self):
        return []

class DropCachesStressor(Stressor):
    def __init__(self, config):
        self.config = config
    def run(self):
        return False
    def stop(self):
        pass
    def validators(self):
        return []

class MemoryPressureStressor(Stressor):
    def __init__(self, config):
        self.config = config
    def run(self):
        return False
    def stop(self):
        pass
    def validators(self):
        return []

def run_validators(validators) -> List[StressFailure]:
    failures = []
    for v in validators:
        f = v.check()
        if f:
            failures.append(f)
    return failures

def keep_going(config, start, validators) -> bool:
    failures = run_validators(validators)
    if failures:
        raise check.StressException(failures)
    now = time.monotonic()
    duration = config["global"]["duration"]
    conf.dbg(config, f"keep going? elapsed {int(now - start)} duration {duration}")
    if now - start > duration:
        return False
    return True

def run_for_duration(config, validators):
    start = time.monotonic()
    print(f"Running for {config["global"]["duration"]}s...")
    try:
        while keep_going(config, start, validators):
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    except check.StressException as e:
        for f in e.failures:
            f.display()
        return False
    return True

def main():
    config = conf.get_config("conf.toml")
    conf.validate_config(config)
    directory = conf.get_directory(config)
    btrfs = conf.get_btrfs_util(config)

    check.write_sentinel()
    stressors:List[Stressor] = []
    validators:List[Validator] = []

    stressors.append(Dmesg(config))
    stressors.append(GatedStressor(config, Fsx(config), ["fsx"]))
    stressors.append(GatedStressor(config, Fsstress(config), ["fsstress"]))
    balance_cmd = [btrfs, "balance", "start", "-dusage=100", directory]
    stressors.append(
        GatedStressor(config, LoopStressor(config, CmdStressor(config, balance_cmd), 1),
                      ["btrfs:balance"]))
    stressors.append(
        GatedStressor(config, LoopStressor(config, ReflinkStressor(config), 1),
                      ["btrfs:reflink"]))
    stressors.append(
        GatedStressor(config, LoopStressor(config, DropCachesStressor(config), 1),
                      ["system:drop_caches"]))

    validators.append(CommitStatsValidator(directory, 10000))
    validators.append(RoValidator(directory))
    for stressor in stressors:
        stressor.run()
        validators.extend(stressor.validators())

    if not stressors:
        exit(0)

    ok = run_for_duration(config, validators)

    for stressor in stressors:
        stressor.stop()

    fsck = FsckValidator(config).check()
    if fsck:
        ok = False
        fsck.display()

    if ok:
        print("\033[32mOK\033[0m")


if __name__ == "__main__":
    main()
