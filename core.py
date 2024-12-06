#!/usr/bin/env python3
import asyncio
from asyncio.subprocess import PIPE

import conf

class ValidationFailure(Exception):
    pass

class Happy(Exception):
    pass

async def stream_grep(stream, needles):
    while (True):
        data = await stream.readline()
        if not data:
            break
        data = data.decode("ascii", "ignore")
        for needle in needles:
            if needle in data:
                raise ValidationFailure(f"stream_grep hit: {needle}")

async def cancellable_proc(name, t, allow_exit=False, grep=None):
    kill_proc = None
    try:
        proc = await t
        kill_proc = proc
        if grep:
            await stream_grep(proc.stdout, grep)
        await proc.wait()
        kill_proc = None
        if not allow_exit:
            raise ValidationFailure(f"{name} exited early")
    finally:
        if kill_proc:
            kill_proc.terminate()
            await proc.wait()
    return proc

async def run_cmd(name, cmd, args, ignore_err=False,
                  stdout=asyncio.subprocess.PIPE,
                  stderr=asyncio.subprocess.PIPE):
    proc = None
    out = None
    err = None
    try:
        proc = await asyncio.create_subprocess_exec(cmd, *args,
                                                    stdout = stdout,
                                                    stderr = stderr)
        out, err = await proc.communicate()
        if out:
            out = out.decode("ascii", "ignore")
        if err:
            err = err.decode("ascii", "ignore")
        if not ignore_err and proc.returncode != 0:
            print(f"{cmd} {args} bad retcode {proc.returncode} {err}")
            raise ValidationFailure(f"{name} failed: {proc.returncode} {err}")
        proc = None
    finally:
        if proc:
            proc.terminate()
            await proc.wait()
    return out, err


async def run_for_duration(config):
    duration = config["global"]["duration"]
    await asyncio.sleep(duration)
    raise Happy("Happy!")

async def dmesg():
    name = "dmesg"
    bad_lines = [
        "WARNING",
        "BTRFS warning",
        "BTRFS error",
        "BTRFS critical",
        "BTRFS alert"
    ]
    proc = asyncio.create_subprocess_exec("dmesg", "-T", "-W",
                                          stdout = asyncio.subprocess.PIPE)
    return await cancellable_proc(name, proc, grep=bad_lines)

async def loopy(t, *args, delay=1):
    while True:
        await t(*args)
        await asyncio.sleep(delay)
