#!/usr/bin/env python3

import asyncio
import glob
import os
import random

import conf
import core

def ltp_cmd(config, cmd):
    fstests_path = conf.get(config, "global:fstests")
    return os.path.join(fstests_path, "ltp", cmd)

async def fsx_task(config, proc_id):
    directory = conf.get_directory(config)
    fsx_path = ltp_cmd(config, "fsx")
    if conf.dry_run(config):
        print(f"dry_run: fsx: {fsx_path} {proc_id}")
        return False
    fsx_dir = os.path.join(directory, "fsx")
    os.makedirs(fsx_dir, exist_ok=True)
    fsx_file = os.path.join(fsx_dir, f"stress-fsx.{proc_id}")
    name = f"fsx.{proc_id}"
    bad_data = ["READ BAD DATA"]
    args = conf.get_optional_config(config["stressors"], "fsx:args")
    if args is None:
        args = []
    fsx_args = ["-X", fsx_file, *args]
    proc = asyncio.subprocess.create_subprocess_exec(
        fsx_path, *fsx_args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    return await core.cancellable_proc(name, proc, grep=bad_data)

async def fsstress_task(config, proc_id):
    directory = conf.get_directory(config)
    fsstress_path = ltp_cmd(config, "fsstress")
    fsstress_dir = os.path.join(directory, "fsstress")
    args = conf.get(config["stressors"], "fsstress:args")
    fsstress_args = ["-d", fsstress_dir, "-w", "-l", "0", *args]
    if conf.dry_run(config):
        print(f"dry_run: fsstress: {fsstress_path} {fsstress_args}")
        return None
    name = f"fsstress.{proc_id}"
    proc = asyncio.subprocess.create_subprocess_exec(
        fsstress_path, *fsstress_args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    return await core.cancellable_proc(name, proc)

async def btrfs_balance(config, proc_id):
    directory = conf.get_directory(config)
    btrfs = conf.get_btrfs_util(config)
    args = conf.get(config["stressors"], "btrfs_balance:args")
    balance_args = ["balance", "start", *args, directory]
    if conf.dry_run(config):
        print(f"dry_run: balance: {btrfs} {balance_args}")
        return None
    proc = asyncio.create_subprocess_exec(btrfs, *balance_args, stdout = asyncio.subprocess.PIPE)
    name = f"balance.{proc_id}"
    proc = await core.cancellable_proc(name, proc, allow_exit=True)
    if proc and proc.returncode != 0:
        _, stderr = await proc.communicate()
        if stderr:
            stderr = stderr.decode("ascii", "ignore")
        raise core.ValidationFailure(f"balance failed: {proc.returncode} {stderr}")

async def btrfs_balance_task(config, proc_id):
    await core.loopy(btrfs_balance, config, proc_id)

def pick_random_file(d):
    fs = os.listdir(d)
    if not fs:
        return None
    while True:
        f = random.choice(fs)
        if "reflink.tgt." in f:
            continue
        if "fsxgood" in f or "fsxlog" in f:
            continue
        break
    f = os.path.join(d, f)
    if os.path.isfile(f):
        return f
    elif os.path.isdir(f):
        return pick_random_file(f)
    return None

async def reflink(config, proc_id):
    directory = conf.get_directory(config)
    if conf.dry_run(config):
        print(f"dry_run: reflink.{proc_id}")
        return None
    src = pick_random_file(directory)
    if not src:
        return
    tgt = os.path.join(directory, f"reflink.tgt.{proc_id}")
    reflink_args = ["--reflink=always", src, tgt]
    await core.run_cmd(f"reflink.{proc_id}", "cp", reflink_args, ignore_err=True)
    await asyncio.sleep(3)
    reflink_rm_args = [tgt]
    await core.run_cmd(f"reflink-rm.{proc_id}", "rm", reflink_rm_args, ignore_err=True)

async def reflink_task(config, proc_id):
    await core.loopy(reflink, config, proc_id)

async def pick_random_subvol(btrfs, d):
    subvols, _ = await core.run_cmd("subvol-list", btrfs, ["subvolume", "list", "-t", d])
    if not subvols:
        return None
    lines = subvols.splitlines()
    if not lines:
        return None
    line = random.choice(lines)
    return line.split()[3]

async def snapshot(config, proc_id):
    directory = conf.get_directory(config)
    btrfs = conf.get_btrfs_util(config)
    if conf.dry_run(config):
        print(f"dry_run: snapshot.{proc_id}")
        return None
    src = await pick_random_subvol(btrfs, directory)
    if not src:
        return
    src = os.path.join(directory, src)
    tgt = os.path.join(directory, f"snap.{proc_id}")
    snap_args = ["subvolume", "snapshot", src, tgt]
    await core.run_cmd(f"snap.{proc_id}", btrfs, snap_args, ignore_err=True)
    await asyncio.sleep(5)
    snap_del_args = ["subvolume", "snapshot", "delete", tgt]
    await core.run_cmd(f"snap-del.{proc_id}", btrfs, snap_del_args, ignore_err=True)

async def snapshot_task(config, proc_id):
    await core.loopy(snapshot, config, proc_id)

async def drop_caches(_, proc_id):
    with open("/proc/sys/vm/drop_caches", "w") as f:
        await core.run_cmd(f"drop_caches.{proc_id}", "echo", ["3"], stdout=f, ignore_err=True)

async def drop_caches_task(_, proc_id):
    await core.loopy(drop_caches, _, proc_id)

def tasks(config, stressor:str):
    procs = conf.get_optional_config(config["stressors"], f"{stressor}:procs", 1)
    task_fn = globals()[f"{stressor}_task"]
    return [task_fn(config, proc_id) for proc_id in range(procs)]
