#!/usr/bin/env python3
import asyncio

import conf
import core

async def btrfs_balance(config):
    directory = conf.get_directory(config)
    btrfs = conf.get_btrfs_util(config)
    balance_args = ["balance", "start", "-dusage=100", directory]
    if conf.dry_run(config):
        print(f"dry_run: balance: {btrfs} {balance_args}")
        return None
    proc = asyncio.create_subprocess_exec(btrfs, *balance_args, stdout = asyncio.subprocess.PIPE)
    proc = await core.cancellable_proc("balance", proc, allow_exit=True)
    if proc and proc.returncode != 0:
        stderr = await proc.communicate()
        if stderr:
            stderr = stderr.decode("ascii", "ignore")
        raise core.ValidationFailure(f"balance failed: {proc.returncode} {stderr}")

async def main():
    config = conf.load_config("conf.toml")
    conf.validate_config(config)
    stressors = __import__("stressors")
    try:
        async with asyncio.TaskGroup() as tg:
            # stressor tasks
            for stressor in conf.get_stressors(config):
                print(f"getting tasks for stressor {stressor}")
                tasks = stressors.tasks(config, stressor)
                print(f"got {len(tasks)} tasks")
                for task in tasks:
                    tg.create_task(task)
            # track dmesg for errors
            tg.create_task(core.dmesg())
            # dummy task that triggers cancellation on timeout
            tg.create_task(core.run_for_duration(config))
    except ExceptionGroup as eg:
        happy = eg.subgroup(core.Happy)
        validation = eg.subgroup(core.ValidationFailure)
        if happy:
            for exc in happy.exceptions:
                print(f"{exc}")
        elif validation:
            for exc in validation.exceptions:
                print(f"{exc}")
        else:
            print("re-raise!")
            raise
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())
