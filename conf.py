import os
import subprocess
import tomllib

class InvalidConfig(Exception):
    pass

def get_config(fname):
    with open(fname, "rb") as f:
        config = tomllib.load(f)
    return config

# Verify that the directory:
# - is a btrfs mount
def validate_directory(directory):
    findmnt = subprocess.run(["findmnt", "-n", "-t", "btrfs", directory], capture_output=True)
    if findmnt.returncode != 0:
        raise InvalidConfig("global: directory: invalid directory")
    return True

def validate_global_section(global_section):
    if "fstests" not in global_section:
        raise InvalidConfig("global: no path to fstests")
    if "directory" not in global_section:
        raise InvalidConfig("global: no directory")
    if "duration" not in global_section:
        raise InvalidConfig("global: no duration")
    validate_directory(global_section["directory"])
    return True

def validate_fsx_section(fsx_section):
    if "procs" not in fsx_section:
        raise InvalidConfig("fsx: no procs")

def validate_fsstress_section(fsstress_section):
    if "procs" not in fsstress_section:
        raise InvalidConfig("fsstress: no procs")

def validate_config(config):
    if "global" not in config:
        raise InvalidConfig("no global section")
    validate_global_section(config["global"])
    if not "fsx" in config and not "fsstress" in config:
        raise InvalidConfig("no fs stressors (fsx/fsstress) configured")
    if "fsx" in config:
        validate_fsx_section(config["fsx"])
    if "fsstress" in config:
        validate_fsstress_section(config["fsstress"])
    return True

def test_optional_config(section, field):
    return field in section and section[field]

def get_optional_config(section, field):
    if field in section:
        return section[field]
    return None

def get_btrfs_util(config):
    path = ""
    if "btrfs" in config:
        progs = get_optional_config(config["btrfs"], "progs")
        if progs:
            path = progs
    return os.path.join(path, "btrfs")
