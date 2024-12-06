import os
import subprocess
import tomllib

class InvalidConfig(Exception):
    pass

def load_config(fname):
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
    if not "stressors" in config:
        raise InvalidConfig("no stressors configured")
    print(f"validate config: {config}")
    _ = get_mandatory_config(config, "stressors")
    fsx = get_optional_config(config, "stressors:fsx")
    fsstress = get_optional_config(config, "stressors:fsstress")
    if not fsx and not fsstress:
        raise InvalidConfig("no fs stressors (fsx/fsstress) configured")
    return True

def test_optional_config(section, field):
    return field in section and section[field]

def parse_selector(selector):
    spl = selector.split(":")
    field = None
    section = spl[0]
    if len(spl) == 2:
        field = spl[1]
    return section, field

# TODO fully recursive
# TODO ":" -> "."
# can both be done with some kind of jq lib?
# it can! "glom"!
def get(config, selector):
    section, field = parse_selector(selector)
    if not field:
        return config[section]
    return config[section][field]

def get_optional_config(config, selector, default=None):
    section, field = parse_selector(selector)
    if section not in config:
        return default
    if field not in config[section]:
        return default
    return config[section][field]

def get_mandatory_config(config, selector):
    section, field = parse_selector(selector)
    if section not in config:
        raise InvalidConfig("Missing section {section}")
    if not field:
        return config[section]
    if field not in config[section]:
        raise InvalidConfig("Missing field {section}:{field}")
    return config[section][field]

def get_directory(config):
    return get_mandatory_config(config, "global:directory")

def get_btrfs_util(config):
    path = ""
    progs = get_optional_config(config, "global:btrfs_progs")
    if progs:
        path = progs
    return os.path.join(path, "btrfs")

def dbg(config, msg):
    if get_optional_config(config, "global:debug"):
        print(msg)

def dry_run(config):
    return get_optional_config(config, "global:dry_run")

def get_stressors(config):
    return config["stressors"]
