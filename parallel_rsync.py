#!/usr/bin/env python3

import os
import sys
import subprocess
import re

USAGE = """Usage:
./parallel_rsync <N> <from> <to>

Documentation:
    <N>             The number of transfers to perform in parallel
    <from>, <to>    The directory to transfer from, or the output location to
                    transfer to. Either can be a local or remote path, though
                    for rsync at least one must be local. Paths are specified
                    as they are to rsync
"""

match_rsync_file = re.compile("([d-][rwx-]+)[ ]+[0-9,]+ \d+\/\d+\/\d+ \d+\:\d+\:\d+ (.*)")

def path_is_remote(path):
    return ":" in path

def get_local_file_list(local_path):
    file_list = []
    if os.path.isdir(local_path):
        for path, dirs, files in os.walk(local_path):
            for f in files:
                filename = os.path.join(path, f)
                file_list.append(filename)
    else:
        file_list.append(local_path)

    # Change all paths to be relative to local path
    for i in range(len(file_list)):
        file_list[i] = os.path.relpath(file_list[i], local_path)
    return file_list

def resolve_remote_regex(remote_info):
    file_list = []
    path = os.path.dirname(remote_info[1]) + "/"
    print(path)
    result = subprocess.run(["rsync", "-s", f"{remote_info[0]}:{path}"], capture_output=True)
    if result.stderr:
        print(f"Error listing remote path: {result.stderr.decode('utf8')}")
        return file_list

    # Just handling * expansion
    pattern = re.sub("\\\\\*", ".*", re.escape(os.path.basename(remote_info[1])))
    print(f"Mathing pattern {pattern}")
    match_pattern = re.compile(pattern)
    stdout = result.stdout.decode("utf8")
    for m in match_rsync_file.finditer(stdout):
        f = m.group(2)
        if f == ".":
            continue

        if match_pattern.match(f):
            print(f"Matched {f}")
            file_list.append(os.path.join(path, f) + "/")
    return file_list

def get_remote_file_list(rem_path):
    remote_info = rem_path.split(":")
    remote_paths = []
    if "*" in remote_info[1]:
        remote_paths = resolve_remote_regex(remote_info)
    else:
        remote_paths = [remote_info[1]]
    print(f"remote paths: {remote_paths}")
    file_list = []
    while len(remote_paths) > 0:
        path = remote_paths.pop(0)
        print(f"{remote_info[0]}:{path}")
        result = subprocess.run(["rsync", "-s", f"{remote_info[0]}:{path}"], capture_output=True)
        if result.stderr:
            print(f"Error listing remote path: {result.stderr.decode('utf8')}")
            continue

        stdout = result.stdout.decode("utf8")
        for m in match_rsync_file.finditer(stdout):
            is_dir = m.group(1)[0] == "d"
            f = m.group(2)
            if f == ".":
                continue

            print(f)
            if is_dir:
                if path != f:
                    remote_paths.append(os.path.join(path, f) + "/")
                else:
                    remote_paths.append(f + "/")
            else:
                file_list.append(os.path.join(path, f))

    # Change all paths to be relative to the original remote path
    if not "*" in remote_info[1]:
        for i in range(len(file_list)):
            file_list[i] = os.path.relpath(file_list[i], remote_info[1])
    print(file_list)
    return file_list

def get_file_list(path):
    if path_is_remote(path):
        return get_remote_file_list(path)
    else:
        return get_local_file_list(path)

def make_file_path(target, file_path):
    # TODO: Needs to handle correcting things for expansion/wildcards
    # the dir mathcing the pattern target should be used as the base dir 
    if path_is_remote(target):
        remote_info = target.split(":")
        return remote_info[0] + ":" + os.path.join(remote_info[1], file_path)
    else:
        return os.path.join(target, file_path)

class ActiveTransfer:
    def __init__(self, from_path, to_path):
        print(f"Will transfer {from_path} to {to_path}")

        self.from_path = from_path
        self.to_path = to_path
        self.pipe_read, self.pipe_write = os.pipe()
        self.proc = subprocess.Popen(["rsync", "-avsP", self.from_path, self.to_path],
                stdout=self.pipe_write, stderr=subprocess.STDOUT)

        self.stdout = os.fdopen(self.pipe_read)
        self.complete = False

    def progress(self):
        if self.complete:
            return (100, None)

        line = self.stdout.readline()
        if not line or "total size" in line:
            status = self.proc.wait()
            self.complete = True
            os.close(self.pipe_read)
            os.close(self.pipe_write)
            return (100, None)
        
        m = re.search("(\d+)\%", line)
        if m:
            return (int(m.group(1)), line)
        return (0, None)

# Monitor the progress of the current transfers,
# returning when it's possible to enqueue another one
def monitor_progress(n_parallel, transfers):
    completed = 0
    while True:
        for t in transfers:
            progress = t.progress()
            print(f"Transfer '{t.from_path} -> {t.to_path}: {progress[0]}%")
            if progress[0] > 0 and progress[0] != 100:
                print(f"\t{progress[1]}")
            if t.complete:
                completed += 1
        
        transfers = [t for t in transfers if not t.complete]
        if len(transfers) < n_parallel:
            break
    return (transfers, completed)

n_parallel = int(sys.argv[1])
arg_from = sys.argv[2]
arg_to = sys.argv[3]

files = get_file_list(arg_from)
completed = 0
transfers = []
for f in files:
    from_path = make_file_path(arg_from, f)
    to_path = os.path.dirname(make_file_path(arg_to, f)) + "/"
    transfers.append(ActiveTransfer(from_path, to_path))
    transfers, new_completed = monitor_progress(n_parallel, transfers)
    completed += new_completed
    print(f"Completed {completed}/{len(files)}")

while len(transfers) > 0:
    transfers, new_completed = monitor_progress(n_parallel, transfers)
    completed += new_completed
    print(f"Completed {completed}/{len(files)}")

