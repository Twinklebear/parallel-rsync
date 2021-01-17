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

def split_target_path(path):
    if path_is_remote(path):
        split = path.split(":")
        return [split[0] + ":", split[1]]
    return ["", path]

def resolve_regex(remote_info):
    file_list = []
    path = os.path.dirname(remote_info[1]) + "/"
    result = subprocess.run(["rsync", "-s", f"{remote_info[0]}{path}"], capture_output=True)
    if result.stderr:
        print(f"Error listing remote path: {result.stderr.decode('utf8')}")
        return file_list

    # Just handling * expansion
    print(os.path.basename(remote_info[1]))
    pattern = re.sub("\\\\\*", ".*", re.escape(os.path.basename(remote_info[1])))
    print(f"Mathing pattern {pattern}")
    match_pattern = re.compile(pattern)
    stdout = result.stdout.decode("utf8")
    for m in match_rsync_file.finditer(stdout):
        f = m.group(2)
        if f == ".":
            continue

        if match_pattern.match(f):
            file_list.append(os.path.join(path, f) + "/")
    return file_list

def get_file_list(rem_path):
    remote_info = split_target_path(rem_path)
    remote_paths = []
    remote_base_path = ""
    if "*" in remote_info[1]:
        remote_paths = resolve_regex(remote_info)
    else:
        remote_paths = [remote_info[1] + "/"]

    remote_base_path = os.path.dirname(remote_paths[0][0:-1])

    print(f"remote base path = {remote_base_path}")
    print(f"remote paths: {remote_paths}")
    file_list = []
    while len(remote_paths) > 0:
        path = remote_paths.pop(0)
        result = subprocess.run(["rsync", "-s", f"{remote_info[0]}{path}"], capture_output=True)
        if result.stderr:
            print(f"Error listing remote path: {result.stderr.decode('utf8')}")
            continue

        stdout = result.stdout.decode("utf8")
        for m in match_rsync_file.finditer(stdout):
            is_dir = m.group(1)[0] == "d"
            f = m.group(2)
            if f == ".":
                continue

            if is_dir:
                if path != f:
                    remote_paths.append(os.path.join(path, f) + "/")
                else:
                    remote_paths.append(f + "/")
            else:
                file_list.append(os.path.join(path, f))

    # Change all paths to be relative to the original remote path
    for i in range(len(file_list)):
        file_list[i] = os.path.relpath(file_list[i], remote_base_path)
    return file_list

class ActiveTransfer:
    def __init__(self, from_path, to_path):
        self.from_path = from_path
        self.to_path = to_path
        print(f"Transfer '{from_path} -> {to_path}: starting")

        self.pipe_read, self.pipe_write = os.pipe()
        if path_is_remote(self.to_path):
            remote_info = split_target_path(self.to_path)
            host = remote_info[0][0:-1]
            to_dir = os.path.dirname(remote_info[1])
            subprocess.run(["ssh", host, "mkdir", "-p", f'"{to_dir}"'])
        else:
            os.makedirs(os.path.dirname(self.to_path), exist_ok=True)

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
            if progress[0] > 0:
                print(f"Transfer '{t.from_path} -> {t.to_path}: {progress[0]}%")
                if progress[0] != 100:
                    print(f"\t{progress[1]}")
            if t.complete:
                completed += 1
        
        transfers = [t for t in transfers if not t.complete]
        if len(transfers) < n_parallel or n_parallel <= 0:
            break
    return (transfers, completed)

n_parallel = int(sys.argv[1])
arg_from = sys.argv[2]
if arg_from[-1] == "/":
    arg_from = arg_from[0:-1]
arg_to = sys.argv[3]

files = get_file_list(arg_from)
completed = 0
transfers = []
for f in files:
    remote_info = split_target_path(arg_from)
    remote_base_path = os.path.dirname(remote_info[1][0:-1])
    from_path = remote_info[0] + os.path.join(remote_base_path, f)

    to_path = os.path.join(arg_to, f)

    transfers.append(ActiveTransfer(from_path, to_path))
    while len(transfers) == n_parallel:
        transfers, new_completed = monitor_progress(n_parallel, transfers)
        completed += new_completed
        print(f"Completed {completed}/{len(files)}")

while len(transfers) > 0:
    transfers, new_completed = monitor_progress(n_parallel, transfers)
    completed += new_completed
    print(f"Completed {completed}/{len(files)}")

