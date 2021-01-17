#!/usr/bin/env python3

import os
import sys
import subprocess
import re

USAGE = """Usage:
./parallel_rsync <from> <to>

Documentation:
    <from>, <to>    The directory to transfer from, or the output location to
                    transfer to. Either can be a local or remote path, though
                    for rsync at least one must be local. Paths are specified
                    as they are to rsync
"""

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

def get_remote_file_list(rem_path):
    match_file = re.compile("([d-][rwx-]+)[ ]+[0-9,]+ \d+\/\d+\/\d+ \d+\:\d+\:\d+ (.*)")

    remote_info = rem_path.split(":")
    remote_paths = [remote_info[1]]
    file_list = []
    while len(remote_paths) > 0:
        path = remote_paths.pop(0)
        result = subprocess.run(["rsync", "-s", f"{remote_info[0]}:{path}"], capture_output=True)
        if result.stderr:
            print(f"Error listing remote path: {result.stderr.decode('utf8')}")
            continue

        stdout = result.stdout.decode("utf8")
        for m in match_file.finditer(stdout):
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
        file_list[i] = os.path.relpath(file_list[i], remote_info[1])
    return file_list

def get_file_list(path):
    if path_is_remote(path):
        return get_remote_file_list(path)
    else:
        return get_local_file_list(path)

def make_file_path(target, file_path):
    if path_is_remote(target):
        remote_info = target.split(":")
        return remote_info[0] + ":" + os.path.join(remote_info[1], file_path)
    else:
        return os.path.join(target, file_path)

class ActiveTransfer:
    def __init__(self, from_path, to_path):
        print(f"Will transfer {from_path} to {to_path}")

        self.pipe_read, self.pipe_write = os.pipe()
        self.proc = subprocess.Popen(["rsync", "-avsP", from_path, to_path], stdout=self.pipe_write, stderr=subprocess.STDOUT)

        self.stdout = os.fdopen(self.pipe_read)

    def progress(self):
        line = self.stdout.readline()
        if not line or "total size" in line:
            status = self.proc.wait()
            os.close(self.pipe_read)
            os.close(self.pipe_write)
            return (100, None)
        
        print(line)
        m = re.search("(\d+)\%", line)
        if m:
            return (int(m.group(1)), line)
        return (0, None)


files = get_file_list(sys.argv[1])
print(f"Will transfer files: {files}")

for f in files:
    from_path = make_file_path(sys.argv[1], f)
    to_path = os.path.dirname(make_file_path(sys.argv[2], f)) + "/"

    transfer = ActiveTransfer(from_path, to_path)
    while True:
        progress = transfer.progress()
        print(f"progress = {progress[0]}: {progress[1]}")
        if progress[0] == 100:
            break


