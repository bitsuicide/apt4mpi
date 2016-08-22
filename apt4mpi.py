#!/usr/bin/env python

import sys
import os
import json
import bash_writer as bash
import mpi_generator as mpi
import subprocess
import stat
import constant as c

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise EnvironmentError("You have to specify the json file")
    else:
        json_path = sys.argv[1]
    json_file = open(json_path, "r")
    json_data = json.load(json_file)
    # create new folders and use it
    new_folder = "{}_api4mpi".format(json_data["job_options"]["job_name"]) 
    dir_cr = False
    i = 0
    while dir_cr == False:
        i += 1
        try:
            os.mkdir(new_folder)
            os.chdir(new_folder)
        except OSError:
            new_folder = "{}_{}_api4mpi".format(json_data["job_options"]["job_name"], i) 
            continue
        dir_cr = True
    os.mkdir(c.OUTPUT_FOLDER)
    # generate new qsub bash file
    sh_script = bash.gen_bash(json_data)
    #print sh_script
    qsub_script = open(c.BASH_FILE, "w")
    qsub_script.write(sh_script)
    qsub_script.close()
    # set execute permission
    perm = os.stat(c.BASH_FILE)
    os.chmod(c.BASH_FILE, perm.st_mode | 0111)
    # generate new mpi4py file
    mpi.gen_mpi(json_data["commands"])
    # execute qsub command
    cmd = "qsub {}".format(c.BASH_FILE)
    try:
        subprocess.call(cmd)
    except OSError:
        print("It is not possible to complete the task: Command error")