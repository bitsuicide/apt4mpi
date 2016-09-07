#!/usr/bin/env python

import sys
import os
import shutil
import yaml
import bash_writer as bash
import mpi_generator as mpi
import subprocess
import stat
import constant as c

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise EnvironmentError("You have to specify the yaml file")
    else:
        yaml_path = sys.argv[1]
    yaml_file = open(yaml_path, "r")
    yaml_data = yaml.load(yaml_file)
    job_name = yaml_data["job_options"]["job_name"]
    # create new folders
    new_folder = "{}_api4mpi".format(job_name) 
    dir_cr = False
    i = 0
    while dir_cr == False:
        i += 1
        try:
            os.mkdir(new_folder) # create new dir
            shutil.copy(c.MPI_FILE, new_folder) # copy mpi file to new dir
            shutil.copy(c.DS_FILE, new_folder) # copy data structure to new dir
            os.chdir(new_folder) # move to new dir
        except OSError:
            new_folder = "{}_{}_api4mpi".format(job_name, i) 
            continue
        dir_cr = True
    os.mkdir(c.OUTPUT_FOLDER)
    # generate new qsub bash file
    sh_script = bash.gen_bash(yaml_data)
    qsub_script = open(c.BASH_FILE, "w")
    qsub_script.write(sh_script)
    qsub_script.close()
    # set execute permission
    perm = os.stat(c.BASH_FILE)
    os.chmod(c.BASH_FILE, perm.st_mode | 0111) # chmod +x
    # generate new mpi4py file
    num_proc = int(yaml_data["job_options"]["chunks"]) * int(yaml_data["job_options"]["mpi_procs"])
    mpi.gen_mpi(yaml_data["commands"], num_proc)

    # execute qsub command
    cmd = "qsub {}".format(c.BASH_FILE)
    try:
        subprocess.call(cmd)
    except OSError:
        print("{} Command Error".format(c.ERROR_PREFIX))
