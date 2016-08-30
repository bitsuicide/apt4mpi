import json
import os
import constant as c

def gen_pbs(json_opt):
    """ Generate pbs bash script section """
    sh_temp = ""
    chunks = memory = mpi_proc = 0
    ncpus = 1
    for opt in json_opt:
        if json_opt[opt] != "":
            if opt == "account":
                sh_temp += "{} {} <{}>\n".format(c.PBS_PREFIX, "-A", json_opt[opt])
            elif opt == "error_log_path":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-e", json_opt[opt])
            elif opt == "output_log_path":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-o", json_opt[opt])
            elif opt == "queue":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-q", json_opt[opt])
            elif opt == "job_name":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-N", json_opt[opt])
            elif opt == "walltime":
                sh_temp += "{} {} walltime={}\n".format(c.PBS_PREFIX, "-l", json_opt[opt])
            elif opt == "chunks":
                chunks = json_opt[opt]
            elif opt == "memory":
                memory = json_opt[opt]
            elif opt == "mpi_procs":
                mpi_proc = json_opt[opt]
            elif opt == "ncpus":
                ncpus = json_opt[opt]
    if chunks != 0 and memory != 0 and mpi_proc != 0:
        sh_temp += "{} {} select={}:ncpus={}:mpiprocs={}:mem={}\n\n".format(c.PBS_PREFIX, "-l", chunks, ncpus, mpi_proc, memory)
    return sh_temp

def gen_module(json_mod):
    """ Generate module bash script section """
    sh_temp = ""
    for mod in json_mod:
        autoload = ""
        if "autoload" in mod and mod["autoload"] != "False":
            autoload = " autoload"
        sh_temp += "{}{} {}/{} \n".format(c.MODULE_PREFIX, autoload, mod["name"], mod["version"]) 
    return sh_temp

def gen_mpicmd():
    """ Generate mpirun command """
    sh_temp = "\necho \"START:\"`date`\n"
    sh_temp += "{} {}/{} \n".format(c.MPI_PREFIX, os.getcwd(), c.MPI_FILE)
    sh_temp += "echo \"END:\"`date`\n"
    return sh_temp

def gen_bash(json):
    """ Generate all the stuff for the bash file """
    sh_script = "{}\n".format(c.BASH)
    sh_script += gen_pbs(json["job_options"])
    sh_script += "cd {}\n\n".format(c.DATA_DIR)
    if "modules" in json: # modules is not mandatory
        sh_script += gen_module(json["modules"])
    sh_script += gen_mpicmd()
    return sh_script

