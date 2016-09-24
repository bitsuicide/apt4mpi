import yaml
import os
import constant as c

def gen_pbs(yaml_opt):
    """ Generate pbs bash script section """
    sh_temp = ""
    chunks = memory = mpi_proc = 0
    ncpus = 1
    for opt in yaml_opt:
        if yaml_opt[opt] != "":
            if opt == "account":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-A", yaml_opt[opt])
            elif opt == "error_log_path":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-e", yaml_opt[opt])
            elif opt == "output_log_path":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-o", yaml_opt[opt])
            elif opt == "queue":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-q", yaml_opt[opt])
            elif opt == "job_name":
                sh_temp += "{} {} {}\n".format(c.PBS_PREFIX, "-N", yaml_opt[opt])
            elif opt == "walltime":
                sh_temp += "{} {} walltime={}\n".format(c.PBS_PREFIX, "-l", yaml_opt[opt])
            elif opt == "chunks":
                chunks = yaml_opt[opt]
            elif opt == "memory":
                memory = yaml_opt[opt]
            elif opt == "mpi_procs":
                mpi_proc = yaml_opt[opt]
            elif opt == "ncpus":
                ncpus = yaml_opt[opt]
    if chunks != 0 and memory != 0 and mpi_proc != 0:
        sh_temp += "{} {} select={}:ncpus={}:mpiprocs={}:mem={}\n\n".format(c.PBS_PREFIX, "-l", chunks, ncpus, mpi_proc, memory)
    return sh_temp

def gen_module(yaml_mod):
    """ Generate module bash script section """
    sh_temp = ""
    for mod in yaml_mod:
        autoload = ""
        if "autoload" in mod and mod["autoload"] != "False":
            autoload = " autoload"
        if "version" not in mod:
            sh_temp += "{}{} {} \n".format(c.MODULE_PREFIX, autoload, mod["name"])
        else:
            sh_temp += "{}{} {}/{} \n".format(c.MODULE_PREFIX, autoload, mod["name"], mod["version"]) 
    return sh_temp

def gen_mpicmd():
    """ Generate mpirun command """
    sh_temp = "\necho \"START:\"`date`\n"
    sh_temp += "{} {}/{} \n".format(c.MPI_PREFIX, os.getcwd(), c.MPI_FILE)
    sh_temp += "echo \"END:\"`date`\n"
    return sh_temp

def gen_bash(yaml):
    """ Generate all the stuff for the bash file """
    sh_script = "{}\n".format(c.BASH)
    sh_script += gen_pbs(yaml["job_options"])
    sh_script += "cd {}\n\n".format(c.DATA_DIR)
    # default modules
    sh_script += "module load profile/advanced\n"
    sh_script += "module load autoload python/2.7.9\n"
    sh_script += "module load autoload intelmpi\n"
    sh_script += "module load autoload mpi4py\n"
    if "modules" in yaml: # modules is not mandatory
        sh_script += gen_module(yaml["modules"])
    sh_script += gen_mpicmd()
    return sh_script

