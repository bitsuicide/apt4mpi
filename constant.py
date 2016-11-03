""" Shared constant """

BASH_FILE = "launch_job.sh"
MPI_FILE = "mpi_parallel.py"
DS_FILE = "dag.py"
SERIAL_FILE = "data_start.p"
INPUT_FILE = "input.yml"
PBS_PREFIX = "#PBS"
MODULE_PREFIX = "module load"
MPI_PREFIX = "time mpirun"
BASH = "#!/bin/bash"
DATA_DIR = "$PBS_O_WORKDIR" 
WARNING_PREFIX = "[WARNING]"
ERROR_PREFIX = "[ERROR]"
IO_MARK = "#"
INPUT_SUB = "#input"
OUTPUT_SUB = "#output"
START_INPUT = "#i"
START_OUTPUT = "#o"
START_ROUT = "#rout"
START_RERR = "#rerr"
REGEX_SYM = ["^", "{", "}", "*", "(", ")", "[", "]", "+", "*", "<", ">", "&"]