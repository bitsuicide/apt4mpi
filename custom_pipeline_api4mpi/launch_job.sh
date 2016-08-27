#!/bin/bash
#PBS -A <cin_staff>
#PBS -q serial
#PBS -N custom_pipeline
#PBS -l walltime=01:30:00
#PBS -l select=10:ncpus=2:mpiprocs=5:mem=5GB

cd $PBS_O_WORKDIR

module load autoload python/2.7.9 
module load hisat2/2.0.4 
module load bowtie/1.1.2 

echo "START:"`date`
time mpirun /Users/bitsuicide/Documents/Develop/git/apt4mpi/custom_pipeline_api4mpi/mpi_parallel.py 
echo "END:"`date`
