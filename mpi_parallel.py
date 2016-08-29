#!/usr/bin/env python

import tree
import pickle
from mpi4py import MPI
import subprocess
import os

comm = MPI.COMM_WORLD   # get MPI communicator object
size = comm.size        # total number of processes
rank = comm.rank        # rank of this process
status = MPI.Status()   # get MPI status object

def dispatch(p_status):
    """ Check if there is a free process and send the job to do """
    for i in range(len(p_status)):
        if p_status[i] == False:
            # send a job
            # get a new job to do
            job = None
            comm.send(job, dest=i+1, tag=tags.START) 
            p_status[i] = True

def stop_comp():
    """ Kill all process """
    for i in range(size-1):
        comm.send(None, dest=i+1, tag=tags.EXIT)

def enum(*sequential, **named):
    """ Simulate enum in python """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

tags = enum("DONE", "EXIT", "START")

if __name__ == '__main__':
    if rank == 0: # master 
        print("I'm the master")
        #data = pickle.load(open("./custom_pipeline_api4mpi/test.p", "rb"))
        p_status = [False for i in range(size - 1)]
        dispatch(p_status)
        while True:
            msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            if tag == tags.DONE:
                p_status[source-1] = False
                print("A new msg from {} with tag {} and value {}".format(source, tag, msg))
                print p_status
                dispatch(p_status)
        stop_comp()
    else: # slaves
        print("I'm the process " + str(rank))
        while True:
            msg = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()
            
            if tag == tags.START: # there is a new job to do
                cmd = "sleep {}".format(rank*3)
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
                proc.wait()
                comm.send(None, dest=0, tag=tags.DONE)
            elif tag == tags.EXIT:
                break

        comm.send(None, dest=0, tag=tags.EXIT)