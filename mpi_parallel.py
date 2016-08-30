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

def dispatch(data, node_status):
    """ Check if there is a free comp node and send the job to do """
    for i in range(len(node_status)):
        if node_status[i] == False:
            job = next_job(data, i+1)
            if job != None: # is available a new job to do
                comm.send(job, dest=i+1, tag=tags.START) 
                node_status[i] = True

def next_job(data, rank):
    """ Return next job ready to execute """
    for j in data.cue:
        process = data.nodes[j[0]]
        status = j[1]
        if process.is_ready() and status == -1:
            j[1] = rank
            return process
    return None # no job ready to execute or job finished

def remove_job(data, job):
    """ Remove a job when is completed """
    for j in data.cue:
        if job.proc_id == j:
            del j
            return 

def stop_comp():
    """ Kill all process """
    for i in range(size-1):
        comm.send(None, dest=i+1, tag=tags.EXIT)

def gen_command(process):
    """ Generate the process command from the options """
    cmd = "{} ".format(process.name)
    for o in process.options.opt_list:
        opt = ""
        for el in o:
            print el
            if el and el != "input" and el != "output":
                opt += "{} ".format(el)
        cmd += opt
    return cmd

def enum(*sequential, **named):
    """ Simulate enum in python """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

tags = enum("DONE", "EXIT", "START")

if __name__ == '__main__':
    if rank == 0: # master 
        print("I'm the master")
        #data = pickle.load(open("./custom_pipeline_api4mpi/data.p", "rb"))
        data = pickle.load(open("data.p", "rb"))
        node_status = [False for i in range(size - 1)]
        dispatch(data, node_status)
        n_job = len(data.cue) # total job to do
        while n_job >= 1:
            msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            if tag == tags.DONE: # job executed
                node_status[source-1] = False
                print("A new msg from {} with tag {} and value {}".format(source, tag, msg))
                j_executed = data.nodes[msg]
                j_executed.status = True
                remove_job(data, j_executed)
                n_job -= 1
                dispatch(data, node_status)
        stop_comp()
    else: # slaves
        print("I'm the node " + str(rank))
        while True:
            job = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()  
            if tag == tags.START: # there is a new job to do
                cmd = "sleep {}".format(rank*3)
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
                proc.wait()
                print("Node {}: {}".format(rank, job.proc_id))
                print gen_command(job)
                comm.send(job.proc_id, dest=0, tag=tags.DONE)
            elif tag == tags.EXIT:
                break