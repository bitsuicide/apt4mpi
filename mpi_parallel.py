#!/usr/bin/env python

import dag
import pickle
from mpi4py import MPI
import subprocess
import os
import glob
import datetime

comm = MPI.COMM_WORLD   # get MPI communicator object
size = comm.size        # total number of processes
rank = comm.rank        # rank of this process
status = MPI.Status()   # get MPI status object
log_file = "log.txt"

def find_file(regex):
    """ Find all files that match with the regex """
    f_list = glob.glob(regex)
    if len(f_list) == 1:
        return f_list[0]
    elif len(f_list) > 1:
        return f_list
    else:
        raise Exception("[ERROR] There is no valid output file for this job")

def dispatch(data, node_status, log):
    """ Check if there is a free comp node and send the job to do """
    for i in range(len(node_status)):
        if node_status[i] == False:
            job = next_job(data, i+1)
            if job != None: # is available a new job to do
                comm.send(job, dest=i+1, tag=tags.START) 
                log.write("[START]\t{}\n".format(job.proc_id))
                log.flush()
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
        i = 0
        opt = ""
        for el in o: 
            if el and el != "input" and el != "output" and i != 3:
                opt += str(el)
                if el[-1] != "=" and el[-1] != "'": # command without space
                    opt += " " # space
            i += 1
        cmd += opt
    return cmd

def enum(*sequential, **named):
    """ Simulate enum in python """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

tags = enum("DONE", "EXIT", "START")

if __name__ == '__main__':
    if rank == 0: # master 
        log = open(log_file, "w")
        current_t = datetime.datetime.now()
        log.write("{}\n".format(current_t))
        data = pickle.load(open("data.p", "rb"))
        node_status = [False for i in range(size - 1)]
        dispatch(data, node_status, log)
        n_job = len(data.cue) # total job to do
        while n_job >= 1:
            msg, exit_code = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            if tag == tags.DONE: # job executed
                node_status[source-1] = False
                print("A new msg from {} with tag {} and value {}".format(source, tag, msg))
                j_executed = data.nodes[msg]
                if exit_code == 0:
                    status = "DONE"
                else:
                    status = "ERROR"
                log.write("[{}]\t{}\n".format(status, j_executed.proc_id))
                log.flush()
                # regex handler
                i = 0
                more_out = False
                for s in j_executed.son:
                    job_opt = s.options
                    if job_opt.regex[0]: # the job has a regex
                        for r in job_opt.regex[1]:
                            option = job_opt.opt_list[r]
                            if option[2] == None: # not expanded
                                file_list = find_file(option[3]) # find file that match with the regex
                                lenght_fl = len(file_list)
                                if lenght_fl >= len(j_executed.son):
                                    new_io = True
                                    while new_io:  
                                        new_io = job_opt.set_io_option(option[0], file_list[i]) # set the result
                                        if new_io == True:
                                            i += 1
                                    if lenght_fl > len(j_executed.son) and more_out != True:
                                        more_out = True
                                else:
                                    raise Exception("[ERROR] The process {} has less output than sons\n".format(j.executed.proc_id))
                if more_out:
                    print("[WARNING] The process {} has more output than sons input\n".format(j_executed.proc_id))
                j_executed.status = True
                remove_job(data, j_executed)
                n_job -= 1
                dispatch(data, node_status, log)
        log.write("[FINISH]\n")
        current_t = datetime.datetime.now()
        log.write("{}\n".format(current_t))
        log.close()
        stop_comp()
    else: # slaves
        #print("I'm the node " + str(rank))
        while True:
            job = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()  
            if tag == tags.START: # there is a new job to do
                cmd = gen_command(job)
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                exit_code = proc.wait()
                print cmd
                print("Error: {}".format(proc.stderr.read()))
                # redirect handler
                redirect = job.options.redirect
                if redirect: # it is a redirect process 
                    for r in redirect: # writing the output or/and the error file
                        file = open(r[1], "w")  
                        if r[0] == "stdout":
                            out = proc.stdout.read()
                        elif r[0] == "stderr":
                            out = proc.stderr.read()
                        elif r[0] == "stdout|stderr":
                            out = proc.stdout.read() + proc.stderr.read()
                        elif r[0] == "stderr|stdout":
                            out = proc.stderr.read() + proc.stdout.read()
                        file.write(out)
                        file.close()
                print("Node {}: {}".format(rank, job.proc_id))
                comm.send((job.proc_id, exit_code), dest=0, tag=tags.DONE)
            elif tag == tags.EXIT:
                break