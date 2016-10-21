#!/usr/bin/env python

import yaml
import constant as c

def check_regex(str):
    """ Check if a string contains regex special characters """
    for sym in c.REGEX_SYM:
        if str.find(sym) != -1:
            return True
    return False

def check_yn(answer):
    """ Check if the answer is yes or no or an error """
    answer.lower()
    if answer == "y":
        return True
    elif answer == "n":
        return False
    else:
        print("You didn't answer or you answer is not correct! Will be used the false value.\n")
        return False

def join_opt(opt_l, start, end):
    """ Concatenate the job options all together """
    opt_val = ""
    for i in range(start, end):
        opt_val += opt_l[i] + " "
    return opt_val.strip()

def comm_analyze(comm):
    """ Take the command line and return execute + options """
    comm_split = comm.split()
    opt = []
    io = False
    i = 0
    start_io_l = [(0, "")]
    end_io = -1
    redirect = []
    for s in comm_split:
        if s.find(c.IO_MARK) != -1: # there is at least one io marker
            io = True
            s = s.lower().strip()
            if s == c.INPUT_SUB or s == c.OUTPUT_SUB:
                if end_io == -1:
                    end = 0
                else:
                    end = end_io
                value = join_opt(comm_split, end + 1, i)
                if value: 
                    opt.append({"value":value})
                if s == c.INPUT_SUB:
                    opt.append({"type":"input"})
                else:
                    opt.append({"type":"output"})
                end_io = i
            elif (s == c.START_INPUT or s == c.START_OUTPUT 
                    or s == c.START_RIN or s == c.START_ROUT or s == c.START_RERR): # io start 
                start_io_l.append((i, s))
            elif s == c.IO_MARK: # io end
                start_io = start_io_l[-1]
                if start_io[0] != 0:
                    last_start_io = start_io_l[-2]
                    if start_io[1] == c.START_INPUT:
                        io_type = "input"
                    elif start_io[1] == c.START_OUTPUT:
                        io_type = "output"
                    elif start_io[1] == c.START_ROUT:
                        io_type = "stdout"
                    elif start_io[1] == c.START_RERR:
                        io_type = "stderr"    
                    if end_io == -1:
                        end = 0
                    else:
                        end = end_io
                    value = join_opt(comm_split, end + 1, start_io[0])
                    if value:
                        opt.append({"value":value}) # before
                    path = join_opt(comm_split, start_io[0] + 1, i)
                    if io_type == "input" or io_type == "output":
                        if check_regex(path):
                            opt.append({"type":io_type, "regex":path}) # io
                        else:
                            opt.append({"type":io_type, "value":path}) # io
                    else:
                        redirect.append({"type":io_type, "value":path})
                else:
                    raise AttributeError("It's not possibile to parse the io marker")
                end_io = i
        i += 1
    if not io: # only one value element
        opt.append({"value":join_opt(comm_split, 0, len(comm_split))})
    elif end_io != len(comm_split) - 1: # after
        opt.append({"value":join_opt(comm_split, end_io + 1, len(comm_split))})
    return comm_split[0], opt, redirect

if __name__ == "__main__":
    yaml_file = open(c.INPUT_FILE, "w")
    data = {}
    print("**********************************\n")
    print("Input generator for apt4mpi\n")
    print("**********************************\n")
    print("Modules declaration")
    modules = []
    i = 1
    answ = raw_input("Do you want to load some modules (y/n)? ")
    if check_yn(answ) == True:
        while(True):
            print("\nModule {}\n".format(i))
            mod_name = raw_input("Insert the module name: ")
            if not mod_name:
                print("You must insert the module name \n")
                continue
            mod_vers = raw_input("Insert the module version: ")
            if not mod_vers:
                print("You didn't choose a version. The default version will be used\n")
            answ = raw_input("Do you want apply the autoload option (y/n)? ")
            mod_auto = check_yn(answ)
            if not mod_vers:
                mod = {"name": mod_name, "autoload": mod_auto}
            else: 
                mod = {"name": mod_name, "version": mod_vers, "autoload": mod_auto}
            modules.append(mod)
            answ = raw_input("Do you want to load another module (y/n)? ")
            print mod_name, mod_vers, mod_auto
            if check_yn(answ) == False:
                break
            i += 1
        data["modules"] = modules
    print("\nCommands declaration")
    commands = []
    i = 1
    while True:
        print("Command {}\n".format(i))
        comm_id = raw_input("Insert the executable id: ")
        if not comm_id:
            print("You didn't choose an id for the executable and will be autogenerated." + 
                "Be caution that with the dependeces can be a problem! \n")
        comm_branch = raw_input("Insert the number of processes: ")
        if not comm_branch:
            print("You didn't choose the number of processes. If it is possibile will be automatically computed or set to 1\n")
        comm_after = raw_input("Insert dependences (zero, one or more separated by a space): ")
        comm_after = comm_after.split()
        comm_exec = raw_input("Insert the full command: ")
        comm_exec, comm_opt, comm_redirect = comm_analyze(comm_exec)
        command = {"id": comm_id, "executable": comm_exec}
        if comm_after:
            command["after"] = comm_after
        if comm_branch:
            command["branching_factor"] = comm_branch
        if comm_redirect:
            command["redirect"] = comm_redirect
        if comm_opt:
            command["options"] = comm_opt
        commands.append(command)
        answ = raw_input("Do you want to load another module (y/n)? ")
        print command
        if check_yn(answ) == False:
            break
        i += 1
    data["commands"] = commands
    print("\nJob options \n")
    job_options = {}
    job_mpiproc = raw_input("Insert the number of MPI PROCESSORS: ")
    if not job_mpiproc:
        print("You didn't choose the number of processors. Will be used 1.\n")
        job_mpiproc = "1"
    job_options["mpi_proc"] = job_mpiproc
    job_ncpu = raw_input("Insert the number of CPU: ")
    if not job_ncpu:
        print("You didn't choose the number of cpu. Will be used 2.\n")
        job_ncpu = "2"
    job_options["ncpus"] = job_ncpu
    job_chunks = raw_input("Insert the number of CHUNKS: ")
    if not job_chunks:
        print("You didn't choose the number of chunks. Will be used 2.\n")
        job_chunks = "2"
    job_options["chunks"] = job_chunks
    job_mem = raw_input("Insert the memory dimension (ex. 1GB/100MB): ")
    if not job_mem:
        print("You didn't choose the memory dimension. Will be used 100MB.\n")
        job_mem = "100MB"
    job_options["memory"] = job_mem
    job_walltime = raw_input("Insert the walltime (ex. 1:00:00): ")
    if not job_walltime:
        print("You didn't choose the walltime. Will be used 0:01:00.\n")
        job_walltime = "0:01:00"
    job_options["walltime"] = job_walltime
    job_queue = raw_input("Insert the queue name: ")
    job_options["queue"] = job_queue
    job_account = raw_input("Insert the account: ")
    job_options["account"] = job_account
    job_name = raw_input("Insert the job name: ")
    if not job_name:
        print("You didn't choose the job name. Will be used paralleljob.\n")
        job_name = "paralleljob"
    job_options["job_name"] = job_name
    job_error = raw_input("Insert the error path: ")
    job_options["error_log_path"] = job_error
    job_output = raw_input("Insert the output path: ")
    job_options["output_log_path"] = job_output
    print job_mpiproc, job_ncpu, job_chunks, job_mem, job_walltime, job_queue, job_account, job_name, job_error, job_output
    data["job_options"] = job_options
    yaml.dump(data, yaml_file, default_flow_style=False)
    yaml_file.close()