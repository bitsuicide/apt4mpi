import dag 
import constant as c
import glob
import copy
import re

def check_regex(regex, io_type):
    """ Expand the regex if it is needed """
    if regex[0] != "/":
        regex_path = "../" + regex
    else:
        regex_path = regex
    f_list = sorted(glob.glob(regex_path))
    branch_f = 1
    if f_list and len(f_list) > 1 and io_type == "input":
        branch_f = len(f_list)
    group = regex_group(regex, f_list)
    return f_list, branch_f, group

def regex_group(regex, result):
    """ Build group list """
    group_n = len([m.start() for m in re.finditer("\*", regex)])
    regex = regex.replace("*", "(.*)", group_n)
    find = re.compile(regex)
    group = [[] for i in range(0, group_n)]
    for r in result:
        for i in range(0, group_n):
            res = find.search(r)
            group[i].append(res.group(i+1))
    return group

def replace_group(regex, ind, group):
    """ Find and replace regex groups """
    i = 0
    for g in group:
        i += 1
        if g:
            break
    if i == len(group): # check if it is possible use the group
        return None
    res = re.findall("\$[0-9]", regex)
    if res:
        path_temp = regex
        path = ""
        for r in res:
            path = re.sub(r"\$[0-9]", group[int(r[1])][ind], path_temp, 1)
            path_temp = path
        return path
    return None

def build_dag(json):
    """ Build three / graph process dependences """ 
    nodes = {}
    root_list = []
    id_count = 1
    re_group = []
    for cmd in json:
        name = proc_id = ""
        branch_f = 1
        fath_temp = []
        options = dag.Options()
        redirect = []
        input_list = []
        id_input = 1
        after_s = []
        for elem in cmd:
            if elem == "id":
                proc_id = cmd[elem]
            elif elem == "executable":
                name = cmd[elem]
            elif elem == "branching_factor":
                branch_f = int(cmd[elem])
            elif elem == "after":
                for i in range(len(cmd[elem])):
                    node_id = cmd[elem][i]
                    if node_id[-1] == "*":
                        node_id = node_id[0:len(node_id)-1]
                        after_s.append(node_id)
                    try:
                        t_node = nodes[node_id]
                        fath_temp.append(t_node)
                    except:
                        print nodes
                        raise Exception("{} There is a problem with the id of {} process".format(c.ERROR_PREFIX, name))
            elif elem == "options": # handle the different options case
                for opt in cmd[elem]:
                    if len(opt) == 1:
                        if "key" in opt or "value" in opt: # only key or filepath
                            options.add_option([opt[opt.keys()[0]]])
                        elif "type" in opt: # input/output (dynamic)
                            options.add_doption(opt["type"]) 
                    elif len(opt) == 2:
                        if "key" in opt and "value" in opt: # key + value
                            options.add_option([opt["key"], opt["value"]]) 
                        elif "key" in opt and "type" in opt: # key + type (dynamic)
                            options.add_doption(opt["type"], d_key=opt["key"]) 
                        elif "value" in opt and "type" in opt: # value + type
                            options.add_option([opt["value"]], io_type=opt["type"])
                        elif "type" in opt and "regex" in opt: # type + regex (dynamic)
                            f_list, n_proc, group = check_regex(opt["regex"], opt["type"])
                            if branch_f == 1:
                                branch_f = n_proc
                            if group:
                                re_group += group
                            if not f_list:
                                options.add_doption(opt["type"], regex=opt["regex"])
                            else:
                                options.add_option([id_input], io_type=opt["type"]) # value + type
                                input_list.append([id_input, f_list])
                                id_input += 1
                    elif len(opt) == 3:
                        if "key" in opt and "value" in opt and "type" in opt: # key + value + type
                            options.add_option([opt["key"], opt["value"]], io_type=opt["type"])
                        elif "key" in opt and "type" in opt and "regex" in opt: # key + type + regex (dynamic)
                            f_list, n_proc, group = check_regex(opt["regex"], opt["type"])
                            if branch_f == 1:
                                branch_f = n_proc
                            if group:
                                re_group += group
                            if not f_list:
                                options.add_doption(opt["type"], d_key=opt["key"], regex=opt["regex"])
                            else: # key + value + type
                                options.add_option([opt["key"], id_input], io_type=opt["type"])
                                input_list.append([id_input, f_list])
                                id_input += 1
                        elif "value" in opt and "type" in opt and "regex" in opt: # value + type + regex (dynamic)
                            f_list, n_proc, group = check_regex(opt["regex"], opt["type"])
                            if branch_f == 1:
                                branch_f = n_proc
                            if group:
                                re_group += group
                            if not f_list:
                                options.add_doption(opt["type"], d_val=opt["value"], regex=opt["regex"])
                            else:
                                options.add_option([id_input], io_type=opt["type"]) # value + type
                                input_list.append([id_input, f_list])
                                id_input += 1
                    elif len(opt) == 4: 
                        if "value" in opt and "regex" in opt and "n" in opt and "type" in opt: # value + type + regex + n (dynamic)
                            f_list, n_proc, group = check_regex(opt["regex"], opt["type"])
                            if group:
                                re_group += group
                            if not f_list:
                                options.add_doption(opt["type"], d_val=opt["value"], regex=opt["regex"])
                            else: 
                                options.add_option([f_list.pop()], io_type=opt["type"]) # value + type
                                input_list.append(f_list)
                            branch_f = int(opt["n"])
                            if branch_f != n_proc: 
                                print("{} The value n is different from the number of file in {} process".format(c.WARNING_PREFIX, name))
            elif elem == "redirect":
                for opt in cmd[elem]:
                    if opt["type"] == "stdout" or opt["type"] == "stderr" or opt["type"] == "stdout|stderr" or opt["type"] == "stderr|stdout":
                        if "file" in opt:
                            file_path = opt["file"]
                        else: # automatic generation redirect file path
                            file_path = "{}_{}_out.txt".format(name, id_count)
                        redirect.append((opt["type"], file_path))
                        if opt["type"] != "stderr":
                            options.io_index["output"].append("redirect")
        if redirect: # there is redirect option
            options.redirect = redirect
        if proc_id == "": # generate new id 
            proc_id = "{}_{}".format(name, id_count)
            id_count += 1
        if branch_f > 1: # process parallelism
            new_proc_id = proc_id
            n_proc = branch_f
            o_options = copy.deepcopy(options)
            for i in range(0, branch_f):
                if i == 1:
                    n_proc = 1
                if not options.io_index["output"] and not options.redirect: # no output in a process
                    print("{} The process {} has no output defined".format(c.WARNING_PREFIX, new_proc_id))
                #father  
                father = [] 
                if fath_temp:
                    for f in fath_temp:
                        if f.branch_f > 1:
                            if i == 0:
                                f_id = "{}".format(t_node.proc_id)
                            else:
                                f_id = "{}_{}".format(t_node.proc_id, i+1)
                            father.append(nodes[f_id])
                        else:
                            father.append(f)                        
                # set input from regex
                if options.regex:
                    for o in options.opt_list:
                        if options.dynamic and len(o) == 4 and o[3] != None: # check if there are groups inside regex
                            o[2] = replace_group(o[3], i, re_group)
                if input_list:
                    j = 0
                    #print input_list
                    for o in options.opt_list:
                        path = len(o) - 1 
                        if o[path] == input_list[j][0]: # id_input
                            o[path] = input_list[j][1].pop(0)
                            j += 1
                            if j == len(input_list):
                                break
                process = dag.Process(name, new_proc_id, options, n_proc, father)
                options = copy.deepcopy(o_options)
                nodes[new_proc_id] = process
                new_proc_id = "{}_{}".format(proc_id, i+2)
                if not father:
                    root_list.append(process)
        else:
            if not options.io_index["output"] and not options.redirect: # no output in a process
                print("{} The process {} has no output defined".format(c.WARNING_PREFIX, proc_id))
            if after_s:
                father = copy.deepcopy(fath_temp)
                for f in father:
                    if f.name in after_s:
                        for i in range(0, f.branch_f - 1):
                            f_id = "{}_{}".format(f.proc_id, i+2)
                            fath_temp.append(nodes[f_id])
            process = dag.Process(name, proc_id, options, branch_f, fath_temp)
            nodes[proc_id] = process
            if not fath_temp:
                root_list.append(process)
    process_dag = dag.Dag(root_list, nodes)
    return process_dag

def build_cue(p_dag, num_proc):
    """ Build the cue and set the io """
    # init the cue
    cue = []
    leaves_o = 0
    for n in p_dag.root:
        print n
        cue.append([n.proc_id, -1]) # process and the node will execute the job
    # init the bfs
    n_list = p_dag.root
    n_visited = {}
    for n in p_dag.nodes:
        n_visited[n] = False
    while n_list:
        node = n_list.pop()
        if node.son:
            for s in node.son:
                if n_visited[s.proc_id] == False and s not in n_list:
                    if not s.son and not s.options.io_index["output"]: # leaves without output
                        leaves_o += 1
                    # cue and bfs
                    n_list.append(s)
                    cue.append([s.proc_id, -1])
                    n_visited[s.proc_id] = True
                    # set the io
                    father_l = len(s.father)
                    i = 0
                    for f in s.father:
                        # add new controls on output here
                        out = f.options.get_io_option("output")
                        if out:
                            for o in out:
                                new_io = s.options.set_io_option("input", o)
                                if new_io == False and i < father_l - 1:
                                    print("{} The process {} has more output than sons input".format(c.WARNING_PREFIX, f.proc_id))
                                    break
                        i += 1
                    print s
                    if s.options.available_input == True:
                        raise Exception("{} The process {} has one or more input without a path".format(c.ERROR_PREFIX, s.proc_id))
    if leaves_o > 1: # leaves warning
        print("{} Is it possible a multiple writing on the stdout".format(c.WARNING_PREFIX))
    return cue

def gen_mpi(json, num_proc):
    """ Generate all the stuff needed for the parallelization """
    process_dag = build_dag(json)
    process_dag.compute_son()
    print "Roots node " + str(dag.Process().print_nlist(process_dag.root))
    process_dag.cue = build_cue(process_dag, num_proc)
    process_dag.save(c.SERIAL_FILE)