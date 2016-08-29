import tree 
import constant as c

def build_tree(json):
    """ Build three / graph process dependences """ 
    nodes = {}
    root_list = []
    id_count = 1
    for cmd in json:
        name = proc_id = ""
        branch_f = 1
        father = []
        options = tree.Options()
        for elem in cmd:
            if elem == "id":
                proc_id = cmd[elem]
            elif elem == "executable":
                name = cmd[elem]
            elif elem == "branching_factor":
                branch_f = int(cmd[elem])
            elif elem == "after":
                for i in range(len(cmd[elem])):
                    t_node = nodes[cmd[elem][i]]
                    father.append(t_node)
                    if t_node.branch_f > 1:
                        for i in range(0, t_node.branch_f - 1):
                            f_id = "{}_{}".format(t_node.proc_id, i+2)
                            father.append(nodes[f_id])
            elif elem == "options": # handle the different options case
                for opt in cmd[elem]:
                    #print len(opt)
                    if len(opt) == 1:
                        if "key" in opt or "value" in opt: # only key or filepath
                            options.add_option([opt.keys()[0]])
                        elif "type" in opt: # input/output (dynamic)
                            options.add_doption(opt["type"]) 
                    elif len(opt) == 2:
                        if "key" in opt and "value" in opt: # key + value
                            options.add_option([opt["key"], opt["value"]]) 
                        elif "key" in opt and "type" in opt: # key + type (dynamic)
                            options.add_doption(opt["type"], opt["key"]) 
                        elif "value" in opt and "type" in opt: # value + type
                            options.add_option([opt["value"]], io_type=opt["type"])
                        elif "type" in opt and "regex" in opt: # type + regex (dynamic)
                            options.add_doption(opt["type"], regex=opt["regex"])
                    elif len(opt) == 3:
                        if "key" in opt and "value" in opt and "type" in opt: # key + value + type
                            options.add_option([opt["key"], opt["value"]], io_type=opt["type"])
                        elif "key" in opt and "type" in opt and "regex" in opt: # key + type + regex (dynamic)
                            options.add_doption(opt["type"], opt["key"], regex=opt["regex"])
                        elif "value" in opt and "type" in opt and "regex" in opt: # value + type + regex (dynamic)
                            options.add_doption(opt["type"], value=opt["value"], regex=opt["regex"])
        if proc_id == "": # generate new id 
            proc_id = "{}_{}".format(name, id_count)
            id_count += 1
        if branch_f > 1: # process parallelism
            new_proc_id = proc_id
            for i in range(0, branch_f):
                process = tree.Process(name, new_proc_id, options, branch_f, father)
                nodes[new_proc_id] = process
                new_proc_id = "{}_{}".format(proc_id, i+2)
                branch_f = 1
                if not father:
                    root_list.append(process)
        else:
            process = tree.Process(name, proc_id, options, branch_f, father)
            nodes[proc_id] = process
            if not father:
                root_list.append(process)
    process_tree = tree.Tree(root_list, nodes)
    return process_tree

def build_cue(p_tree, num_proc):
    """ Write the mpi file """
    # init the cue
    cue = []
    for n in p_tree.root:
        cue.append([n.proc_id, -1]) # process and the node will execute the job
    # init the bfs
    n_list = p_tree.root
    n_visited = {}
    for n in p_tree.nodes:
        n_visited[n] = False
    while n_list:
        node = n_list.pop()
        if node.son:
            for c in node.son:
                if n_visited[c.proc_id] == False and c not in n_list:
                    print c
                    n_list.append(c)
                    cue.append([c.proc_id, -1])
                    n_visited[c.proc_id] = True
    return cue

def gen_mpi(json, num_proc):
    """ Generate all the stuff needed for the paralallelization """
    process_tree = build_tree(json)
    process_tree.compute_son()
    print process_tree.root
    process_tree.cue = build_cue(process_tree, num_proc)
    process_tree.save(c.SERIAL_FILE)


