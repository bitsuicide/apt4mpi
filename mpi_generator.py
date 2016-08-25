import tree 
import constant as c

def build_tree(json):
    """ Build three / graph process dependences """ 
    nodes = {}
    root = None
    id_count = 0
    for cmd in json:
        name = proc_id = ""
        branch_f = 1
        father = []
        options = tree.Options([], False)
        for elem in cmd:
            if elem == "id":
                proc_id = cmd[elem]
            elif elem == "executable":
                name = cmd[elem]
            elif elem == "branching_factor":
                branch_f = int(cmd[elem])
            elif elem == "after":
                for i in range(len(cmd[elem])):
                    father.append(nodes[cmd[elem][i]])
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
        process = tree.Process(name, proc_id, options, branch_f, father)
        nodes[proc_id] = process
        if root == None:
            root = process
    process_tree = tree.Tree(root, nodes)
    return process_tree

def write_mpi(p_tree, num_proc):
    """ Write the mpi file """
    mpi_script = ""
    n_list = [p_tree.root]
    while n_list:
        node = n_list.pop()
        if node.son:
            for c in node.son:
                print c    
                if c not in n_list:
                    n_list.append(c)
    return mpi_script

def gen_mpi(json, num_proc):
    """ Generate all the stuff needed for the paralallelization """
    mpi_script = ""
    process_tree = build_tree(json)
    process_tree.compute_son()
    print process_tree.root
    mpi_script = write_mpi(process_tree, num_proc)
    return mpi_script


