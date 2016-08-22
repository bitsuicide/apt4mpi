import tree 
import constant as c

def build_tree(json):
    nodes = {}
    root = None
    for cmd in json:
        input_path = output_path = name = proc_id = options = ""
        parallelism = 1
        father = None
        for opt in cmd:
            if opt == "input_file_path":
                input_path = cmd[opt]
            elif opt == "output_file_path":
                output_path = cmd[opt]
            elif opt == "id":
                proc_id = cmd[opt]
            elif opt == "executable":
                name = cmd[opt]
            elif opt == "branching_factor":
                parallelism = int(cmd[opt])
            elif opt == "after":
                father = nodes[cmd[opt][0]]
            elif opt == "options":
                for elem in cmd[opt]:
                    options += "{} {} ".format(elem["key"], elem["value"])
        process = tree.Process(name, proc_id, input_path, output_path, options, parallelism, father)
        nodes[proc_id] = process
        if root == None:
            root = process
    process_tree = tree.Tree(root, nodes)
    return process_tree

def write_mpi(p_tree):
    mpi_script = ""
    elem_list = [p_tree.root]
    while elem_list:
        node = elem_list.pop()
        if node.son:
            for c in node.son:
                print c
                if c not in elem_list:
                    elem_list.append(c)
    return mpi_script

def gen_mpi(json):
    mpi_script = ""
    process_tree = build_tree(json)
    process_tree.compute_son()
    print process_tree.root
    mpi_script = write_mpi(process_tree)
    return mpi_script


