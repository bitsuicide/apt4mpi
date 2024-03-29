""" Dag data class and utilities """

import pickle

class Options:
    """ Option class for handle different case """
    def __init__(self):
        self.opt_list = []
        self.dynamic = False
        self.regex = [False, []]
        self.io_index = {"input" : [], "output" : []}
        self.redirect = []

    def __str__(self):
        return "{}, Dynamic: {} Regex: {} Redirect: {}".format(self.opt_list, self.dynamic, self.regex, self.redirect)

    def set_io_option(self, io_type, d_val):
        """ Set io option """
        index = self.io_index[io_type]
        if index:
            for io in index:
                if self.dynamic and len(self.opt_list[io]) == 4:
                    path = 2
                elif len(self.opt_list[io]) >= 1:
                    path = len(self.opt_list[io]) - 1
                if self.opt_list[io][path] == None:
                    self.opt_list[io][path] = d_val
                    return True
        return False

    def get_io_option(self, io_type):
        """ Get io option """
        index = self.io_index[io_type]
        io_list = []
        if index:
            for io in index:
                if io != "redirect":
                    if self.dynamic and len(self.opt_list[io]) == 4:
                        path = 2
                    elif len(self.opt_list[io]) >= 1:
                        path = len(self.opt_list[io]) - 1
                    io_list.append(self.opt_list[io][path])
                else:
                    for r in self.redirect:
                        if r[0] == "stdout" or r[0] == "stdout|stderr":
                            io_list.append(r[1])
        return io_list 

    def add_option(self, opt, io_type=""):
        """ Add standard option """
        self.opt_list.append(opt)
        if io_type:
            self.io_index[io_type].append(len(self.opt_list) - 1)

    def add_doption(self, io_type, d_key=None, d_val=None, regex=None):
        """ Add dynamic option """
        self.opt_list.append([io_type, d_key, d_val, regex])
        self.io_index[io_type].append(len(self.opt_list) - 1)
        self.dynamic = True
        if regex:
            self.regex[0] = True
            self.regex[1].append(len(self.opt_list) - 1)

    def available_input(self):
        """ Check if there are some input without path """
        if self.dynamic != True or self.redirect[0] != True:
            for i in self.io_index["input"]:
                c_input = self.opt_list[i]
                path = len(self.opt_list[io]) - 1
                if self.opt_list[io][path] == None:
                    return True
            return False 
        else: # Dynamic and redirect process is not checkable
            return None

class Process:
    """ Process class - DAG node """
    STAT_READY = "ready"
    STAT_NREADY = "waiting"
    STAT_RUNNING = "running"
    STAT_DONE = "done"
    STAT_FAILED = "failed"

    def __init__(self, name="", proc_id="", options=None, branch_f=1, father=None):
        self.name = name
        self.proc_id = proc_id 
        self.options = options
        self.branch_f = branch_f
        self.father = father
        self.son = []
        self.status = self.STAT_NREADY
        self.start_time = 0
        self.end_time = 0
        self.exec_time = 0
        self.exit_status = None
        self.std_err = ""

    def __str__(self):
        if self.father != None:
            return "Name: {}, Id: {}, Options: {}, branch_f level: {}, Fathers: {} Sons: {}".format(self.name, self.proc_id, self.options, 
                self.branch_f, self.print_nlist(self.father), self.print_nlist(self.son))
        else:
            return "Name: {}, id: {}, Options: {}, branch_f level: {}, Fathers: None Sons: {}".format(self.name, self.proc_id, self.options, 
                self.branch_f, self.print_nlist(self.son))    

    def print_nlist(self, l):
        """ Print in a readable style process in a list """
        t_list = []
        for e in l:
            t_list.append(e.proc_id)
        return t_list

    def is_ready(self):
        """ Check if a process is ready to execute """
        if not self.father: # a node without father
            return True
        else:
            for f in self.father:
                if f.status == self.STAT_NREADY or f.status == self.STAT_RUNNING:
                    return False
            #self.status = self.STAT_READY
            return True

class Dag:
    """ Process flow data structure """
    def __init__(self, root=None, nodes=None):
        self.nodes = nodes
        self.root = root
        self.queue = []

    def compute_son(self):
        """ Generate the son list for every nodes """
        for n in self.nodes:
            node = self.nodes[n]
            father = node.father
            f_temp = []
            # compute and add parallels sons
            """
            for f in father:
                if f.branch_f > 1:
                    for i in range(0, f.branch_f - 1):
                        f_id = "{}_{}".format(f.proc_id, i+2)
                        f_temp.append(self.nodes[f_id])
            father = father + f_temp
            """
            for f in father: 
                if node not in f.son:
                        f.son.append(node)

    def save(self, path):
        """ Save data structure on file """
        f_obj = open(path, "wb")
        pickle.dump(self, f_obj)
        f_obj.close()