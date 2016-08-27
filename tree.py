""" Tree data class """

class Options:
    """ Option class for handle different case """
    def __init__(self):
        self.opt_list = []
        self.dynamic = False
        self.io_index = {"input" : 0, "output" : 0}

    def __str__(self):
        return "{}, Dynamic: {}".format(self.opt_list, self.dynamic)

    def set_doption(self, io_type, d_val):
        if dynamic:
            index = io_index[io_type]
            if index != 0:
                opt_list[index][2] = d_val
            else:
                raise AttributeError
        else:
            raise Exception("It is possible to set a dynamic value only if the obj is dynamic")

    def get_doption(self, io_type):
        if dynamic:
            index = io_index[io_type]
            if index != 0:
                return opt_list[index]
        else:
            raise Exception("It is possible to set a dynamic value only if the obj is dynamic")

    def add_option(self, opt, io_type=""):
        self.opt_list.append(opt)
        if io_type:
            self.io_index[io_type] = len(self.opt_list) - 1

    def add_doption(self, io_type, d_key=None, d_val=None, regex=None):
        self.opt_list.append([io_type, d_key, d_val, regex])
        self.io_index[io_type] = len(self.opt_list) - 1

class Process:
    """ Process class - Graph node """
    def __init__(self, name, proc_id, options, branch_f, father):
        self.name = name
        self.proc_id = proc_id 
        self.options = options
        self.branch_f = branch_f
        self.father = father
        self.son = []
        self.status = False

    def __str__(self):
        if self.father != None:
            return "Name: {}, Id: {}, Options: {}, branch_f level: {}, Father: {} Sons: {}".format(self.name, self.proc_id, self.options, 
                self.branch_f, self.print_nlist(self.father), self.print_nlist(self.son))
        else:
            return "Name: {}, id: {}, Options: {}, branch_f level: {}, Father: None Sons: {}".format(self.name, self.proc_id, self.options, 
                self.branch_f, self.print_nlist(self.son))    

    def print_nlist(self, l):
        t_list = []
        for e in l:
            t_list.append(e.proc_id)
        return t_list

class Tree:
    def __init__(self, root, nodes):
        self.nodes = nodes
        self.root = root

    def compute_son(self):
        """ Generate the son list for every nodes """
        for n in self.nodes:
            node = self.nodes[n]
            father = node.father
            f_temp = []
            # compute and add parallels sons
            for f in father:
                if f.branch_f > 1:
                    for i in range(0, f.branch_f - 1):
                        f_id = "{}_{}".format(f.proc_id, i+2)
                        f_temp.append(self.nodes[f_id])
            father = father + f_temp
            for f in father: 
                if node not in f.son:
                        f.son.append(node)