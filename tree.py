""" Tree data class """

class Process:
    name = ""
    proc_id = ""
    input_path = ""
    output_path = ""
    options = ""
    parallelism = 1
    father = None
    son = []

    def __init__(self, name, proc_id, input_path, output_path, options, parallelism, father):
        self.name = name
        self.proc_id = proc_id 
        self.input_path = input_path
        self.output_path = output_path
        self.options = options
        self.parallelism = parallelism
        self.father = father

    def print_son(self):
        son = []
        for s in self.son:
            son.append(s.name)
        return son

    def __str__(self):
        if self.father != None:
            return "Name: {}, Options: {}, Parallelism level: {}, Father: {} Sons: {}".format(self.name, self.options, self.parallelism, self.father.name, self.print_son())
        else:
            return "Name: {}, Options: {}, Parallelism level: {}, Father: None Sons: {}".format(self.name, self.options, self.parallelism, self.print_son())

class Tree:
    nodes = {}
    root = None

    def __init__(self, root, nodes):
        self.nodes = nodes
        self.root = root

    def compute_son(self):
        """ Generate the son list for every nodes """
        for n in self.nodes:
            node = self.nodes[n]
            father = node.father
            if father != None and node not in father.son:
                if not father.son:
                    father.son = [node]
                else:
                    father.son += node