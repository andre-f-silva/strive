class UndirectedGraph:
    def __init__(self):
        self.__assertions = {}
        self.__nodes = {}
        self.__business_sequences = []
        self.__business_sequences_data_dependencies = {}

    def nodes(self):
        return self.__nodes

    def business_sequences(self):
        return self.__business_sequences

    def add_node(self, node):
        if node not in self.__nodes:
            self.__nodes[node] = set()

    def add_edge(self, node1, node2, label, assertions=[]):
        self.add_node(node1)
        self.add_node(node2)
        if node1 in self.__nodes and node2 in self.__nodes:
            self.__nodes[node1].add((node2, label))
            self.__nodes[node2].add((node1, label))
            self.__assertions[(node1, node2)] = assertions
            self.__assertions[(node2, node1)] = assertions

    def add_business_sequence(self, nodes):
        self.__business_sequences.append(nodes)

    def get_business_sequence_of(self, node):
        for s in self.__business_sequences:
            if node in s:
                return s

    def remove_edge(self, node1, node2):
        if node1 in self.__nodes and node2 in self.__nodes:
            for neighbour, label in set(self.__nodes[node1]):
                if neighbour == node2:
                    self.__nodes[node1].discard((neighbour, label))
            for neighbour, label in set(self.__nodes[node2]):
                if neighbour == node1:
                    self.__nodes[node2].discard((neighbour, label))

    def get_neighbours(self, node):
        if node in self.__nodes:
            return list(self.__nodes[node])
        else:
            return []

    def __str__(self):
        return str(self.__nodes)

    def has_edge(self, node1, node2):

        if node1 in self.__nodes and node2 in self.__nodes:
            for neighbour, label in self.__nodes[node1]:
                if neighbour == node2:
                    return True

        return False

    def get_assertions_of_nodes(self, node1, node2):
        return self.__assertions[(node1, node2)]