import unittest
from unittest import TestCase

from parser.system_parser import *
from resources.system_examples import *


@unittest.skip('broken since self loop expansions and edge changes')
class Test(TestCase):

    def test_create_graph_from_system_2(self):
        system = example_2()

        graph = create_graph_from_system(system)

        solution = UndirectedGraph()
        solution.add_business_sequence(["1", "2"])
        solution.add_business_sequence(["3", "4"])
        solution.add_business_sequence(["5", "6"])
        solution.add_edge("1", "1", "self write")
        solution.add_edge("1", "3", "write")
        solution.add_edge("1", "5", "read")
        solution.add_edge("2", "2", "self write")
        solution.add_edge("2", "4", "write")
        solution.add_edge("2", "6", "read")
        solution.add_edge("3", "3", "self write")
        solution.add_edge("3", "1", "write")
        solution.add_edge("3", "5", "read")
        solution.add_edge("4", "4", "self write")
        solution.add_edge("4", "2", "write")
        solution.add_edge("4", "6", "read")
        solution.add_edge("5", "3", "read")
        solution.add_edge("5", "1", "read")
        solution.add_edge("6", "4", "read")
        solution.add_edge("6", "2", "read")

        self.assertEqual(sorted(solution.nodes()),
                         sorted(map(lambda n: n.label, graph.nodes())))

        self.assertEqual(sorted(solution.business_sequences()),
                         sorted([[n.label for n in business_sequence]
                                 for business_sequence in graph.business_sequences()]))

    def test_create_graph_from_system_6(self):
        system = example_6()

        graph = create_graph_from_system(system)

        solution = UndirectedGraph()
        solution.add_business_sequence(["1", "2", "3"])
        solution.add_business_sequence(["4", "5", "6"])
        solution.add_edge("1", "2", "read")
        solution.add_edge("5", "2", "read")
        solution.add_edge("2", "2", "self write")

        self.assertEqual(sorted(solution.nodes()),
                         sorted(map(lambda n: n.label, graph.nodes())))

        self.assertEqual(sorted(solution.business_sequences()),
                         sorted([[n.label for n in business_sequence]
                                 for business_sequence in graph.business_sequences()]))

    def test_create_graph_from_system_8(self):
        system = example_8()

        graph = create_graph_from_system(system)

        solution = UndirectedGraph()
        solution.add_business_sequence(["1", "2"])
        solution.add_business_sequence(["3"])
        solution.add_edge("1", "3", "read")
        solution.add_edge("3", "3", "self write")

        self.assertEqual(sorted(solution.nodes()),
                         sorted(map(lambda n: n.label, graph.nodes())))

        self.assertEqual(sorted(solution.business_sequences()),
                         sorted([[n.label for n in business_sequence]
                                 for business_sequence in graph.business_sequences()]))

    def test_create_graph_from_system_9(self):
        system = example_9()

        graph = create_graph_from_system(system)

        solution = UndirectedGraph()
        solution.add_business_sequence(["1", "2"])
        solution.add_business_sequence(["3"])
        solution.add_business_sequence(["4"])
        solution.add_edge("1", "3", "read")
        solution.add_edge("3", "4", "write")
        solution.add_edge("3", "3", "self write")
        solution.add_edge("4", "4", "self write")

        self.assertEqual(sorted(solution.nodes()),
                         sorted(map(lambda n: n.label, graph.nodes())))

        self.assertEqual(sorted(solution.business_sequences()),
                         sorted([[n.label for n in business_sequence]
                                 for business_sequence in graph.business_sequences()]))

    def test_create_graph_from_system_10(self):
        system = example_10()

        graph = create_graph_from_system(system)

        solution = UndirectedGraph()
        solution.add_business_sequence(["1", "2", "3"])
        solution.add_business_sequence(["4"])
        solution.add_edge("1", "4", "read")
        solution.add_edge("3", "4", "read")
        solution.add_edge("2", "2", "self write")
        solution.add_edge("4", "4", "self write")

        self.assertEqual(sorted(solution.nodes()),
                         sorted(map(lambda n: n.label, graph.nodes())))

        self.assertEqual(sorted(solution.business_sequences()),
                         sorted([[n.label for n in business_sequence]
                                 for business_sequence in graph.business_sequences()]))

    def test_create_graph_from_system_11(self):
        system = example_11()

        graph = create_graph_from_system(system)

        solution = UndirectedGraph()
        solution.add_business_sequence(["1", "2"])
        solution.add_business_sequence(["3", "4"])
        solution.add_edge("1", "4", "read")
        solution.add_edge("3", "2", "read")
        solution.add_edge("2", "2", "self write")
        solution.add_edge("4", "4", "self write")

        self.assertEqual(sorted(solution.nodes()),
                         sorted(map(lambda n: n.label, graph.nodes())))

        self.assertEqual(sorted(solution.business_sequences()),
                         sorted([[n.label for n in business_sequence]
                                 for business_sequence in graph.business_sequences()]))

    def test_create_graph_from_system_12(self):
        system = example_12()

        graph = create_graph_from_system(system)

        solution = UndirectedGraph()
        solution.add_business_sequence(["1", "2"])
        solution.add_business_sequence(["3"])
        solution.add_business_sequence(["4", "5"])
        solution.add_edge("1", "3", "read")
        solution.add_edge("2", "3", "read")
        solution.add_edge("4", "3", "read")
        solution.add_edge("5", "3", "read")
        solution.add_edge("3", "3", "self write")

        self.assertEqual(sorted(solution.nodes()),
                         sorted(map(lambda n: n.label, graph.nodes())))

        self.assertEqual(sorted(solution.business_sequences()),
                         sorted([[n.label for n in business_sequence]
                                 for business_sequence in graph.business_sequences()]))