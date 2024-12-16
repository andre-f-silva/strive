from inspect import getmembers, isfunction
from unittest import TestCase

from core.system_processor import get_cycles_and_dag_paths, set_self_loop_budget, verify_cycles
from parser.system_parser import create_graph_from_system
from resources import system_examples
from resources.system_examples import *


class Test(TestCase):
    def test_run_all_without_exceptions(self):

        example_functions = getmembers(system_examples, isfunction)
        for name, example in example_functions:
            if "example" not in name:
                continue
            print(name)
            system = example()
            graph = create_graph_from_system(system)
            cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)
            verify_cycles(topological_paths_for_cycles, system)

    def test_run_system_8(self):
        system = example_8()
        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)

        self.assertEqual(0, len(cycles))

    def test_run_system_9(self):
        system = example_9()
        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)

        self.assertEqual(0, len(cycles))

    def test_run_system_10(self):
        system = example_10()
        graph = create_graph_from_system(system)
        set_self_loop_budget(0)
        cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)

        self.assertEqual({('1', '4', '2', '3'), ('1', '2', '4', '3')}, paths)

    def test_run_system_11(self):
        system = example_11()
        graph = create_graph_from_system(system)
        #set_self_loop_budget(1)
        cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)
        # 3' e 4' não aparecem de seguida

    def test_run_system_12(self):
        system = example_12()
        graph = create_graph_from_system(system)
        #set_self_loop_budget(1)
        cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)
        for c in cycles:
            self.assertTrue(len(cycles) <= 4)
