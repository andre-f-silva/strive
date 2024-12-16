import time
from collections import deque
from io import BytesIO
from typing import Tuple, Set

import networkx as nx
from matplotlib import pyplot as plt
from networkx import DiGraph

from parser.smt_utils import check_cycle_sat
from parser.system_parser import *
from resources.system_examples import *

SELF_LOOP_BUDGET = 0
SHOW_DAG_IMAGE = False
SHOW_INVALID_DAG_IMAGE = False
ENABLE_PRINT = False
ADD_REDUNDANT_SEQ_OP = False
VISIT_FUTURE_ONLY = True


def set_self_loop_budget(budget):
    global SELF_LOOP_BUDGET
    SELF_LOOP_BUDGET = budget


def set_show_dag(show):
    global SHOW_DAG_IMAGE
    SHOW_DAG_IMAGE = show


def is_valid_cycle(path, graph: UndirectedGraph):

    external_from_initial_bt = [i for i in path if i not in graph.get_business_sequence_of(path[0])]
    if not len(external_from_initial_bt) > 0:
        #print("removing due to not visiting external", path)
        return False, []

    if graph.get_business_sequence_of(path[0]) == graph.get_business_sequence_of(path[-1]) and \
            int(path[0].label) > int(path[-1].label):
        #print("removing due to path starting in the future", path)
        return False, []

    if path[0].bt_owner != path[-1].bt_owner:
        raise ValueError("this should not happen")

    if VISIT_FUTURE_ONLY:
        sorted_lt_by_owner = sorted(list(map(lambda lt: (lt.bt_owner.name, lt.label), path)), key=lambda x: x[0])
        separated_by_owners = {}
        for tuple in sorted_lt_by_owner:
            key = tuple[0]
            if key in separated_by_owners:
                separated_by_owners[key].append(tuple)
            else:
                separated_by_owners[key] = [tuple]

        cycle_is_in_future_order = True
        for lts_with_same_bt in separated_by_owners.values():
            for i in range(len(lts_with_same_bt) - 1):
                current = lts_with_same_bt[i]
                next = lts_with_same_bt[i + 1]
                if int(current[1]) > int(next[1]):
                    cycle_is_in_future_order = False

        if not cycle_is_in_future_order:
            #print("will delete due to not future order", path)
            return False, []

    result, assertions = check_cycle_sat(path, graph)
    return result, assertions


def number_of_cloned_bt_in_path(path, graph):
    seen = set()
    for node in path:
        if node.cloned and node not in seen:
            seen.add(tuple(graph.get_business_sequence_of(node)))

    return len(seen)


def dfs_cycles(graph: UndirectedGraph, valid_starting_points: Set[BusinessTransactionUnit]):
    cycles = set()
    used_edges = dict()
    cycle_assertions = dict()

    for node in valid_starting_points:

        q = deque()
        q.append((node, [node], [(node, "start")], 0))

        while q:

            curr_node, path, edges, self_loop_counter = q.pop()

            if curr_node in graph.get_business_sequence_of(node) and curr_node != node:

                is_valid, assertions = is_valid_cycle(path, graph)
                if is_valid:
                    cycles.add(tuple(path))
                    used_edges[tuple(path)] = (tuple(edges))
                    cycle_assertions[tuple(path)] = assertions
                    continue

            for (neighbour, label) in graph.get_neighbours(curr_node):

                if (neighbour not in path or neighbour == curr_node) and (neighbour, label) not in edges:

                    next_self_loop_counter = self_loop_counter

                    if neighbour.cloned:
                        next_self_loop_counter += 1

                    if number_of_cloned_bt_in_path(path + [neighbour], graph) > SELF_LOOP_BUDGET:
                        continue

                    q.append((neighbour, path + [neighbour], edges + [(neighbour, label)], next_self_loop_counter))

    return cycles, used_edges, cycle_assertions


def generate_dependency_dag(graph: UndirectedGraph, cycle: Tuple[BusinessTransactionUnit]):
    dag = nx.DiGraph()

    skip_counter = 0
    for index, node in enumerate(cycle):
        if skip_counter > 0:
            skip_counter -= 1
            continue

        business_sequence = graph.get_business_sequence_of(node)
        for previous_in_sequence, current_in_sequence in zip(business_sequence, business_sequence[1:]):
            if int(current_in_sequence.label) > int(node.label):
                break
            dag.add_edge(previous_in_sequence.label, current_in_sequence.label)

        # connect to next node in cycle
        for next_node in cycle[index + 1:]:
           if not dag.has_node(next_node.label):
               dag.add_edge(node.label, next_node.label)
               break
           else:
               print("DIDNT ADD EDGE")

            # using reverse to check if the current node is going to connect to something in the past
           reversed = dag.reverse(copy=True)
           print("HELLO REVERSE CHECK")
           if nx.has_path(reversed, node.label, next_node.label):
               print("SKIPPING DUE TO REVERSE PATH FOUND")
               skip_counter += 1
               continue

    if ADD_REDUNDANT_SEQ_OP:
        for index, node in enumerate(cycle):
           business_sequence = graph.get_business_sequence_of(node)
           for previous_in_sequence, current_in_sequence in zip(business_sequence, business_sequence[1:]):
               if not dag.has_edge(previous_in_sequence.label, current_in_sequence.label):
                   dag.add_edge(previous_in_sequence.label, current_in_sequence.label)

    return dag


def get_valid_starting_nodes(graph: UndirectedGraph):

    # TODO test with and without this. optimize
    #return list(graph.nodes().keys())

    valid_starting_nodes = set()
    for bt_seq in graph.business_sequences():
        going_to_add_from_bt = set()
        if len(bt_seq) == 1:
            continue
        if bt_seq[0].cloned:
            continue

        going_to_add_from_bt.add(bt_seq[0])

        for next in bt_seq[1:]:
            found_edge = False

            for n in going_to_add_from_bt:
                if graph.has_edge(n, next):
                    found_edge = True

            if not found_edge:
                going_to_add_from_bt.add(next)

        valid_starting_nodes.update(going_to_add_from_bt)

    return valid_starting_nodes


def generate_dags(cycles: Set[Tuple[BusinessTransactionUnit]], graph: UndirectedGraph):
    dags = set()
    for cycle in cycles:

        dag_result = generate_dependency_dag(graph, cycle)

        if not any(d.edges == dag_result.edges and d.nodes == dag_result.nodes for _, d in dags):
            dags.add((cycle, dag_result))
    return dags


def generate_topological_paths(dags: Set[Tuple[Tuple[BusinessTransactionUnit], DiGraph]]):
    topological_paths_all = set()
    topological_paths_for_cycle = []

    for c, d in dags:
        try:
            topological_paths = [tuple(topological_path) for topological_path in nx.all_topological_sorts(d)]
            topological_paths_all.update(topological_paths)
            topological_paths_for_cycle.append((c, topological_paths))

            if SHOW_DAG_IMAGE:
                print(c)
                pos = nx.spring_layout(d)
                nx.draw(d, pos, with_labels=True)
                plt.show()
                plt.close()
        except Exception as err:
            print("error", c, err)
            if SHOW_INVALID_DAG_IMAGE:
                pos = nx.spring_layout(d)
                nx.draw(d, pos, with_labels=True)
                plt.show()
                plt.close()

    return topological_paths_for_cycle, topological_paths_all


def generate_system_graph_image(graph):
    nx_graph = nx.DiGraph()

    for node in graph.nodes():
        if "Parallel" in str(node):
            continue
        nx_graph.add_node(node)

    for node, neighbors in graph.nodes().items():
        for neighbor in neighbors:
            if "Parallel" in str(neighbor[0]) or "Parallel" in str(node):
                continue
            nx_graph.add_edge(node, neighbor[0], label=neighbor[1])

    for business_sequence in graph.business_sequences():
        for previous_in_sequence, current_in_sequence in zip(business_sequence, business_sequence[1:]):
            if "Parallel" in str(previous_in_sequence) or "Parallel" in str(current_in_sequence):
                continue
            nx_graph.add_edge(previous_in_sequence, current_in_sequence, label="sequence")

    pos = nx.circular_layout(nx_graph)
    nx.draw(nx_graph, pos, with_labels=True)
    edge_labels = nx.get_edge_attributes(nx_graph, 'label')
    nx.draw_networkx_edge_labels(nx_graph, pos, edge_labels=edge_labels)

    plt.show()
    plt.close()

    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    png_bytes = buffer.getvalue()
    buffer.close()
    nx_graph.clear()
    plt.close()

    return png_bytes

def generate_system_graph_image_2(graph):
    nx_graph = nx.Graph()
    for node, neighbors in graph.nodes().items():
        if "Parallel" in str(node):
            continue
        nx_graph.add_node(node)
        for neighbor in neighbors:
            if "Parallel" in str(neighbor[0]):
                continue
            nx_graph.add_edge(node, neighbor[0], label=neighbor[1])

    pos = nx.spring_layout(graph, k=0.15, iterations=20)
    nx.draw(nx_graph, pos, with_labels=True, node_size=500)

    plt.show()
    plt.close()


def get_cycles_and_dag_paths(graph: UndirectedGraph):

    valid_starting_nodes = get_valid_starting_nodes(graph)

    cycles, used_edges, cycle_assertions = dfs_cycles(graph, valid_starting_nodes)
    #print("Time (s)", time.process_time())

    # results = dict()
    #
    # bts = list()
    # bts_no_dups = set()
    # for c in cycles:
    #     #print(c)
    #     lt_chain = list(map(lambda lt: lt.bt_owner.name, c))
    #     bts.append(str(lt_chain))
    # #print(bts)
    # print(Counter(bts))
    # print(len(Counter(bts)))
    # bt_seqs = list([x for x in bts if not (x in bts_no_dups or bts_no_dups.add(x))])
    # print(Counter(bt_seqs))
    # print(len(bt_seqs))


    dags = generate_dags(cycles, graph)

    #print("Number of dags", len(dags))

    topological_paths_for_cycles, topological_paths = generate_topological_paths(dags)

    if ENABLE_PRINT:
        print(cycles)
        print(topological_paths)

    return cycles, topological_paths, topological_paths_for_cycles, cycle_assertions


def verify_cycles(topological_paths_for_cycles, system):

    for c, paths in topological_paths_for_cycles:
        count = 0
        for path in paths:
            bt_path = list(map(lambda node: system.get_bt_of_unit_label(node).name, path))
            stopped = False
            seen_again = False
            start_bt = bt_path[0]
            for bt in bt_path:

                if bt == start_bt and not stopped:
                    pass
                elif bt == start_bt and stopped:
                    seen_again = True
                else:
                    stopped = True

            if not seen_again:
                count += 1
                #print("warning, cycle generated serialized path: ", path, c)

            stopped = False
            seen_again_2 = False
            start_bt = list(reversed(bt_path))[0]
            for bt in reversed(bt_path):

                if bt == start_bt and not stopped:
                    pass
                elif bt == start_bt and stopped:
                    seen_again_2 = True
                else:
                    stopped = True

            if not seen_again_2:
                #print("warning, cycle generated serialized path: ", path, c)
                count += 1

            if not seen_again_2 and not seen_again:
                count -= 1

        if count > 0:
            print("warning cycle generated serialized paths", count, c)


def generate_dependency_dag_tese(graph: UndirectedGraph, cycle: Tuple[BusinessTransactionUnit]):
    dag = nx.DiGraph()

    for index, node in enumerate(cycle):

        business_sequence = graph.get_business_sequence_of(node)
        for previous_in_sequence, current_in_sequence in zip(business_sequence, business_sequence[1:]):
            if int(current_in_sequence.label) > int(node.label):
                break
            dag.add_edge(previous_in_sequence.label, current_in_sequence.label)

        # connect to next node in cycle
        dag.add_edge(node.label, cycle[index + 1].label)

if __name__ == '__main__':
    system = example_4()
    graph = create_graph_from_system(system)
    cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)
    verify_cycles(topological_paths_for_cycles, system)
