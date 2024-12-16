import copy
import itertools

from domain.domain_model import *
from domain.graphUtils import *
from parser.smt_utils import check_edge

OPERATION_PREDICATE = True
DATA_DEP = True
PARALLEL_EXPANSION = True


def get_ops_edge_label(o1, o2):
    if o1.type == OperationType.WRITE and o2.type == OperationType.WRITE:
        return "WRITE"
    elif (o1.type == OperationType.READ and o2.type == OperationType.WRITE) or \
            (o2.type == OperationType.READ and o1.type == OperationType.WRITE):
        return "READ"

    return None


def create_graph_from_system(system: System) -> UndirectedGraph:

    graph = UndirectedGraph()

    # flatten remote transactions
    changed = True
    while changed:
        changed = False
        units = refresh_unit_and_add_labels(system)
        for u1 in units:
            if isinstance(u1, RemoteBusinessTransaction):
                changed = True
                units_to_copy = u1.business_transaction.business_transaction_units
                unit_clones = list(map(lambda unit: copy.deepcopy(unit), units_to_copy))
                system.get_bt_of_unit(u1).replace_remote_call(u1, unit_clones)

    # internal business transactions are only used during remote flattening
    # these transactions are not visible to the public which internally means
    # the independent nodes dont exist, only the flattened ones
    marked_for_removal = set()
    for m in system.microservices:
        for bt in m.business_transactions:
            if isinstance(bt, InternalBusinessTransaction):
                marked_for_removal.add((m, bt))

    for (m, bt) in marked_for_removal:
        m.remove_bt(bt)

    units = refresh_unit_and_add_labels(system)

    # expand write self loops to clone business transactions
    if PARALLEL_EXPANSION:
        bt_added_with_self_expansion = set()
        for unit in units:
            for o in unit.operations:
                if o.type == OperationType.WRITE:
                    bt_of_unit = system.get_bt_of_unit(unit)
                    microservice_owner_of_bt = system.get_m_of_bt(bt_of_unit)
                    if bt_of_unit.name + "Parallel" in bt_added_with_self_expansion:
                        continue

                    units_to_copy = bt_of_unit.business_transaction_units
                    unit_clones = list(map(lambda unit: copy.deepcopy(unit), units_to_copy))
                    for u in unit_clones:
                        u.set_cloned(True)

                    microservice_owner_of_bt.add_bt(BusinessTransaction(bt_of_unit.name + "Parallel", unit_clones, bt_of_unit.params))
                    bt_added_with_self_expansion.add(bt_of_unit.name + "Parallel")
                    break

        units = refresh_unit_and_add_labels(system)

    # add table dependencies
    for u1, u2 in itertools.combinations(units, 2):
        for o1, o2 in itertools.product(u1.operations, u2.operations):

            # add edge if same table, ignoring operation wheres
            if not OPERATION_PREDICATE and o1.table.name == o2.table.name:
                label = get_ops_edge_label(o1, o2)

                if not graph.has_edge(u1, u2) and label:
                    graph.add_edge(u1, u2, label)

            # add edges considering operations conditions (using z3)
            else:
                edge, assertions = check_edge(u1, u2)

                if edge and not graph.has_edge(u1, u2):
                    graph.add_edge(u1, u2, "z3" + str(assertions), assertions)

    # add edges between local transactions in each business transactions using variables
    if DATA_DEP:
        for m in system.microservices:
            for bt in m.business_transactions:
                for i in range(len(bt.business_transaction_units) - 1):
                    for j in range(i + 1, len(bt.business_transaction_units)):
                        lt_1 = bt.business_transaction_units[i]
                        lt_2 = bt.business_transaction_units[j]

                        edge, assertions = check_edge(lt_1, lt_2)
                        #TODO join labels
                        if edge and not graph.has_edge(lt_1, lt_2):
                            graph.add_edge(lt_1, lt_2, "output->input dependency", assertions)

    # add edges between local transactions just because they are in sequence
    else:
        for m in system.microservices:
            for bt in m.business_transactions:
                for lt1, lt2 in zip(bt.business_transaction_units, bt.business_transaction_units[1:]):
                    # TODO join labels
                    if not graph.has_edge(lt1, lt2):
                        graph.add_edge(lt1, lt2, "sequence", [])

    # auxiliary list just to simplify getters of local transactions in the same sequence
    for m in system.microservices:
        for bt in m.business_transactions:
            graph.add_business_sequence(bt.business_transaction_units)

    # remove edges that are marked as non-conflict
    for non_conflict_pair in system.non_conflict_units or []:
        graph.remove_edge(non_conflict_pair[0], non_conflict_pair[1])

    return graph


def refresh_unit_and_add_labels(system):
    units = [bu for m in system.microservices for bt in m.business_transactions for bu in bt.business_transaction_units]
    label_counter = 1
    for u in units:
        u.set_label(str(label_counter))
        label_counter += 1
    return units
