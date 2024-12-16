import inspect
import itertools
import re

from z3 import *

from domain.domain_model import *
from domain.graphUtils import UndirectedGraph


def check_cycle_sat(cycle, graph: UndirectedGraph):
    solver = z3.Solver()
    unique_assertions = set()
    for n1, n2 in list(zip(cycle, cycle[1:])):
        for a in graph.get_assertions_of_nodes(n1, n2):
            unique_assertions.add(a)

    #for a in unique_assertions:
    #    solver.add(a)

    combined_assertions = z3.And(list(unique_assertions))
    solver.add(combined_assertions)
    #print("TEST", combined_assertions)

    #for a in solver.assertions():
    #    print("simplifying")
    #    print(simplify(simplify(a)))
    #    print("before", a)


    simplified_assertions = list(set([simplify(assertion) for assertion in solver.assertions()]))

    #print("TEST2", simplified_assertions)

    result = solver.check()
    if result==sat:
        print(solver.model())
    return result == sat, simplified_assertions


def check_edge(lt1, lt2):

    solver = z3.Solver()
    assertions = []

    # an edge can be present if at least a pair of operations have an edge
    for o1, o2 in itertools.product(lt1.operations, lt2.operations):

        var_dep = False
        intersection = []
        if lt1.bt_owner == lt2.bt_owner:
            o1_output = {p.name for p in o1.get_output()}
            o2_input = {p.name for p in o2.get_input()}
            o2_output = {p.name for p in o2.get_output()}
            o1_input = {p.name for p in o1.get_input()}
            var_dep = o1_output.intersection(o2_input) or o2_output.intersection(o1_input)

        table_dep = False
        if o1.table.name == o2.table.name and \
                not (o1.type == OperationType.READ and o2.type == OperationType.READ) and \
                o1.used_columns.intersection(o2.used_columns):
            table_dep = True

        # if table is not in common and no variable dependency is present skip operation pair
        if not var_dep and not table_dep:
            continue
        #print("lt1", lt1, "lt2", lt2)
        #print("var_dep", var_dep, "; table_dep", table_dep)

        # the following code is to check if all variables of the interception are being used
        op_lambdas = ""
        if o1.predicates:
            op_lambdas = inspect.getsource(o1.predicates)

        if o2.predicates:
            op_lambdas += inspect.getsource(o2.predicates)

        matches = re.findall(r'ctx\["(.*?)"]', op_lambdas)
        used = True
        for m in matches:
            if m in intersection:
                used = True
                intersection.remove(m)

        #if len(intersection) > 0 and used:
        #    print(f"WARNING: not all variables in interception are being used")

        if not used and not table_dep:
            continue

        ctx_1 = o1.get_operation_ctx()
        ctx_2 = o2.get_operation_ctx()

        if o1.predicates and o2.predicates:
            assertions.append(z3.And(o1.predicates(ctx_1), o2.predicates(ctx_2)))
        elif o1.predicates:
            assertions.append(o1.predicates(ctx_1))
        elif o2.predicates:
            assertions.append(o2.predicates(ctx_2))
        else:
            #TODO YOOOOO test True vs False
            assertions.append(True)

    if len(assertions) == 0:
        return False, []

    if len(assertions) > 1:
        solver.add(z3.And(assertions))
    else:
        solver.add(assertions)

    past_lts_1 = lt1.bt_owner.get_lts_before_given_lt(lt1)
    past_lts_2 = lt2.bt_owner.get_lts_before_given_lt(lt2)

    for past_lts, curr_lt in [(past_lts_1, lt1), (past_lts_2, lt2)]:
        for lt in past_lts:
            for o in lt.operations:
                past_o_output = set(map(lambda p: p.name, o.get_output()))
                curr_lt_input = set(map(lambda p: p.name, curr_lt.get_input()))
                intersection = past_o_output.intersection(curr_lt_input)
                if intersection and o.predicates:
                    solver.add(o.predicates(o.get_operation_ctx()))

    result = solver.check()

    simplified_assertions = [simplify(assertion) for assertion in solver.assertions()]

    #print(solver.assertions())
    if result == sat:
        #model = solver.model()
        return True, simplified_assertions
    elif result == unsat:
        return False, simplified_assertions
    else:
        return None, simplified_assertions
