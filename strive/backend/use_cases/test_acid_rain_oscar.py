from unittest import TestCase

from core.system_processor import verify_cycles, get_cycles_and_dag_paths, set_self_loop_budget
from domain.domain_model import *
from parser.system_parser import create_graph_from_system


class Test(TestCase):

    def test(self):

        voucher_application_t = Table("voucher_application_t", [Column("id", int), Column("user_id", int)])

        select_voucher = Operation("select_voucher",
                                    [InputParameter("voucher_id", int)],
                                    OperationType.READ,
                                    voucher_application_t,
                                    lambda ctx: voucher_application_t.column("id") == ctx["voucher_id"],
                                    ["voucher_id"])

        insert_voucher = Operation("insert_voucher",
                                    [InputParameter("voucher_id", int)],
                                    OperationType.WRITE,
                                    voucher_application_t,
                                    lambda ctx: voucher_application_t.column("id") == ctx["voucher_id"],
                                    ["voucher_id"])

        select_voucher_lt = LocalTransaction([select_voucher], select_voucher.params)
        insert_voucher_lt = LocalTransaction([insert_voucher], insert_voucher.params)

        checkout_bt = BusinessTransaction("checkout",
                                      [select_voucher_lt, insert_voucher_lt],
                                      [InputParameter("voucher_id", int)])

        set_self_loop_budget(2)
        ms = Microservice("oscar", [checkout_bt])

        system = System([ms])

        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, cycle_assertions = get_cycles_and_dag_paths(graph)

        print("System endpoints:")
        for m in system.microservices:
            for bt in m.business_transactions:
                print(bt.business_transaction_units)

        print("Number of cycles", len(cycles))
        for c,a in cycle_assertions.items():
            print("cycle: ", c)
            print("assertions: " + str(a))

        print("Number of topological paths", len(topological_paths_for_cycles[0]))
        print(topological_paths_for_cycles[0][1])