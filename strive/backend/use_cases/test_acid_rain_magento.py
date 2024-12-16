from unittest import TestCase

from core.system_processor import verify_cycles, get_cycles_and_dag_paths, set_self_loop_budget, \
    generate_system_graph_image
from domain.domain_model import *
from parser.system_parser import create_graph_from_system


class Test(TestCase):

    def test(self):

        catalog_inventory_stock_item_t = Table("catalog_inventory_stock_item", [Column("id", int), Column("stock", int)])

        check_stock = Operation("check_stock",
                                [InputParameter("item_id", int)],
                                OperationType.READ,
                                catalog_inventory_stock_item_t,
                                lambda ctx: catalog_inventory_stock_item_t.column("id") == ctx["item_id"],
                                ["stock"])

        update_stock = Operation("update_stock",
                                [InputParameter("item_id", int)],
                                OperationType.WRITE,
                                catalog_inventory_stock_item_t,
                                lambda ctx: catalog_inventory_stock_item_t.column("id") == ctx["item_id"],
                                ["stock"])

        check_stock_lt = LocalTransaction([check_stock], check_stock.params)
        decrease_stock_lt = LocalTransaction([check_stock, update_stock], check_stock.params)

        checkout_bt = BusinessTransaction("checkout",
                                      [check_stock_lt, decrease_stock_lt],
                                      [InputParameter("item_id", int)])

        set_self_loop_budget(2)
        ms = Microservice("magento", [checkout_bt])


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

        print("Number of topological paths", len(topological_paths_for_cycles[0][1]))
        print(topological_paths_for_cycles[0][1])