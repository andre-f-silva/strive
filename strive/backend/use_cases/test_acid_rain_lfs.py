from unittest import TestCase

from core.system_processor import verify_cycles, get_cycles_and_dag_paths
from domain.domain_model import *
from parser.system_parser import create_graph_from_system


class Test(TestCase):

    def lightning_fast_shop(self):

        cart_item_t = Table("cart_item_t", [Column("id", int), Column("cart_id", int), Column("product_id", int)])
        order_t = Table("order_t", [Column("id", int)])
        order_items_t = Table("order_items_t", [Column("id", int)])

        insert_to_cart = Operation("insert_to_cart",
                                [InputParameter("product_id", int), InputParameter("amount", int), InputParameter("cart_id", int)],
                                OperationType.WRITE,
                                cart_item_t,
                                [],
                                ["id", "cart_id", "product_id"])

        select_cart_items = Operation("select_cart_items",
                                      [InputParameter("cart_id", int)],
                                      OperationType.READ,
                                      cart_item_t,
                                      lambda ctx: cart_item_t.column("cart_id") == ctx["cart_id"],
                                      ["id", "cart_id", "product_id"])

        insert_order = Operation("insert_order",
                                   [],
                                   OperationType.WRITE,
                                   order_t,
                                   [],
                                   [])

        insert_order_items = Operation("insert_order_items",
                                 [],
                                 OperationType.WRITE,
                                 order_items_t,
                                 [],
                                 [])

        insert_to_cart_lt = LocalTransaction([insert_to_cart], insert_to_cart.params)
        insert_order_lt = LocalTransaction([select_cart_items, insert_order], select_cart_items.params)
        insert_order_items_lt = LocalTransaction([select_cart_items, insert_order_items], select_cart_items.params)

        insert_to_cart_bt = BusinessTransaction("insert_to_cart_bt",
                                      [insert_to_cart_lt],
                                      [InputParameter("amount", int), InputParameter("product_id", int), InputParameter("cart_id", int)])

        order_bt = BusinessTransaction("order_bt",
                                                [insert_order_lt, insert_order_items_lt],
                                                [InputParameter("cart_id", int)])

        ms = Microservice("lfs", [insert_to_cart_bt, order_bt])

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