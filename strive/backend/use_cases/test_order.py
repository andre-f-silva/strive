from unittest import TestCase

from core.system_processor import verify_cycles, get_cycles_and_dag_paths, generate_system_graph_image, \
    generate_system_graph_image_2, set_self_loop_budget
from domain.domain_model import *
from parser.system_parser import create_graph_from_system

order_t = Table("order_t", [Column("id", int), Column("user_id", int), Column("status", bool)])
user_t = Table("user_t", [Column("id", int), Column("available", bool)])
ticket_t = Table("ticket_t", [Column("id", int), Column("user_id", int), Column("status", bool), Column("order_id", bool)])
payment_t = Table("payment_t", [Column("id", int), Column("user_id", int), Column("status", bool), Column("order_id", bool)])

create_order = Operation("create_order",
                         [InputParameter("user_id", int),
                          OutputParameter("order_id", int)],
                         OperationType.WRITE,
                         order_t,
                         [],
                         ["id", "user_id", "status"])

approve_order = Operation("approve_order",
                          [InputParameter("order_id", int)],
                          OperationType.WRITE,
                          order_t,
                          lambda ctx: order_t.column("id") == ctx["order_id"],
                          ["id", "status"])

verify_consumer = Operation("verify_consumer",
                            [InputParameter("user_id", int)],
                            OperationType.READ,
                            user_t,
                            lambda ctx: user_t.column("id") == ctx["user_id"],
                            ["id", "available"])

create_ticket = Operation("create_ticket",
                          [InputParameter("order_id", int), InputParameter("user_id", int),
                           OutputParameter("ticket_id", int)],
                          OperationType.WRITE,
                          ticket_t,
                          [],
                          ["id", "user_id", "status", "order_id"])

authorize_card = Operation("authorize_card",
                           [InputParameter("order_id", int), InputParameter("user_id", int),
                            InputParameter("ticket_id", int), OutputParameter("payment_id", int)],
                           OperationType.WRITE,
                           payment_t,
                           [],
                           ["id", "user_id", "status", "order_id"])

approve_ticket = Operation("approve_ticket",
                           [InputParameter("ticket_id", int)],
                           OperationType.WRITE,
                           ticket_t,
                           lambda ctx: ticket_t.column("id") == ctx["ticket_id"],
                           ["id", "status"])


class Test(TestCase):

    def test_mono(self):

        createOrder = LocalTransaction([create_order, verify_consumer, create_ticket, authorize_card, approve_ticket, approve_order],
                                       [InputParameter("user_id", int)])

        createOrder_bt = BusinessTransaction("createOrder_bt",
                                             [createOrder],
                                             [InputParameter("user_id", int)])

        ms = Microservice("order", [createOrder_bt])

        system = System([ms])

        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        for c in cycles:
            print(c)
        print(paths)
        verify_cycles(topological_paths_for_cycles, system)
        generate_system_graph_image(graph)

    def test_3_ms(self):

        createOrder_lt = LocalTransaction([create_order],
                                       [InputParameter("user_id", int), OutputParameter("order_id", int)])

        verify_consumer_lt = LocalTransaction([verify_consumer], [])

        create_ticket_lt = LocalTransaction([create_ticket], [OutputParameter("ticket_id", int)])

        authorize_card_lt = LocalTransaction([authorize_card], [])

        approve_ticket_lt = LocalTransaction([approve_ticket], [])

        approve_order_lt = LocalTransaction([approve_order], [])

        verify_consumer_bt = InternalBusinessTransaction("verify_consumer_in", [verify_consumer_lt], [])

        create_ticket_bt = InternalBusinessTransaction("create_ticket_in", [create_ticket_lt], [])

        authorize_card_bt = InternalBusinessTransaction("authorize_card_in", [authorize_card_lt], [])

        approve_ticket_bt = InternalBusinessTransaction("approve_ticket_in", [approve_ticket_lt], [])


        createOrder_bt = BusinessTransaction("createOrder_bt",
                                             [createOrder_lt,
                                              RemoteBusinessTransaction(verify_consumer_bt),
                                              RemoteBusinessTransaction(create_ticket_bt),
                                              RemoteBusinessTransaction(authorize_card_bt),
                                              RemoteBusinessTransaction(approve_ticket_bt),
                                              approve_order_lt],
                                             [InputParameter("user_id", int)])

        ms = Microservice("order", [createOrder_bt])

        v = Microservice("v", [verify_consumer_bt])

        system = System([ms, v])

        set_self_loop_budget(2)
        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        for c in cycles:
            print(c)
        print(paths)
        verify_cycles(topological_paths_for_cycles, system)
        generate_system_graph_image(graph)


    def test_coerography(self):

        createOrder_lt = LocalTransaction([create_order],
                                       [InputParameter("user_id", int), OutputParameter("order_id", int)])

        verify_consumer_lt = LocalTransaction([verify_consumer], [])

        create_ticket_lt = LocalTransaction([create_ticket], [OutputParameter("ticket_id", int)])

        authorize_card_lt = LocalTransaction([authorize_card], [])

        approve_ticket_lt = LocalTransaction([approve_ticket], [InputParameter("ticket_id", int)])

        approve_order_lt = LocalTransaction([approve_order], [])

        authorize_card_bt = InternalBusinessTransaction("authorize_card_in", [authorize_card_lt], [])

        #all_lt = LocalTransaction([create_ticket, authorize_card, approve_ticket], [])

        #create_ticket_bt = InternalBusinessTransaction("create_ticket_in", [create_ticket_lt, authorize_card_lt, approve_ticket_lt], [])

        create_ticket_bt = InternalBusinessTransaction("create_ticket_in",
                                                       [create_ticket_lt,
                                                        RemoteBusinessTransaction(authorize_card_bt),
                                                        approve_ticket_lt])

        verify_consumer_bt = InternalBusinessTransaction("verify_consumer_in", [verify_consumer_lt, RemoteBusinessTransaction(create_ticket_bt)], [])


        createOrder_bt = BusinessTransaction("createOrder_bt",
                                             [createOrder_lt,
                                              RemoteBusinessTransaction(verify_consumer_bt),
                                              approve_order_lt],
                                             [InputParameter("user_id", int)])

        order_ms = Microservice("order", [createOrder_bt])

        consumer_ms = Microservice("consumer", [verify_consumer_bt])

        kitchen_ms = Microservice("kitchen", [create_ticket_bt])

        accounting_ms = Microservice("accounting", [authorize_card_bt])

        system = System([order_ms, consumer_ms, kitchen_ms, accounting_ms])

        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        for c in cycles:
            print(c)
        print(paths)
        verify_cycles(topological_paths_for_cycles, system)
        generate_system_graph_image(graph)

    def test_external_ms(self):

        createOrder_lt = LocalTransaction([create_order],
                                       [InputParameter("user_id", int), OutputParameter("order_id", int)])

        verify_consumer_lt = LocalTransaction([verify_consumer], [])

        create_ticket_lt = LocalTransaction([create_ticket], [InputParameter("order_id", int), InputParameter("user_id", int), OutputParameter("ticket_id", int)])

        authorize_card_lt = LocalTransaction([authorize_card], [])

        approve_ticket_lt = LocalTransaction([approve_ticket], [InputParameter("ticket_id", int)])

        approve_order_lt = LocalTransaction([approve_order], [InputParameter("order_id", int)])

        verify_consumer_bt = BusinessTransaction("verify_consumer_in", [verify_consumer_lt], [])

        create_ticket_bt = BusinessTransaction("create_ticket_in", [create_ticket_lt], [InputParameter("order_id", int), InputParameter("user_id", int)])

        authorize_card_bt = BusinessTransaction("authorize_card_in", [authorize_card_lt], [InputParameter("ticket_id", int), InputParameter("user_id", int), InputParameter("order_id", int)])

        approve_ticket_bt = BusinessTransaction("approve_ticket_in", [approve_ticket_lt], [InputParameter("ticket_id", int)])


        createOrder_bt = BusinessTransaction("createOrder_bt",
                                             [createOrder_lt,
                                              RemoteBusinessTransaction(verify_consumer_bt),
                                              RemoteBusinessTransaction(create_ticket_bt),
                                              RemoteBusinessTransaction(authorize_card_bt),
                                              RemoteBusinessTransaction(approve_ticket_bt),
                                              approve_order_lt],
                                             [InputParameter("user_id", int)])

        order_ms = Microservice("order", [createOrder_bt])

        consumer_ms = Microservice("consumer", [verify_consumer_bt])

        kitchen_ms = Microservice("kitchen", [create_ticket_bt, approve_ticket_bt])

        accounting_ms = Microservice("accounting", [authorize_card_bt])

        system = System([order_ms, consumer_ms, kitchen_ms, accounting_ms])

        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        for c in cycles:
            print(c)
        verify_cycles(topological_paths_for_cycles, system)
        generate_system_graph_image(graph)