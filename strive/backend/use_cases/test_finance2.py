from unittest import TestCase

from core.system_processor import *

class Test(TestCase):

    def test(self):

        account_t = Table("account", [Column("id", int), Column("customer_id", int), Column("balance", int)])
        customer_credit_t = Table("customer_credit", [Column("id", int), Column("customer_id", int), Column("rating", int)])

        write_balance_op = Operation("writeBalanceOperation",
                                     [InputParameter("customer_id", int),
                                      InputParameter("updateBalance", int)],
                                     OperationType.WRITE,
                                     account_t,
                                     lambda ctx: account_t.column("customer_id") == ctx["customer_id"],
                                     ["balance"])

        read_credit_op = Operation("readCredit",
                                   [InputParameter("customer_id", int), OutputParameter("rating", int)],
                                   OperationType.READ,
                                   customer_credit_t,
                                   lambda ctx: customer_credit_t.column("customer_id") == ctx["customer_id"],
                                   ["rating"])

        write_credit_op = Operation("writeCredit",
                                   [InputParameter("customer_id", int)],
                                   OperationType.WRITE,
                                   customer_credit_t,
                                   lambda ctx: customer_credit_t.column("customer_id") == ctx["customer_id"],
                                   ["rating"])

        lt_write_balance_op = LocalTransaction([write_balance_op], [InputParameter("customer_id", int), InputParameter("updateBalance", int)])

        lt_read_credit_op = LocalTransaction([read_credit_op], [InputParameter("customer_id", int), OutputParameter("rating", int)])

        lt_write_credit_op = LocalTransaction([write_credit_op], [InputParameter("customer_id", int)])

        readCustomerCreditRating = BusinessTransaction("readCustomerCreditRating", [lt_read_credit_op], [InputParameter("customer_id", int), OutputParameter("rating", int)])

        writeCustomerCreditRating = BusinessTransaction("writeCustomerCreditRating", [lt_write_credit_op], [InputParameter("customer_id", int), InputParameter("updateBalance", int)])

        withdraw = BusinessTransaction("withdraw",
                                       [RemoteBusinessTransaction(readCustomerCreditRating), lt_write_balance_op, RemoteBusinessTransaction(writeCustomerCreditRating)],
                                       [InputParameter("customer_id", int), InputParameter("updateBalance", int)])

        finance_ms = Microservice("finance", [withdraw])
        client_ms = Microservice("client", [readCustomerCreditRating, writeCustomerCreditRating])

        system = System([finance_ms, client_ms])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, assertions = get_cycles_and_dag_paths(graph)
        for c in cycles:
            print(c)
        verify_cycles(topological_paths_for_cycles, system)
