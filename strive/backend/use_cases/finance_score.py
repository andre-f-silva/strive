from unittest import TestCase

from core.system_processor import *


class Test(TestCase):

    def test_finance_example(self):

        account_t = Table("accountTable", [Column("id", str), Column("user_id", str), Column("balance", str)])
        user_t = Table("userTable", [Column("id", str), Column("name", str), Column("score", str)])

        read_balance_op = Operation("readBalanceOperation",
                                    [InputParameter("accountId", str), OutputParameter("balance", int)],
                                    OperationType.READ,
                                    account_t,
                                    lambda ctx: account_t.column("id") == ctx["accountId"],
                                    ["id", "balance"])

        write_balance_op = Operation("writeBalanceOperation",
                                     [InputParameter("accountId", str),
                                      InputParameter("updateBalance", int)],
                                     OperationType.WRITE,
                                     account_t,
                                     lambda ctx: account_t.column("id") == ctx["accountId"],
                                     ["balance"])

        read_score_op = Operation("readScoreOperation",
                                  [InputParameter("userId", str), OutputParameter("score", str)],
                                  OperationType.READ,
                                  user_t,
                                  lambda ctx: user_t.column("id") == ctx["userId"],
                                  ["id", "score"])

        write_score_op = Operation("writeScoreOperation",
                                   [InputParameter("userId", str), InputParameter("updateScore", str)],
                                   OperationType.WRITE,
                                   user_t,
                                   lambda ctx: user_t.column("id") == ctx["userId"],
                                   ["score"])

        read_user_id_from_account_id_op = Operation("readUserIdOperation",
                                                    [InputParameter("accountId", str), OutputParameter("userId", str)],
                                                    OperationType.READ,
                                                    account_t,
                                                    lambda ctx: account_t.column("id") == ctx["accountId"],
                                                    ["id"])

        lt_read_balance = LocalTransaction([read_balance_op], [InputParameter("accountId", str)])

        getBalance_bt = BusinessTransaction("getBalanceBT",
                                            [lt_read_balance],
                                            [InputParameter("accountId", str)])

        lt_read_score = LocalTransaction([read_score_op], [InputParameter("userId", str)])
        getScore_bt = BusinessTransaction("getScoreBT",
                                          [lt_read_score],
                                          [InputParameter("userId", str)])

        lt_read_bal_remote = RemoteBusinessTransaction(getBalance_bt)
        lt_write_score = LocalTransaction([write_score_op], [InputParameter("userId", str), InputParameter("updateScore", str)])
        update_score_with_bal_bt = BusinessTransaction("updateScoreWithBalanceBT",
                                             [lt_read_bal_remote, lt_write_score],
                                             [InputParameter("userId", str), InputParameter("updateScore", str)])

        update_score = BusinessTransaction("updateScoreBT",
                                           [lt_write_score],
                                           [InputParameter("userId", str), InputParameter("updateScore", str)])

        lt_get_user_id_from_account_id = LocalTransaction([read_user_id_from_account_id_op], [InputParameter("accountId", str), OutputParameter("userId", str)])
        remote_get_score = RemoteBusinessTransaction(getScore_bt)
        lt_read_write_balance = LocalTransaction([read_balance_op, write_balance_op],
                                                 [InputParameter("accountId", str), InputParameter("updateBalance", int)])
        remote_update_score = RemoteBusinessTransaction(update_score)
        withdraw_bt = BusinessTransaction("withdrawBT",
                                          [lt_get_user_id_from_account_id, remote_get_score, lt_read_write_balance, remote_update_score],
                                          [InputParameter("accountId", str), InputParameter("updateBalance", str), InputParameter("updateScore", str)])

        finance_ms = Microservice("finance", [getBalance_bt, withdraw_bt])
        client_ms = Microservice("client", [getScore_bt, update_score_with_bal_bt])

        system = System([finance_ms, client_ms])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, assertions = get_cycles_and_dag_paths(graph)
        print(cycles)
        verify_cycles(topological_paths_for_cycles, system)

    def test_finance_example_with_hidden_update_score(self):

        account_t = Table("accountTable", [Column("id", str), Column("user_id", str), Column("balance", str)])
        user_t = Table("userTable", [Column("id", str), Column("name", str), Column("score", str)])

        read_balance_op = Operation("readBalanceOperation",
                                    [InputParameter("accountId", str), OutputParameter("balance", int)],
                                    OperationType.READ,
                                    account_t,
                                    lambda ctx: account_t.column("id") == ctx["accountId"],
                                    ["id", "balance"])

        write_balance_op = Operation("writeBalanceOperation",
                                     [InputParameter("accountId", str),
                                      InputParameter("updateBalance", int)],
                                     OperationType.WRITE,
                                     account_t,
                                     lambda ctx: account_t.column("id") == ctx["accountId"],
                                     ["balance"])

        read_score_op = Operation("readScoreOperation",
                                  [InputParameter("userId", str), OutputParameter("score", str)],
                                  OperationType.READ,
                                  user_t,
                                  lambda ctx: user_t.column("id") == ctx["userId"],
                                  ["id", "score"])

        write_score_op = Operation("writeScoreOperation",
                                   [InputParameter("userId", str), InputParameter("updateScore", str)],
                                   OperationType.WRITE,
                                   user_t,
                                   lambda ctx: user_t.column("id") == ctx["userId"],
                                   ["score"])

        read_user_id_from_account_id_op = Operation("readUserIdOperation",
                                                    [InputParameter("accountId", str), OutputParameter("userId", str)],
                                                    OperationType.READ,
                                                    account_t,
                                                    lambda ctx: account_t.column("id") == ctx["accountId"],
                                                    ["id"])

        lt_read_balance = LocalTransaction([read_balance_op], [InputParameter("accountId", str)])

        getBalance_bt = BusinessTransaction("getBalanceBT",
                                            [lt_read_balance],
                                            [InputParameter("accountId", str)])

        lt_read_score = LocalTransaction([read_score_op], [InputParameter("userId", str)])
        getScore_bt = BusinessTransaction("getScoreBT",
                                          [lt_read_score],
                                          [InputParameter("userId", str)])

        lt_write_score = LocalTransaction([write_score_op], [InputParameter("userId", str), InputParameter("updateScore", str)])

        update_score = InternalBusinessTransaction("updateScoreBT_INTERNAL",
                                                   [lt_write_score],
                                                   [InputParameter("userId", str), InputParameter("updateScore", str)])

        lt_read_bal_remote = RemoteBusinessTransaction(getBalance_bt)
        remote_updateScore = RemoteBusinessTransaction(update_score)
        update_score_with_bal_bt = BusinessTransaction("updateScoreWithBalanceBT",
                                                       [lt_read_bal_remote, remote_updateScore],
                                                       [InputParameter("userId", str), InputParameter("updateScore", str)])

        lt_get_user_id_from_account_id = LocalTransaction([read_user_id_from_account_id_op], [InputParameter("accountId", str), OutputParameter("userId", str)])
        remote_get_score = RemoteBusinessTransaction(getScore_bt)
        lt_read_write_balance = LocalTransaction([read_balance_op, write_balance_op],
                                                 [InputParameter("accountId", str), InputParameter("updateBalance", int)])
        remote_update_score = RemoteBusinessTransaction(update_score)
        withdraw_bt = BusinessTransaction("withdrawBT",
                                          [lt_get_user_id_from_account_id, remote_get_score, lt_read_write_balance, remote_update_score],
                                          [InputParameter("accountId", str), InputParameter("updateBalance", str), InputParameter("updateScore", str)])

        finance_ms = Microservice("finance", [getBalance_bt, withdraw_bt])
        client_ms = Microservice("client", [getScore_bt, update_score_with_bal_bt, update_score])

        system = System([finance_ms, client_ms])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)
        verify_cycles(topological_paths_for_cycles, system)
