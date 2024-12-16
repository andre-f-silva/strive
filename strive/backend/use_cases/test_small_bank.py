import time
from unittest import TestCase

from core.system_processor import get_cycles_and_dag_paths, verify_cycles
from domain.domain_model import *
from parser.system_parser import create_graph_from_system

non_conflict = []

accounts_t = Table("accounts_t", [Column("A_ID", int), Column("A_CUSTOMER_ID", int)])
savings_t = Table("accounts_t", [Column("S_CUSTOMER_ID", int), Column("S_BAL", int)])
checking_t = Table("accounts_t", [Column("C_CUSTOMER_ID", int), Column("C_BAL", int)])

def amalgamate():

    GetAccount = Operation("GetAccount",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           accounts_t,
                           lambda ctx: accounts_t.column("A_CUSTOMER_ID") == ctx["C_ID"],
                           ["A_CUSTOMER_ID", "A_ID"])

    GetSavingsBalance = Operation("GetSavingsBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           savings_t,
                           lambda ctx: savings_t.column("S_CUSTOMER_ID") == ctx["C_ID"],
                           ["S_BAL"])

    GetCheckingBalance = Operation("GetCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    UpdateSavingsBalance = Operation("UpdateSavingsBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.WRITE,
                           savings_t,
                           lambda ctx: savings_t.column("S_CUSTOMER_ID") == ctx["C_ID"],
                           ["S_BAL"])

    UpdateCheckingBalance = Operation("UpdateCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.WRITE,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    ZeroCheckingBalance = Operation("ZeroCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.WRITE,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    GetAccount_lt = LocalTransaction([GetAccount], GetAccount.params)
    GetSavingsBalance_lt = LocalTransaction([GetSavingsBalance], GetSavingsBalance.params)
    GetCheckingBalance_lt = LocalTransaction([GetCheckingBalance], GetCheckingBalance.params)
    UpdateSavingsBalance_lt = LocalTransaction([UpdateSavingsBalance], UpdateSavingsBalance.params)
    UpdateCheckingBalance_lt = LocalTransaction([UpdateCheckingBalance], UpdateCheckingBalance.params)
    ZeroCheckingBalance_lt = LocalTransaction([ZeroCheckingBalance], ZeroCheckingBalance.params)

    return BusinessTransaction("amalgamate_bt",
                               [GetAccount_lt, GetSavingsBalance_lt, GetCheckingBalance_lt,
                                UpdateSavingsBalance_lt, UpdateCheckingBalance_lt, ZeroCheckingBalance_lt],
                               [InputParameter("C_ID", int)])

def balance():

    GetAccount = Operation("GetAccount",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           accounts_t,
                           lambda ctx: accounts_t.column("A_CUSTOMER_ID") == ctx["C_ID"],
                           ["A_CUSTOMER_ID", "A_ID"])

    GetSavingsBalance = Operation("GetSavingsBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           savings_t,
                           lambda ctx: savings_t.column("S_CUSTOMER_ID") == ctx["C_ID"],
                           ["S_BAL"])

    GetCheckingBalance = Operation("GetCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    GetAccount_lt = LocalTransaction([GetAccount], GetAccount.params)
    GetSavingsBalance_lt = LocalTransaction([GetSavingsBalance], GetSavingsBalance.params)
    GetCheckingBalance_lt = LocalTransaction([GetCheckingBalance], GetCheckingBalance.params)

    return BusinessTransaction("balance_bt",
                               [GetAccount_lt, GetSavingsBalance_lt, GetCheckingBalance_lt],
                               [InputParameter("C_ID", int)])

def deposit_checking():

    GetAccount = Operation("GetAccount",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           accounts_t,
                           lambda ctx: accounts_t.column("A_CUSTOMER_ID") == ctx["C_ID"],
                           ["A_CUSTOMER_ID", "A_ID"])

    UpdateCheckingBalance = Operation("UpdateCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.WRITE,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    GetAccount_lt = LocalTransaction([GetAccount], GetAccount.params)
    UpdateCheckingBalance_lt = LocalTransaction([UpdateCheckingBalance], UpdateCheckingBalance.params)

    return BusinessTransaction("balance_bt",
                               [GetAccount_lt, UpdateCheckingBalance_lt],
                               [InputParameter("C_ID", int)])

def send_payment():

    GetAccount = Operation("GetAccount",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           accounts_t,
                           lambda ctx: accounts_t.column("A_CUSTOMER_ID") == ctx["C_ID"],
                           ["A_CUSTOMER_ID", "A_ID"])

    GetCheckingBalance = Operation("GetCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    UpdateCheckingBalance = Operation("UpdateCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.WRITE,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    GetAccount_lt = LocalTransaction([GetAccount], GetAccount.params)
    GetCheckingBalance_lt = LocalTransaction([GetCheckingBalance], GetCheckingBalance.params)
    UpdateCheckingBalance_lt = LocalTransaction([UpdateCheckingBalance], UpdateCheckingBalance.params)

    return BusinessTransaction("send_payment_bt",
                               [GetAccount_lt, GetCheckingBalance_lt, UpdateCheckingBalance_lt],
                               [InputParameter("C_ID", int)])

def transaction_savings():

    GetAccount = Operation("GetAccount",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           accounts_t,
                           lambda ctx: accounts_t.column("A_CUSTOMER_ID") == ctx["C_ID"],
                           ["A_CUSTOMER_ID", "A_ID"])

    GetSavingsBalance = Operation("GetSavingsBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           savings_t,
                           lambda ctx: savings_t.column("S_CUSTOMER_ID") == ctx["C_ID"],
                           ["S_BAL"])

    UpdateSavingsBalance = Operation("UpdateSavingsBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.WRITE,
                           savings_t,
                           lambda ctx: savings_t.column("S_CUSTOMER_ID") == ctx["C_ID"],
                           ["S_BAL"])

    GetAccount_lt = LocalTransaction([GetAccount], GetAccount.params)
    GetSavingsBalance_lt = LocalTransaction([GetSavingsBalance], GetSavingsBalance.params)
    UpdateSavingsBalance_lt = LocalTransaction([UpdateSavingsBalance], UpdateSavingsBalance.params)

    return BusinessTransaction("send_payment_bt",
                               [GetAccount_lt, GetSavingsBalance_lt, UpdateSavingsBalance_lt],
                               [InputParameter("C_ID", int)])

def write_check():

    GetAccount = Operation("GetAccount",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           accounts_t,
                           lambda ctx: accounts_t.column("A_CUSTOMER_ID") == ctx["C_ID"],
                           ["A_CUSTOMER_ID", "A_ID"])

    GetSavingsBalance = Operation("GetSavingsBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           savings_t,
                           lambda ctx: savings_t.column("S_CUSTOMER_ID") == ctx["C_ID"],
                           ["S_BAL"])

    GetCheckingBalance = Operation("GetCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.READ,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    UpdateCheckingBalance = Operation("UpdateCheckingBalance",
                           [InputParameter("C_ID", int)],
                           OperationType.WRITE,
                           checking_t,
                           lambda ctx: checking_t.column("C_CUSTOMER_ID") == ctx["C_ID"],
                           ["C_BAL"])

    GetAccount_lt = LocalTransaction([GetAccount], GetAccount.params)
    GetSavingsBalance_lt = LocalTransaction([GetSavingsBalance], GetSavingsBalance.params)
    GetCheckingBalance_lt = LocalTransaction([GetCheckingBalance], GetCheckingBalance.params)
    UpdateCheckingBalance_lt = LocalTransaction([UpdateCheckingBalance], UpdateCheckingBalance.params)

    return BusinessTransaction("send_payment_bt",
                               [GetAccount_lt, GetSavingsBalance_lt, GetCheckingBalance_lt, UpdateCheckingBalance_lt],
                               [InputParameter("C_ID", int)])

class Test(TestCase):

    def test(self):
        print(time.process_time())

        amalgamate_bt = amalgamate()

        balance_bt = balance()

        deposit_checking_bt = deposit_checking()

        send_payment_bt = send_payment()

        transaction_savings_bt = transaction_savings()

        write_check_bt = write_check()

        ms = Microservice("ms", [amalgamate_bt, balance_bt, deposit_checking_bt, send_payment_bt, transaction_savings_bt, write_check_bt])

        system = System([ms], non_conflict)

        graph = create_graph_from_system(system)
        print("nodes: " + str(len(graph.nodes())))

        cycles, paths, topological_paths_for_cycles = get_cycles_and_dag_paths(graph)

        # print(cycles)
        # print(len(topological_paths_for_cycles))
        # print(topological_paths_for_cycles)
        verify_cycles(topological_paths_for_cycles, system)