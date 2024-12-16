from domain.domain_model import *

input_id_str = InputParameter("id", str)
input_user_id_str = InputParameter("userId", str)
input_amount_str = InputParameter("amount", str)
input_balance_int = InputParameter("balance", int)

output_score_str = OutputParameter("score", str)
output_balance_int = OutputParameter("balance", int)
output_id_str = OutputParameter("id", str)

def example_helper_for_dsl_parser():

    user_t = Table("userTable", [Column("id", str), Column("username", str), Column("score", str)])

    read_score_op = Operation("readScoreOperation", [input_user_id_str, output_score_str], OperationType.READ, user_t)
    write_score_op = Operation("writeScoreOperation", [input_user_id_str, input_amount_str], OperationType.WRITE, user_t)

    getScore_bt = BusinessTransaction("getScoreBT", [LocalTransaction([read_score_op], [input_user_id_str])], [input_user_id_str])
    updateScore_bt = BusinessTransaction("updateScoreBT", [LocalTransaction([write_score_op], [input_user_id_str, input_amount_str])], [input_user_id_str, input_amount_str])

    client_ms = Microservice("client", [getScore_bt, updateScore_bt])
    system = System([client_ms])

    return system


# exemplo principal das discussoes e parecido ao que esta escrito no relatorio (withdraw e user score etc)
def example_1():
    account_t = Table("accountTable", [Column("id", str), Column("user_id", str), Column("balance", str)])
    user_t = Table("userTable", [Column("id", str), Column("name", str), Column("score", str)])
    
    read_balance_op = Operation("readBalanceOperation", [input_user_id_str, output_balance_int], OperationType.READ, account_t)
    write_balance_op = Operation("writeBalanceOperation", [input_user_id_str, input_balance_int], OperationType.WRITE, account_t)
    read_score_op = Operation("readScoreOperation", [input_user_id_str, output_score_str], OperationType.READ, user_t)
    write_score_op = Operation("writeScoreOperation", [input_user_id_str, input_amount_str], OperationType.WRITE, user_t)
    
    getBalance_bt = BusinessTransaction("getBalanceBT", [LocalTransaction([read_balance_op], [input_user_id_str])], [input_user_id_str])
    getScore_bt = BusinessTransaction("getScoreBT", [LocalTransaction([read_score_op], [input_user_id_str])], [input_user_id_str])
    updateScore_bt = BusinessTransaction("updateScoreBT", [LocalTransaction([write_score_op], [input_user_id_str, input_amount_str])], [input_user_id_str, input_amount_str])
    withdraw_bt = BusinessTransaction("withdrawBT", [RemoteBusinessTransaction(getScore_bt),
                                                     LocalTransaction([read_balance_op, write_balance_op], [input_user_id_str, input_balance_int]),
                                                     RemoteBusinessTransaction(updateScore_bt)], [input_user_id_str, input_balance_int, input_amount_str])
    
    finance_ms = Microservice("finance", [getBalance_bt, withdraw_bt])
    client_ms = Microservice("client", [getScore_bt, updateScore_bt])
    system = System([finance_ms, client_ms])

    return system

# exemplo complexo, generico com read e writes a's e b's.
def example_2():
    a_t = Table("aTable", [Column("id", str)])
    b_t = Table("bTable", [Column("id", str)])
    
    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)
    read_b_op = Operation("readAOperation", [output_id_str], OperationType.READ, b_t)
    write_b_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, b_t)
    
    write_a_b_bt_1 = BusinessTransaction("write_a_b_1", [LocalTransaction([write_a_op], [input_id_str]), LocalTransaction([write_b_op], [
        input_id_str])], [input_id_str])
    write_a_b_bt_2 = BusinessTransaction("write_a_b_2", [LocalTransaction([write_a_op], [input_id_str]), LocalTransaction([write_b_op])], [
        input_id_str])
    read_a_b_bt = BusinessTransaction("read_a_b_2", [LocalTransaction([read_a_op]), LocalTransaction([read_b_op])])
    
    ms = Microservice("MS", [write_a_b_bt_1, write_a_b_bt_2, read_a_b_bt])

    system = System([ms])

    return system

# exemplo do acid rain mas num so microservico
def example_3():
    employee = Table("employee", [Column("id", str)])
    salary = Table("salary", [Column("id", str)])

    read_employee_op = Operation("read_employee_op", [output_id_str], OperationType.READ, employee)
    write_employee_op = Operation("write_employee_op", [input_id_str], OperationType.WRITE, employee)
    write_salary_op = Operation("write_salary_op", [input_id_str], OperationType.WRITE, salary)
    
    add_employee = BusinessTransaction("add_employee", [LocalTransaction([read_employee_op, write_employee_op], [input_id_str])], [input_id_str])
    raise_salary = BusinessTransaction("raise_salary", [LocalTransaction([write_employee_op], [input_id_str]), LocalTransaction([read_employee_op, write_salary_op])], [input_id_str])
    
    ms = Microservice("MS", [add_employee, raise_salary])
    system = System([ms])

    return system

#acid rain em microservicos
def example_4():
    employee = Table("employee", [Column("id", str)])
    salary = Table("salary", [Column("id", str)])

    read_employee_op = Operation("read_employee_op", [output_id_str], OperationType.READ, employee)
    write_employee_op = Operation("write_employee_op", [input_id_str], OperationType.WRITE, employee)
    write_salary_op = Operation("write_salary_op", [input_id_str], OperationType.WRITE, salary)

    write_employee = BusinessTransaction("write_employee", [LocalTransaction([write_employee_op], [input_id_str])], [input_id_str])
    read_employee = BusinessTransaction("read_employee", [LocalTransaction([read_employee_op])])
    add_employee = BusinessTransaction("add_employee", [LocalTransaction([read_employee_op, write_employee_op])])
    raise_salary = BusinessTransaction("raise_salary", [RemoteBusinessTransaction(write_employee), RemoteBusinessTransaction(read_employee), LocalTransaction([write_salary_op], [input_id_str])], [input_id_str])

    employeeMS = Microservice("employeeMS", [read_employee, write_employee, add_employee])
    salaryMS = Microservice("salaryMS", [raise_salary])
    system = System([employeeMS, salaryMS])

    return system

# test non_conflict unit list
def example_5():
    account_t = Table("accountTable", [Column("id", str), Column("user_id", str), Column("balance", int)])
    user_t = Table("userTable", [Column("id", str), Column("name", str), Column("score", str)])

    read_balance_op = Operation("readBalanceOperation", [input_user_id_str, output_balance_int], OperationType.READ, account_t)
    write_balance_op = Operation("writeBalanceOperation", [input_user_id_str, input_balance_int], OperationType.WRITE, account_t)
    read_score_op = Operation("readScoreOperation", [input_user_id_str, output_score_str], OperationType.READ, user_t)
    write_score_op = Operation("writeScoreOperation", [input_user_id_str, input_amount_str], OperationType.WRITE, user_t)

    getBalance_bt = BusinessTransaction("getBalanceBT", [LocalTransaction([read_balance_op], [input_user_id_str])], [input_user_id_str])
    read_score_lt = LocalTransaction([read_score_op], [input_user_id_str])
    getScore_bt = BusinessTransaction("getScoreBT", [read_score_lt], [input_user_id_str])
    updateScore_bt = BusinessTransaction("updateScoreBT", [LocalTransaction([write_score_op], [input_amount_str, input_user_id_str])], [input_amount_str, input_user_id_str])
    get_score_remote_bt = RemoteBusinessTransaction(getScore_bt)
    withdraw_bt = BusinessTransaction("withdrawBT", [get_score_remote_bt,
                                                     LocalTransaction([read_balance_op, write_balance_op], [input_user_id_str, input_balance_int]),
                                                     RemoteBusinessTransaction(updateScore_bt)], [input_user_id_str, input_balance_int, input_amount_str])
    finance_ms = Microservice("finance", [getBalance_bt, withdraw_bt])
    client_ms = Microservice("client", [getScore_bt, updateScore_bt])

    non_conflict = [(get_score_remote_bt, read_score_lt)]
    system = System([finance_ms, client_ms], non_conflict)

    return system

# dag test
def example_6():

    onefivetwo_t = Table("152", [Column("id", str)])
    three_t = Table("3", [Column("id", str)])
    four_t = Table("4", [Column("id", str)])
    six_t = Table("6", [Column("id", str)])

    one_op = Operation("one", [], OperationType.READ, onefivetwo_t)
    two_op = Operation("two", [], OperationType.WRITE, onefivetwo_t)
    five_op = Operation("five", [], OperationType.WRITE, onefivetwo_t)

    three_op = Operation("three", [], OperationType.READ, three_t)
    four_op = Operation("four", [], OperationType.READ, four_t)
    six_op = Operation("five", [], OperationType.READ, six_t)

    a_bt = BusinessTransaction("A_BT",
                               [LocalTransaction([one_op]),
                                LocalTransaction([two_op]),
                                LocalTransaction([three_op])])

    b_bt = BusinessTransaction("B_BT",
                               [LocalTransaction([four_op]),
                                LocalTransaction([five_op]),
                                LocalTransaction([six_op])])

    a_ms = Microservice("A_MS", [a_bt, b_bt])

    system = System([a_ms])

    return system


# exemplo 2 com non conflict 2-6
def example_7():
    a_t = Table("aTable", [Column("id", str)])
    b_t = Table("bTable", [Column("id", str)])

    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)
    read_b_op = Operation("readAOperation", [output_id_str], OperationType.READ, b_t)
    write_b_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, b_t)

    local_transaction_2 = LocalTransaction([write_b_op])

    write_a_b_bt_1 = BusinessTransaction("write_a_b_1",
                                         [LocalTransaction([write_a_op], [input_id_str]), local_transaction_2], [input_id_str])
    write_a_b_bt_2 = BusinessTransaction("write_a_b_2",
                                         [LocalTransaction([write_a_op], [input_id_str]), LocalTransaction([write_b_op])], [input_id_str])

    local_transaction_6 = LocalTransaction([read_b_op])

    read_a_b_bt = BusinessTransaction("read_a_b_2",
                                      [LocalTransaction([read_a_op]), local_transaction_6])
    ms = Microservice("MS", [write_a_b_bt_1, write_a_b_bt_2, read_a_b_bt])

    non_conflict = [(local_transaction_2, local_transaction_6)]
    system = System([ms], non_conflict)

    return system

# 1 pdf scanned
def example_8():
    a_t = Table("aTable", [Column("id", str)])
    b_t = Table("bTable", [Column("id", str)])

    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)
    read_b_op = Operation("readAOperation", [output_id_str], OperationType.READ, b_t)

    local_transaction_1 = LocalTransaction([read_a_op])
    local_transaction_2 = LocalTransaction([read_b_op])
    local_transaction_3 = LocalTransaction([write_a_op], [input_id_str])

    bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2])
    bt_2 = BusinessTransaction("BT2", [local_transaction_3], [input_id_str])

    ms = Microservice("MS", [bt_1, bt_2])

    system = System([ms])

    return system

# 2 pdf scanned
def example_9():
    a_t = Table("aTable", [Column("id", str)])
    b_t = Table("bTable", [Column("id", str)])

    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)
    read_b_op = Operation("readAOperation", [output_id_str], OperationType.READ, b_t)

    local_transaction_1 = LocalTransaction([read_a_op])
    local_transaction_2 = LocalTransaction([read_b_op])
    local_transaction_3 = LocalTransaction([write_a_op], [input_id_str])
    local_transaction_4 = LocalTransaction([write_a_op], [input_id_str])

    bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2])
    bt_2 = BusinessTransaction("BT2", [local_transaction_3], [input_id_str])
    bt_3 = BusinessTransaction("BT3", [local_transaction_4], [input_id_str])

    ms = Microservice("MS", [bt_1, bt_2, bt_3])

    system = System([ms])

    return system


# 3 pdf scanned
def example_10():
    a_t = Table("aTable", [Column("id", str)])
    b_t = Table("bTable", [Column("id", str)])

    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)
    write_b_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, b_t)

    local_transaction_1 = LocalTransaction([read_a_op])
    local_transaction_2 = LocalTransaction([write_b_op], [input_id_str])
    local_transaction_3 = LocalTransaction([read_a_op])
    local_transaction_4 = LocalTransaction([write_a_op], [input_id_str])

    bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2, local_transaction_3], [input_id_str])
    bt_2 = BusinessTransaction("BT2", [local_transaction_4], [input_id_str])

    ms = Microservice("MS", [bt_1, bt_2])

    system = System([ms])

    return system

# 4 pdf scanned
def example_11():
    a_t = Table("aTable", [Column("id", str)])
    b_t = Table("bTable", [Column("id", str)])

    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)
    read_b_op = Operation("readAOperation", [output_id_str], OperationType.READ, b_t)
    write_b_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, b_t)

    local_transaction_1 = LocalTransaction([read_b_op])
    local_transaction_2 = LocalTransaction([write_a_op], [input_id_str])
    local_transaction_3 = LocalTransaction([read_a_op])
    local_transaction_4 = LocalTransaction([write_b_op], [input_id_str])

    bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2], [input_id_str])
    bt_2 = BusinessTransaction("BT2", [local_transaction_3, local_transaction_4], [input_id_str])

    ms = Microservice("MS", [bt_1, bt_2])

    system = System([ms])

    return system

# 4 pdf scanned
def example_aula():
    a_t = Table("aTable", [Column("id", str)])
    b_t = Table("bTable", [Column("id", str)])

    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)
    read_b_op = Operation("readAOperation", [output_id_str], OperationType.READ, b_t)
    write_b_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, b_t)

    local_transaction_1 = LocalTransaction([read_b_op])
    local_transaction_2 = LocalTransaction([write_a_op], [input_id_str])
    local_transaction_3 = LocalTransaction([read_a_op])
    local_transaction_4 = LocalTransaction([write_b_op], [input_id_str])

    bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2], [input_id_str])
    bt_2 = BusinessTransaction("BT2", [local_transaction_3, local_transaction_4], [input_id_str])

    ms = Microservice("MS", [bt_1, bt_2])

    system = System([ms])

    return system

#pdf scanned (oito)
def example_12():
    a_t = Table("aTable", [Column("id", str)])

    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)

    local_transaction_1 = LocalTransaction([read_a_op])
    local_transaction_2 = LocalTransaction([read_a_op])
    local_transaction_4 = LocalTransaction([read_a_op])
    local_transaction_5 = LocalTransaction([read_a_op])
    local_transaction_3 = LocalTransaction([write_a_op], [input_id_str])

    bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2])
    bt_2 = BusinessTransaction("BT2", [local_transaction_3], [input_id_str])
    bt_3 = BusinessTransaction("BT3", [local_transaction_4, local_transaction_5])

    ms = Microservice("MS", [bt_1, bt_2, bt_3])

    system = System([ms])

    return system


# exemplo 5 da aula
def example_simples():
    a_t = Table("aTable", [Column("id", str)])

    read_a_op = Operation("readAOperation", [output_id_str], OperationType.READ, a_t)
    write_a_op = Operation("writeAOperation", [input_id_str], OperationType.WRITE, a_t)
    local_transaction_1 = LocalTransaction([read_a_op])
    local_transaction_2 = LocalTransaction([write_a_op], [input_id_str])

    bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2], [input_id_str])

    ms = Microservice("MS", [bt_1])

    system = System([ms])

    return system