import time
from unittest import TestCase

from core.system_processor import verify_cycles, get_cycles_and_dag_paths
from domain.domain_model import *
from parser.system_parser import create_graph_from_system

non_conflict = []

new_order_t = Table("new_order_t", [Column("NO_O_ID", int), Column("NO_D_ID", int), Column("NO_W_ID", int)])

open_order_t = Table("open_order_t", [Column("O_ID", int), Column("O_D_ID", int), Column("O_W_ID", int), Column("O_C_ID", int), Column("O_CARRIER_ID", int), Column("O_ENTRY_D", int)])

order_line_t = Table("order_line_t", [Column("OL_O_ID", int), Column("OL_D_ID", int), Column("OL_W_ID", int), Column("OL_DELIVERY_D", int), Column("OL_I_ID", int), Column("OL_SUPPLY_W_ID", int), Column("OL_QUANTITY", int), Column("OL_AMOUNT", int)])

customer_t = Table("customer_t", [Column("C_BALANCE", int), Column("C_DELIVERY_CNT", int), Column("C_W_ID", int), Column("C_D_ID", int), Column("C_ID", int)])

district_t = Table("district_t", [Column("D_NEXT_O_ID", int), Column("D_W_ID", int), Column("D_ID", int)])

stock_t = Table("stock_t", [Column("S_I_ID", int), Column("S_W_ID", int), Column("S_QUANTITY", int)])

warehouse_t = Table("warehouse_t", [Column("W_TAX", int), Column("W_ID", int)])

item_t = Table("item_t", [Column("I_PRICE", int), Column("I_NAME", int), Column("I_ID", int), Column("I_DATA", int)])

history_t = Table("history_t", [Column("H_C_D_ID", int), Column("H_C_W_ID", int), Column("H_C_ID", int), Column("H_D_ID", int), Column("H_W_ID", int), Column("H_DATE", int), Column("H_AMOUNT", int), Column("H_DATA", int)])

def get_delivery_bt():
    delivGetOrderIdSQL = Operation("delivGetOrderIdSQL",
                                   [InputParameter("D_ID", int), InputParameter("W_ID", int),
                                    OutputParameter("NO_O_ID", int)],
                                   OperationType.READ,
                                   new_order_t,
                                   lambda ctx: z3.And(new_order_t.column("NO_D_ID") == ctx["D_ID"],
                                                      new_order_t.column("NO_W_ID") == ctx["W_ID"]),
                                   ["NO_O_ID"])

    delivDeleteNewOrderSQL = Operation("delivDeleteNewOrderSQL",
                                       [InputParameter("NO_O_ID", int), InputParameter("D_ID", int),
                                        InputParameter("W_ID", int)],
                                       OperationType.WRITE,
                                       new_order_t,
                                       lambda ctx: z3.And(new_order_t.column("NO_D_ID") == ctx["D_ID"],
                                                          new_order_t.column("NO_W_ID") == ctx["W_ID"],
                                                          new_order_t.column("NO_O_ID") == ctx["NO_O_ID"]),
                                       ["NO_D_ID", "NO_W_ID", "NO_O_ID"])

    delivGetCustIdSQL = Operation("delivGetCustIdSQL",
                                  [InputParameter("NO_O_ID", int), InputParameter("D_ID", int),
                                   InputParameter("W_ID", int),
                                   OutputParameter("O_C_ID", int)],
                                  OperationType.READ,
                                  open_order_t,
                                  lambda ctx: z3.And(open_order_t.column("O_ID") == ctx["NO_O_ID"],
                                                     open_order_t.column("O_D_ID") == ctx["D_ID"],
                                                     open_order_t.column("O_W_ID") == ctx["W_ID"]),
                                  ["O_C_ID"])

    delivUpdateCarrierIdSQL = Operation("delivUpdateCarrierIdSQL",
                                        [InputParameter("NO_O_ID", int), InputParameter("D_ID", int),
                                         InputParameter("W_ID", int)],
                                        OperationType.WRITE,
                                        open_order_t,
                                        lambda ctx: z3.And(open_order_t.column("O_ID") == ctx["NO_O_ID"],
                                                           open_order_t.column("O_D_ID") == ctx["D_ID"],
                                                           open_order_t.column("O_W_ID") == ctx["W_ID"]),
                                        ["O_ID", "O_D_ID", "O_W_ID"])

    delivUpdateDeliveryDateSQL = Operation("delivUpdateDeliveryDateSQL",
                                           [InputParameter("NO_O_ID", int),
                                            InputParameter("D_ID", int), InputParameter("W_ID", int)],
                                           OperationType.WRITE,
                                           order_line_t,
                                           lambda ctx: z3.And(order_line_t.column("OL_O_ID") == ctx["NO_O_ID"],
                                                              order_line_t.column("OL_D_ID") == ctx["D_ID"],
                                                              order_line_t.column("OL_W_ID") == ctx["W_ID"]),
                                           ["OL_DELIVERY_D", "OL_O_ID", "OL_D_ID", "OL_W_ID"])

    delivSumOrderAmountSQL = Operation("delivSumOrderAmountSQL",
                                       [InputParameter("NO_O_ID", int),
                                        InputParameter("D_ID", int), InputParameter("W_ID", int),
                                        OutputParameter("OL_TOTAL", int)],
                                       OperationType.READ,
                                       order_line_t,
                                       lambda ctx: z3.And(order_line_t.column("OL_O_ID") == ctx["NO_O_ID"],
                                                          order_line_t.column("OL_D_ID") == ctx["D_ID"],
                                                          order_line_t.column("OL_W_ID") == ctx["W_ID"]),
                                       ["OL_O_ID", "OL_D_ID", "OL_W_ID"])

    delivUpdateCustBalDelivCntSQL = Operation("delivUpdateCustBalDelivCntSQL",
                                              [InputParameter("OL_TOTAL", int),
                                               InputParameter("W_ID", int), InputParameter("D_ID", int),
                                               InputParameter("O_C_ID", int)],
                                              OperationType.WRITE,
                                              customer_t,
                                              lambda ctx: z3.And(customer_t.column("C_W_ID") == ctx["W_ID"],
                                                                 customer_t.column("C_D_ID") == ctx["D_ID"],
                                                                 customer_t.column("C_ID") == ctx["O_C_ID"],
                                                                 ctx["OL_TOTAL"] == ctx["OL_TOTAL"]),
                                              ["C_DELIVERY_CNT", "C_W_ID", "C_D_ID", "C_ID", "C_BALANCE"])

    delivGetOrderIdSQL_lt = LocalTransaction([delivGetOrderIdSQL], delivGetOrderIdSQL.params)
    delivDeleteNewOrderSQL_lt = LocalTransaction([delivDeleteNewOrderSQL], delivDeleteNewOrderSQL.params)
    delivGetCustIdSQL_lt = LocalTransaction([delivGetCustIdSQL], delivGetCustIdSQL.params)
    delivUpdateCarrierIdSQL_lt = LocalTransaction([delivUpdateCarrierIdSQL], delivUpdateCarrierIdSQL.params)
    delivUpdateDeliveryDateSQL_lt = LocalTransaction([delivUpdateDeliveryDateSQL], delivUpdateDeliveryDateSQL.params)
    delivSumOrderAmountSQL_lt = LocalTransaction([delivSumOrderAmountSQL], delivSumOrderAmountSQL.params)
    delivUpdateCustBalDelivCntSQL_lt = LocalTransaction([delivUpdateCustBalDelivCntSQL],
                                                        delivUpdateCustBalDelivCntSQL.params)

    non_conflict.extend([(delivSumOrderAmountSQL_lt, delivUpdateDeliveryDateSQL_lt)])

    return BusinessTransaction("delivery_bt",
                               [delivGetOrderIdSQL_lt, delivDeleteNewOrderSQL_lt, delivGetCustIdSQL_lt,
                                delivUpdateCarrierIdSQL_lt, delivUpdateDeliveryDateSQL_lt, delivSumOrderAmountSQL_lt,
                                delivUpdateCustBalDelivCntSQL_lt],
                               [InputParameter("D_ID", int), InputParameter("W_ID", int)])


def get_stock_level():
    stockGetDistOrderIdSQL = Operation("stockGetDistOrderIdSQL",
                                       [InputParameter("D_ID", int), InputParameter("W_ID", int),
                                        OutputParameter("D_NEXT_O_ID", int)],
                                       OperationType.READ,
                                       district_t,
                                       lambda ctx: z3.And(new_order_t.column("NO_D_ID") == ctx["D_ID"],
                                                          new_order_t.column("NO_W_ID") == ctx["W_ID"]),
                                       ["D_NEXT_O_ID"])

    stockGetCountStockSQL1 = Operation("stockGetCountStockSQL1",
                                       [InputParameter("D_NEXT_O_ID", int), InputParameter("D_ID", int), InputParameter("W_ID", int), InputParameter("threshold", int),
                                        OutputParameter("OL_I_ID", int)],
                                       OperationType.READ,
                                       order_line_t,
                                       lambda ctx: z3.And(order_line_t.column("OL_W_ID") == ctx["W_ID"],
                                                          order_line_t.column("OL_D_ID") == ctx["D_ID"],
                                                          order_line_t.column("OL_O_ID") < ctx["D_NEXT_O_ID"],
                                                          order_line_t.column("OL_O_ID") >= ctx["D_NEXT_O_ID"] - 20),
                                       ["OL_I_ID"])

    stockGetCountStockSQL2 = Operation("stockGetCountStockSQL2",
                                       [InputParameter("OL_I_ID", int), InputParameter("W_ID", int), InputParameter("threshold", int),
                                        OutputParameter("STOCK_COUNT", int)],
                                       OperationType.READ,
                                       stock_t,
                                       lambda ctx: z3.And(stock_t.column("S_W_ID") == ctx["W_ID"],
                                                          stock_t.column("S_I_ID") == ctx["OL_I_ID"],
                                                          stock_t.column("S_QUANTITY") < ctx["threshold"]),
                                       [])

    stockGetDistOrderIdSQL_lt = LocalTransaction([stockGetDistOrderIdSQL], stockGetDistOrderIdSQL.params)
    stockGetCountStockSQL_lt = LocalTransaction([stockGetCountStockSQL1, stockGetCountStockSQL2],
                                                [InputParameter("D_NEXT_O_ID", int),
                                                 InputParameter("D_ID", int),
                                                 InputParameter("W_ID", int),
                                                 InputParameter("threshold", int)])

    return BusinessTransaction("stock_level_bt",
                               [stockGetDistOrderIdSQL_lt, stockGetCountStockSQL_lt],
                               [InputParameter("D_ID", int), InputParameter("W_ID", int), InputParameter("threshold", int)])


def get_order_status():
    payGetCustSQL = Operation("payGetCustSQL",
                              [InputParameter("W_ID", int), InputParameter("D_ID", int), InputParameter("C_ID", int),
                               OutputParameter("C_FIRST", int), OutputParameter("C_MIDDLE", int),
                               OutputParameter("C_LAST", int), OutputParameter("C_STREET_1", int),
                               OutputParameter("C_STREET_2", int), OutputParameter("C_CITY", int),
                               OutputParameter("C_STATE", int), OutputParameter("C_ZIP", int),
                               OutputParameter("C_PHONE", int), OutputParameter("C_CREDIT", int),
                               OutputParameter("C_CREDIT_LIM", int), OutputParameter("C_DISCOUNT", int),
                               OutputParameter("C_BALANCE", int), OutputParameter("C_YTD_PAYMENT", int),
                               OutputParameter("C_PAYMENT_CNT", int), OutputParameter("C_SINCE", int)],
                              OperationType.READ,
                              customer_t,
                              lambda ctx: z3.And(customer_t.column("C_W_ID") == ctx["W_ID"],
                                                 customer_t.column("C_D_ID") == ctx["D_ID"],
                                                 customer_t.column("C_ID") == ctx["C_ID"]),
                              ["C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2",
                               "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_CREDIT", "C_CREDIT_LIM",
                               "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_SINCE"])

    ordStatGetNewestOrdSQL = Operation("ordStatGetNewestOrdSQL",
                                       [InputParameter("D_ID", int), InputParameter("W_ID", int),
                                        OutputParameter("O_ID", int), OutputParameter("O_CARRIER_ID", int), OutputParameter("O_ENTRY_D", int)],
                                       OperationType.READ,
                                       open_order_t,
                                       lambda ctx: z3.And(open_order_t.column("O_W_ID") == ctx["D_ID"],
                                                          open_order_t.column("O_D_ID") == ctx["W_ID"],
                                                          open_order_t.column("O_C_ID") == ctx["W_ID"]),
                                       ["O_ID", "O_CARRIER_ID", "O_ENTRY_D"])

    ordStatGetOrderLinesSQL = Operation("ordStatGetOrderLinesSQL",
                                        [InputParameter("D_ID", int), InputParameter("W_ID", int),
                                         OutputParameter("OL_I_ID", int), OutputParameter("OL_SUPPLY_W_ID", int),
                                         OutputParameter("OL_QUANTITY", int), OutputParameter("OL_AMOUNT", int),
                                         OutputParameter("OL_DELIVERY_D", int)],
                                        OperationType.READ,
                                        order_line_t,
                                        lambda ctx: z3.And(order_line_t.column("OL_O_ID") == ctx["D_ID"],
                                                           order_line_t.column("OL_D_ID") == ctx["W_ID"],
                                                           order_line_t.column("OL_W_ID") == ctx["W_ID"]),
                                        ["OL_I_ID", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_AMOUNT", "OL_DELIVERY_D"])

    payGetCustSQL_lt = LocalTransaction([payGetCustSQL], payGetCustSQL.params)
    ordStatGetNewestOrdSQL_lt = LocalTransaction([ordStatGetNewestOrdSQL], ordStatGetNewestOrdSQL.params)
    ordStatGetOrderLinesSQL_lt = LocalTransaction([ordStatGetOrderLinesSQL], ordStatGetOrderLinesSQL.params)

    return BusinessTransaction("order_status_bt",
                               [payGetCustSQL_lt, ordStatGetNewestOrdSQL_lt, ordStatGetOrderLinesSQL_lt],
                               [InputParameter("W_ID", int), InputParameter("D_ID", int), InputParameter("C_ID", int)])

def get_new_order():

    stmtGetCustSQL = Operation("stmtGetCustSQL",
                                   [InputParameter("D_ID", int), InputParameter("W_ID", int), InputParameter("C_ID", int),
                                    OutputParameter("C_DISCOUNT", int), OutputParameter("C_LAST", int), OutputParameter("C_CREDIT", int)],
                                   OperationType.READ,
                                   customer_t,
                                   lambda ctx: z3.And(customer_t.column("C_W_ID") == ctx["W_ID"],
                                                      customer_t.column("C_D_ID") == ctx["D_ID"],
                                                      customer_t.column("C_ID") == ctx["C_ID"]),
                                   ["C_DISCOUNT", "C_LAST", "C_CREDIT"])

    stmtGetWhseSQL = Operation("stmtGetWhseSQL",
                               [InputParameter("W_ID", int), OutputParameter("W_TAX", int)],
                               OperationType.READ,
                               warehouse_t,
                               lambda ctx: z3.And(warehouse_t.column("W_ID") == ctx["W_ID"]),
                               ["W_TAX"])

    stmtGetDistSQL = Operation("stmtGetDistSQL",
                               [InputParameter("W_ID", int), InputParameter("D_ID", int),
                                OutputParameter("D_NEXT_O_ID", int)],
                               OperationType.READ,
                               district_t,
                               lambda ctx: z3.And(district_t.column("D_W_ID") == ctx["W_ID"],
                                                  district_t.column("D_ID") == ctx["D_ID"]),
                               ["D_W_ID", "D_ID"])

    stmtUpdateDistSQL = Operation("stmtUpdateDistSQL",
                                   [InputParameter("W_ID", int), InputParameter("D_ID", int)],
                                   OperationType.WRITE,
                                   district_t,
                                  lambda ctx: z3.And(district_t.column("D_W_ID") == ctx["W_ID"],
                                                     district_t.column("D_ID") == ctx["D_ID"]),
                                   ["D_NEXT_O_ID"])

    stmtInsertOOrderSQL = Operation("stmtInsertOOrderSQL",
                                    [InputParameter("W_ID", int), InputParameter("D_ID", int),
                                     InputParameter("D_NEXT_O_ID", int), InputParameter("C_ID", int)],
                                    OperationType.WRITE,
                                    open_order_t,
                                    lambda ctx: open_order_t.column("O_ID") == ctx["D_NEXT_O_ID"],
                                    ["O_ID", "O_D_ID", "O_W_ID", "O_C_ID", "O_ENTRY_D"])

    stmtInsertNewOrderSQL = Operation("stmtInsertNewOrderSQL",
                                   [InputParameter("W_ID", int), InputParameter("D_ID", int),
                                    InputParameter("D_NEXT_O_ID", int)],
                                   OperationType.WRITE,
                                   new_order_t,
                                   lambda ctx: new_order_t.column("NO_O_ID") == ctx["D_NEXT_O_ID"],
                                   ["NO_O_ID", "NO_D_ID", "NO_W_ID"])

    stmtGetItemSQL = Operation("stmtGetItemSQL",
                                  [InputParameter("I_ID", int), OutputParameter("I_PRICE", int)],
                                  OperationType.READ,
                                  item_t,
                                  lambda ctx: item_t.column("I_ID") == ctx["I_ID"],
                                  ["I_NAME", "I_PRICE", "I_DATA"])

    stmtGetStockSQL = Operation("stmtGetStockSQL",
                                  [InputParameter("OL_I_ID", int), InputParameter("OL_SUPPLY_W_ID", int),
                                   OutputParameter("S_QUANTITY", int), OutputParameter("S_DATA", int),
                                   OutputParameter("S_DIST_01", int)],
                                  OperationType.READ,
                                  item_t,
                                  lambda ctx: z3.And(item_t.column("S_I_ID") == ctx["I_ID"],
                                                    item_t.column("S_W_ID") == ctx["I_ID"]),
                                  ["S_QUANTITY", "S_DATA", "S_DIST_01"])

    stmtInsertOrderLineSQL = Operation("stmtInsertOrderLineSQL",
                                    [InputParameter("D_NEXT_O_ID", int), InputParameter("D_ID", int),
                                     InputParameter("W_ID", int),
                                     InputParameter("I_ID", int), InputParameter("OL_SUPPLY_W_ID", int),
                                     InputParameter("OL_QUANTITY", int)],
                                    OperationType.WRITE,
                                    order_line_t,
                                    lambda ctx: True,
                                    ["OL_O_ID", "OL_D_ID", "OL_W_ID", "OL_NUMBER", "OL_I_ID", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_AMOUNT", "OL_DIST_INFO"])

    stmtUpdateStockSQL = Operation("stmtUpdateStockSQL",
                                      [InputParameter("I_ID", int), InputParameter("W_ID", int)],
                                      OperationType.WRITE,
                                      stock_t,
                                      lambda ctx: z3.And(stock_t.column("S_I_ID") == ctx["I_ID"],
                                                        stock_t.column("S_W_ID") == ctx["I_ID"]),
                                      ["S_QUANTITY", "S_DATA", "S_DIST_01"])

    stmtGetCustSQL_lt = LocalTransaction([stmtGetCustSQL], stmtGetCustSQL.params)
    stmtGetWhseSQL_lt = LocalTransaction([stmtGetWhseSQL], stmtGetWhseSQL.params)
    stmtGetDistSQL_lt = LocalTransaction([stmtGetDistSQL], stmtGetDistSQL.params)
    stmtUpdateDistSQL_lt = LocalTransaction([stmtUpdateDistSQL], stmtUpdateDistSQL.params)
    stmtInsertOOrderSQL_lt = LocalTransaction([stmtInsertOOrderSQL], stmtInsertOOrderSQL.params)
    stmtInsertNewOrderSQL_lt = LocalTransaction([stmtInsertNewOrderSQL], stmtInsertNewOrderSQL.params)
    stmtGetItemSQL_lt = LocalTransaction([stmtGetItemSQL], stmtGetItemSQL.params)
    stmtGetStockSQL_lt = LocalTransaction([stmtGetStockSQL], stmtGetStockSQL.params)
    stmtInsertOrderLineSQL_lt = LocalTransaction([stmtInsertOrderLineSQL], stmtInsertOrderLineSQL.params)
    stmtUpdateStockSQL_lt = LocalTransaction([stmtUpdateStockSQL], stmtUpdateStockSQL.params)

    return BusinessTransaction("new_order_bt",
                               [stmtGetCustSQL_lt, stmtGetWhseSQL_lt, stmtGetDistSQL_lt, stmtUpdateDistSQL_lt,
                                stmtInsertOOrderSQL_lt, stmtInsertNewOrderSQL_lt, stmtGetItemSQL_lt, stmtGetStockSQL_lt,
                                stmtInsertOrderLineSQL_lt, stmtUpdateStockSQL_lt],
                               [InputParameter("W_ID", int), InputParameter("D_ID", int), InputParameter("C_ID", int),
                                InputParameter("I_ID", int), InputParameter("OL_SUPPLY_W_ID", int), InputParameter("OL_QUANTITY", int)])

def get_payment():

    payUpdateWhseSQL = Operation("payUpdateWhseSQL",
                                 [InputParameter("W_ID", int)],
                                   OperationType.WRITE,
                                   warehouse_t,
                                   lambda ctx: warehouse_t.column("W_ID") == ctx["W_ID"],
                                   ["W_YTD"])

    payGetWhseSQL = Operation("payGetWhseSQL",
                               [InputParameter("W_ID", int), OutputParameter("W_NAME", int)],
                               OperationType.READ,
                               warehouse_t,
                               lambda ctx: warehouse_t.column("W_ID") == ctx["W_ID"],
                               ["W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP", "W_NAME"])

    payUpdateDistSQL = Operation("payUpdateDistSQL",
                               [InputParameter("W_ID", int), InputParameter("D_ID", int)],
                               OperationType.WRITE,
                               district_t,
                               lambda ctx: z3.And(district_t.column("D_W_ID") == ctx["W_ID"],
                                                  district_t.column("D_ID") == ctx["D_ID"]),
                               ["D_YTD"])

    payGetDistSQL = Operation("payGetDistSQL",
                               [InputParameter("W_ID", int), InputParameter("D_ID", int),
                                OutputParameter("D_NAME", int)],
                               OperationType.READ,
                               district_t,
                              lambda ctx: z3.And(district_t.column("D_W_ID") == ctx["W_ID"],
                                                 district_t.column("D_ID") == ctx["D_ID"]),
                               ["D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP", "D_NAME"])

    payGetCustSQL = Operation("payGetCustSQL",
                                [InputParameter("W_ID", int), InputParameter("D_ID", int),
                                 InputParameter("C_ID", int), OutputParameter("C_BALANCE", int),
                                 OutputParameter("C_YTD_PAYMENT", int), OutputParameter("C_PAYMENT_CNT", int),
                                 OutputParameter("C_ID", int)],
                                OperationType.READ,
                                customer_t,
                                lambda ctx: z3.And(customer_t.column("C_W_ID") == ctx["W_ID"],
                                                   customer_t.column("C_D_ID") == ctx["D_ID"],
                                                   customer_t.column("C_ID") == ctx["C_ID"]),
                                ["C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2",
                                   "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_CREDIT", "C_CREDIT_LIM",
                                   "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_SINCE"])

    payUpdateCustBalSQL = Operation("payUpdateCustBalSQL",
                                   [InputParameter("W_ID", int), InputParameter("D_ID", int),
                                    InputParameter("C_ID", int)],
                                   OperationType.WRITE,
                                   customer_t,
                                    lambda ctx: z3.And(customer_t.column("C_W_ID") == ctx["W_ID"],
                                                       customer_t.column("C_ID") == ctx["C_ID"],
                                                       customer_t.column("C_D_ID") == ctx["D_ID"]),
                                   ["C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT"])

    payInsertHistSQL = Operation("payInsertHistSQL",
                                    [InputParameter("C_D_ID", int), InputParameter("C_W_ID", int),
                                     InputParameter("C_ID", int),
                                     InputParameter("D_ID", int), InputParameter("W_ID", int),
                                     InputParameter("H_AMOUNT", int)],
                                    OperationType.WRITE,
                                    history_t,
                                    [],
                                    ["H_C_D_ID", "H_C_W_ID", "H_C_ID", "H_D_ID", "H_W_ID", "H_DATE", "H_AMOUNT", "H_DATA"])

    payUpdateWhseSQL_lt = LocalTransaction([payUpdateWhseSQL], payUpdateWhseSQL.params)
    payGetWhseSQL_lt = LocalTransaction([payGetWhseSQL], payGetWhseSQL.params)
    payUpdateDistSQL_lt = LocalTransaction([payUpdateDistSQL], payUpdateDistSQL.params)
    payGetDistSQL_lt = LocalTransaction([payGetDistSQL], payGetDistSQL.params)
    payGetCustSQL_lt = LocalTransaction([payGetCustSQL], payGetCustSQL.params)
    payUpdateCustBalSQL_lt = LocalTransaction([payUpdateCustBalSQL], payUpdateCustBalSQL.params)
    payInsertHistSQL = LocalTransaction([payInsertHistSQL], payInsertHistSQL.params)

    return BusinessTransaction("payment_bt",
                               [payUpdateWhseSQL_lt, payGetWhseSQL_lt, payUpdateDistSQL_lt, payGetDistSQL_lt,
                                payGetCustSQL_lt, payUpdateCustBalSQL_lt, payInsertHistSQL],
                               [InputParameter("W_ID", int), InputParameter("D_ID", int), InputParameter("C_ID", int),
                                InputParameter("C_W_ID", int), InputParameter("H_AMOUNT", int), InputParameter("C_D_ID", int)])
class Test(TestCase):

    def test(self):
        print(time.process_time())

        delivery_bt = get_delivery_bt()

        stock_level_bt = get_stock_level()

        order_status_bt = get_order_status()

        new_order_bt = get_new_order()

        payment_bt = get_payment()

        ms = Microservice("ms", [delivery_bt, stock_level_bt, order_status_bt, new_order_bt, payment_bt])

        system = System([ms], non_conflict)

        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, assertions = get_cycles_and_dag_paths(graph)
        print("nodes: " + str(len(graph.nodes())))
        for c in cycles:
            print(c)
        # print(len(topological_paths_for_cycles))
        # print(topological_paths_for_cycles)
        verify_cycles(topological_paths_for_cycles, system)
