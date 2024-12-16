from unittest import TestCase

from core.system_processor import get_cycles_and_dag_paths, verify_cycles
from domain.domain_model import *
from parser.smt_utils import check_edge
from parser.system_parser import create_graph_from_system


class Test(TestCase):
    def test_get_edge_condition_un_sat_admin(self):
        user_t = Table("user", [Column("id", int), Column("age", int), Column("admin", bool)])

        read_parameters = [InputParameter("x", int), OutputParameter("admin", str)]
        read_wheres_test = lambda ctx: z3.And(user_t.column("id") == ctx["x"], user_t.column("admin"))
        read_admin = Operation("readAdmin", read_parameters, OperationType.READ, user_t, read_wheres_test)

        write_parameters = [InputParameter("admin_id", str)]
        write_wheres = lambda ctx: z3.Not(user_t.column("admin"))
        write_not_admin = Operation("readEmployee", write_parameters, OperationType.WRITE, user_t, write_wheres)

        local_transaction_1 = LocalTransaction([read_admin], [InputParameter("x", int), OutputParameter("admin", str)])
        local_transaction_2 = LocalTransaction([write_not_admin])

        bt_1 = BusinessTransaction("BT1", [local_transaction_1], read_parameters)
        bt_2 = BusinessTransaction("BT2", [local_transaction_2], write_parameters)

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])

        edge_is_present, condition = check_edge(local_transaction_1, local_transaction_2)
        self.assertFalse(edge_is_present)

    def test_get_edge_condition_sat_doesnt_have_admin_conflict(self):
        user_t = Table("user", [Column("id", str), Column("age", int), Column("admin", bool)])

        read_parameters = [InputParameter("x", str), OutputParameter("admin_id", str)]
        read_wheres_test = lambda ctx: user_t.column("id") == ctx["x"]
        read_admin = Operation("readAdmin", read_parameters, OperationType.READ, user_t, read_wheres_test)

        write_parameters = [InputParameter("admin_id", str)]
        write_wheres = lambda ctx: z3.Not(user_t.column("admin"))
        write_not_admin = Operation("readEmployee", write_parameters, OperationType.WRITE, user_t, write_wheres)

        local_transaction_1 = LocalTransaction([read_admin], read_parameters)
        local_transaction_2 = LocalTransaction([write_not_admin], write_parameters)

        bt_1 = BusinessTransaction("BT1", [local_transaction_1], [InputParameter("x", str), OutputParameter("admin_id", str)])
        bt_2 = BusinessTransaction("BT2", [local_transaction_2], write_parameters)

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])

        edge_is_present, condition = check_edge(local_transaction_1, local_transaction_2)
        self.assertTrue(edge_is_present)

    def test_unsat_admin_cycle(self):
        user_t = Table("user", [Column("id", int), Column("age", int), Column("admin", bool)])

        read_parameters = [InputParameter("x", int), OutputParameter("admin", str)]
        read_wheres_test = lambda ctx: z3.And(user_t.column("id") == ctx["x"], user_t.column("admin"))
        read_admin = Operation("readAdmin", read_parameters, OperationType.READ, user_t, read_wheres_test)

        write_parameters = [InputParameter("admin_id", int)]
        write_wheres = lambda ctx: z3.And(user_t.column("id") == ctx["admin_id"], z3.Not(user_t.column("admin")))
        write_not_admin = Operation("readEmployee", write_parameters, OperationType.WRITE, user_t, write_wheres)

        local_transaction_1 = LocalTransaction([read_admin], read_parameters)
        local_transaction_2 = LocalTransaction([write_not_admin], write_parameters)

        bt_1 = BusinessTransaction("BT1", [local_transaction_1],
                                   read_parameters)
        bt_2 = BusinessTransaction("BT2", [local_transaction_2], write_parameters)

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])
        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        verify_cycles(topological_paths_for_cycles, system)
        self.assertTrue(len(cycles) == 0)

    def test_sat_cycle(self):
        user_t = Table("user", [Column("id", int), Column("age", int), Column("admin", bool)])

        read_parameters = [InputParameter("x", int), OutputParameter("admin_id", int)]
        read_wheres = lambda ctx: user_t.column("id") == ctx["x"]
        read_admin = Operation("readAdmin", read_parameters, OperationType.READ, user_t, read_wheres)

        write_admin_parameters = [InputParameter("admin_id", int)]
        write_admin_wheres = lambda ctx: user_t.column("id") == ctx["x"]
        write_admin = Operation("readAdmin", write_admin_parameters, OperationType.WRITE, user_t, write_admin_wheres)

        write_parameters = [InputParameter("admin_id", int)]
        write_wheres = lambda ctx: z3.Not(user_t.column("admin"))
        write_not_admin = Operation("readEmployee", write_parameters, OperationType.WRITE, user_t, write_wheres)

        local_transaction_1 = LocalTransaction([read_admin], read_parameters)
        local_transaction_3 = LocalTransaction([write_admin], write_admin_parameters)
        local_transaction_2 = LocalTransaction([write_not_admin], write_parameters)

        bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_3],
                                   [InputParameter("admin_id", int), InputParameter("x", int), OutputParameter("admin_id", int)])
        bt_2 = BusinessTransaction("BT2", [local_transaction_2], write_parameters)

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])

        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        verify_cycles(topological_paths_for_cycles, system)
        self.assertTrue(len(cycles) > 0)

    def test_get_edge_condition_column_intersection_not_empty(self):
        user_t = Table("user", [Column("id", str), Column("age", int), Column("admin", bool)])

        read_parameters = [InputParameter("x", int), OutputParameter("y", str)]
        read = Operation("read", read_parameters, OperationType.READ, user_t, [], ["age"])

        write_parameters = [InputParameter("a", str), OutputParameter("b", str)]
        write = Operation("write", write_parameters, OperationType.WRITE, user_t, [], ["admin"])

        local_transaction_1 = LocalTransaction([read], [InputParameter("x", int), OutputParameter("y", str)])
        local_transaction_2 = LocalTransaction([write], [InputParameter("a", int), OutputParameter("b", str)])

        bt_1 = BusinessTransaction("BT1", [local_transaction_1], [InputParameter("x", int), OutputParameter("y", str)])
        bt_2 = BusinessTransaction("BT2", [local_transaction_2], [InputParameter("a", int), OutputParameter("b", str)])

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])

        edge_is_present, condition = check_edge(local_transaction_1, local_transaction_2)
        self.assertFalse(edge_is_present)

    def test_get_edge_condition_column_intersection_empty(self):
        user_t = Table("user", [Column("id", str), Column("age", int), Column("admin", bool)])

        read_parameters = [InputParameter("x", int), OutputParameter("y", str)]
        read = Operation("read", read_parameters, OperationType.READ, user_t, [], ["age", "id"])

        write_parameters = [InputParameter("a", str), OutputParameter("b", str)]
        write = Operation("write", write_parameters, OperationType.WRITE, user_t, [], ["age"])

        local_transaction_1 = LocalTransaction([read], [InputParameter("x", int), OutputParameter("y", str)])
        local_transaction_2 = LocalTransaction([write], [InputParameter("a", int), OutputParameter("b", str)])

        bt_1 = BusinessTransaction("BT1", [local_transaction_1], [InputParameter("x", int), OutputParameter("y", str)])
        bt_2 = BusinessTransaction("BT2", [local_transaction_2], [InputParameter("a", int), OutputParameter("b", str)])

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])

        edge_is_present, condition = check_edge(local_transaction_1, local_transaction_2)
        self.assertTrue(edge_is_present)

    def test_full_cycle_fail_when_edges_are_individually_present(self):
        user_t = Table("user", [Column("id", int), Column("age", int)])

        read_parameters = [InputParameter("age", int), OutputParameter("user_id", int)]
        read_wheres_1 = lambda ctx: user_t.column("age") == ctx["age"]
        read_age_1 = Operation("readAge_1", read_parameters, OperationType.READ, user_t, read_wheres_1, ["age"])

        read_parameters_2 = [InputParameter("age", int), OutputParameter("user_id_2", int)]
        read_wheres_2 = lambda ctx: user_t.column("age") == ctx["age"] + 1
        read_age_2 = Operation("readAge_2", read_parameters_2, OperationType.READ, user_t, read_wheres_2, ["age"])

        write_parameters = [InputParameter("age_update", int), OutputParameter("b", int)]
        write_wheres = lambda ctx: user_t.column("age") == ctx["age_update"]
        write_age = Operation("write_age", write_parameters, OperationType.WRITE, user_t, write_wheres, ["age"])

        local_transaction_1 = LocalTransaction([read_age_1],
                                               read_parameters)

        local_transaction_2 = LocalTransaction([read_age_2],
                                               read_parameters_2)

        local_transaction_3 = LocalTransaction([write_age],
                                               write_parameters)

        bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2], read_parameters)

        bt_2 = BusinessTransaction("BT2", [local_transaction_3], write_parameters)

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])

        graph = create_graph_from_system(system)

        edge, assertions = check_edge(local_transaction_1, local_transaction_3)
        self.assertTrue(edge)

        edge, assertions = check_edge(local_transaction_2, local_transaction_3)
        self.assertTrue(edge)

        edge, assertions = check_edge(local_transaction_1, local_transaction_2)
        self.assertFalse(edge)

        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        verify_cycles(topological_paths_for_cycles, system)
        self.assertEqual(0, len(cycles))

    def test_number_lt_and_gt_conditions(self):
        user_t = Table("user", [Column("id", int), Column("age", int)])

        read_wheres_1 = lambda ctx: z3.And(user_t.column("age") > 10, user_t.column("age") < 20)
        read_age_1 = Operation("readAge_1", [], OperationType.READ, user_t, read_wheres_1, ["age"])

        read_wheres_2 = lambda ctx: z3.And(user_t.column("age") > 15, user_t.column("age") < 25)
        read_age_2 = Operation("readAge_2", [], OperationType.READ, user_t, read_wheres_2, ["age"])

        write_parameters = [InputParameter("age_update", int)]
        write_wheres = lambda ctx: user_t.column("age") == ctx["age_update"]
        write_age = Operation("write_age", write_parameters, OperationType.WRITE, user_t, write_wheres, ["age"])

        local_transaction_1 = LocalTransaction([read_age_1])

        local_transaction_2 = LocalTransaction([read_age_2])

        local_transaction_3 = LocalTransaction([write_age], write_parameters)

        bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2])

        bt_2 = BusinessTransaction("BT2", [local_transaction_3], write_parameters)

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])

        graph = create_graph_from_system(system)

        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        verify_cycles(topological_paths_for_cycles, system)

        edge, assertions = check_edge(local_transaction_1, local_transaction_3)
        self.assertTrue(edge)

        edge, assertions = check_edge(local_transaction_2, local_transaction_3)
        self.assertTrue(edge)

        edge, assertions = check_edge(local_transaction_1, local_transaction_2)
        self.assertFalse(edge)

    def test_used_vars_in_op_filter(self):
        user_t = Table("user", [Column("id", int), Column("age", int)])

        read_parameters = [InputParameter("age", int), OutputParameter("user_id", int)]
        read_wheres_1 = lambda ctx: user_t.column("age") == ctx["age"]
        read_age_1 = Operation("readAge_1", read_parameters, OperationType.READ, user_t, read_wheres_1, ["age"])

        # lambda does not use user_id, so there is no dependency between reads
        read_parameters_2 = [InputParameter("user_id", int), OutputParameter("age", int)]
        read_wheres_2 = lambda ctx: user_t.column("id") == 1
        read_user = Operation("readAge_2", read_parameters_2, OperationType.READ, user_t, read_wheres_2, ["age"])

        lt_1 = LocalTransaction([read_age_1], read_parameters)

        lt_2 = LocalTransaction([read_user], read_parameters_2)

        bt_1 = BusinessTransaction("BT1", [lt_1, lt_2], read_parameters)

        edge, assertions = check_edge(lt_1, lt_2)
        self.assertFalse(edge)

    def test_admin_and_age(self):
        user_t = Table("user", [Column("id", int), Column("age", int), Column("admin", bool)])

        read_parameters = [InputParameter("x", int), OutputParameter("admin_ids", int)]
        read_wheres_test = lambda ctx: z3.And(user_t.column("age") == ctx["x"], user_t.column("admin"))
        read_admin_by_age = Operation("readAdmin", read_parameters, OperationType.READ, user_t, read_wheres_test, ["id", "admin"])

        write_parameters_2 = [InputParameter("admin_ids", int)]
        write_wheres_2 = lambda ctx: user_t.column("id") == ctx["admin_ids"]
        write_with_age = Operation("writeAdmin", write_parameters_2, OperationType.WRITE, user_t, write_wheres_2, ["id"])

        write_parameters = []
        write_wheres = lambda ctx: z3.Not(user_t.column("admin"))
        write_not_admin = Operation("writeNotAdmin", write_parameters, OperationType.WRITE, user_t, write_wheres, ["admin", "id"])

        local_transaction_1 = LocalTransaction([read_admin_by_age], [InputParameter("x", int)])
        local_transaction_2 = LocalTransaction([write_with_age], [InputParameter("admin_ids", int)])
        local_transaction_3 = LocalTransaction([write_not_admin])

        bt_1 = BusinessTransaction("BT1", [local_transaction_1, local_transaction_2], [InputParameter("x", int), InputParameter("admin_ids", int)])
        bt_2 = BusinessTransaction("BT2", [local_transaction_3], write_parameters)

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])

        edge, assertions = check_edge(local_transaction_1, local_transaction_2)
        self.assertTrue(edge)
        edge, assertions = check_edge(local_transaction_1, local_transaction_3)
        self.assertFalse(edge)
        edge, assertions = check_edge(local_transaction_2, local_transaction_3)
        self.assertFalse(edge)
        edge, assertions = check_edge(local_transaction_3, local_transaction_2)
        print(edge, assertions)

    def test_tese_1(self):
        user_t = Table("user", [Column("id", int), Column("age", int)])

        read_parameters = [InputParameter("x", int)]
        read_wheres = lambda ctx: z3.And(user_t.column("id") == ctx["x"])
        read_age = Operation("readAge", read_parameters, OperationType.READ, user_t, read_wheres,
                                      ["id", "age"])

        write_parameters = [InputParameter("x", int)]
        update_wheres = lambda ctx: z3.And(user_t.column("id") == ctx["x"])
        update_age = Operation("updateAge", write_parameters, OperationType.WRITE, user_t, update_wheres,
                             ["id", "age"])

        local_transaction_1 = LocalTransaction([read_age], [InputParameter("x", int)])
        local_transaction_2 = LocalTransaction([update_age], [InputParameter("x", int)])

        bt_1 = BusinessTransaction("BT1", [local_transaction_1], read_parameters)
        bt_2 = BusinessTransaction("BT2", [local_transaction_2], write_parameters)

        ms = Microservice("MS", [bt_1, bt_2])

        system = System([ms])


        edge, assertions = check_edge(local_transaction_1, local_transaction_2)
        print(edge, assertions)