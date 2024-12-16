from unittest import TestCase

from core.system_processor import verify_cycles, get_cycles_and_dag_paths, generate_system_graph_image
from domain.domain_model import *
from parser.system_parser import create_graph_from_system


class Test(TestCase):

    def test(self):

        student_t = Table("student_t", [Column("st_id", int),
                                        Column("st_name", str),
                                        Column("st_em_id", int),
                                        Column("st_co_id", int),
                                        Column("st_reg", str)])

        course_t = Table("course_t", [Column("co_id", int),
                                      Column("co_avail", bool),
                                      Column("co_st_cnt", int)])

        email_t = Table("email_t", [Column("em_id", int),
                                    Column("em_addr", str)])

        s1 = Operation("s1",
                       [InputParameter("getSt_id", int), OutputParameter("s1_st_em_id", int), OutputParameter("s1_st_co_id", int)],
                       OperationType.READ,
                       student_t,
                       lambda ctx: student_t.column("st_id") == ctx["getSt_id"],
                       ["st_id", "st_name", "st_em_id", "st_co_id", "st_reg"])

        s2 = Operation("s2",
                       [InputParameter("s1_st_em_id", int), OutputParameter("s2_em_addr", str)],
                       OperationType.READ,
                       email_t,
                       lambda ctx: email_t.column("em_id") == ctx["s1_st_em_id"],
                       ["em_id", "em_addr"])

        s3 = Operation("s3",
                       [InputParameter("s1_st_co_id", int), OutputParameter("s2_co_aval", bool)],
                       OperationType.READ,
                       course_t,
                       lambda ctx: course_t.column("co_id") == ctx["s1_st_co_id"],
                       ["co_id", "co_aval"])

        s4 = Operation("s4",
                       [InputParameter("setSt_id", int), OutputParameter("s4_st_em_id", int)],
                       OperationType.READ,
                       student_t,
                       lambda ctx: student_t.column("st_id") == ctx["setSt_id"],
                       ["st_id", "st_em_id"])

        u1 = Operation("u1",
                       [InputParameter("setSt_id", int)],
                       OperationType.WRITE,
                       student_t,
                       lambda ctx: student_t.column("st_id") == ctx["setSt_id"],
                       ["st_id", "st_name"])

        u2 = Operation("u2",
                       [InputParameter("s4_st_em_id", int)],
                       OperationType.WRITE,
                       email_t,
                       lambda ctx: email_t.column("em_id") == ctx["s4_st_em_id"],
                       ["em_id", "em_addr"])

        u3 = Operation("u3",
                       [InputParameter("regSt_id", int)],
                       OperationType.WRITE,
                       student_t,
                       lambda ctx: student_t.column("st_id") == ctx["regSt_id"],
                       ["st_id", "st_co_id", "st_reg"])

        s5 = Operation("s5",
                       [InputParameter("regSt_course", int), OutputParameter("s5_co_st_cnt", int)],
                       OperationType.READ,
                       course_t,
                       lambda ctx: course_t.column("co_id") == ctx["regSt_course"],
                       ["co_id", "co_st_cnt"])

        u4 = Operation("u4",
                       [InputParameter("regSt_course", int)],
                       OperationType.WRITE,
                       course_t,
                       lambda ctx: course_t.column("co_id") == ctx["regSt_course"],
                       ["co_id", "co_aval", "co_st_cnt"])

        s1_lt = LocalTransaction([s1], s1.params)
        s2_lt = LocalTransaction([s2], s2.params)
        s3_lt = LocalTransaction([s3], s3.params)
        s4_lt = LocalTransaction([s4], s4.params)
        s5_lt = LocalTransaction([s5], s5.params)
        u1_lt = LocalTransaction([u1], u1.params)
        u2_lt = LocalTransaction([u2], u2.params)
        u3_lt = LocalTransaction([u3], u3.params)
        u4_lt = LocalTransaction([u4], u4.params)

        getSt = BusinessTransaction("getSt", [s1_lt, s2_lt, s3_lt], [InputParameter("getSt_id", int)])
        setSt = BusinessTransaction("setSt", [s4_lt, u1_lt, u2_lt], [InputParameter("setSt_id", int)])
        regSt = BusinessTransaction("regSt", [u3_lt, s5_lt, u4_lt], [InputParameter("regSt_id", int), InputParameter("regSt_course", int)])

        ms = Microservice("course_management", [getSt, setSt, regSt])

        system = System([ms])

        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        print(topological_paths_for_cycles)
        verify_cycles(topological_paths_for_cycles, system)
        # get past
        # generate_system_graph_image(graph)
        pass


    def test_safe(self):

        student_t = Table("student_t", [Column("st_id", int),
                                        Column("st_name", str),
                                        Column("st_em_id", int),
                                        Column("st_em_addr", str),
                                        Column("st_co_id", int),
                                        Column("st_co_avail", bool),
                                        Column("st_reg", str)])

        course_t = Table("course_t", [Column("co_id", int),
                                      Column("log_id", str),
                                      Column("co_st_cnt_log", int)])

        s1 = Operation("s1",
                       [InputParameter("getSt_id", int), OutputParameter("s1_st_em_id", int), OutputParameter("s1_st_co_id", int)],
                       OperationType.READ,
                       student_t,
                       lambda ctx: student_t.column("st_id") == ctx["getSt_id"],
                       ["st_id", "st_name", "st_em_id", "st_em_addr", "st_co_id", "st_co_avail", "st_reg"])

        u1 = Operation("u1",
                       [InputParameter("setSt_id", int)],
                       OperationType.WRITE,
                       student_t,
                       lambda ctx: student_t.column("st_id") == ctx["setSt_id"],
                       ["st_id", "st_name", "st_em_addr"])

        u3 = Operation("u3",
                       [InputParameter("regSt_id", int)],
                       OperationType.WRITE,
                       student_t,
                       lambda ctx: student_t.column("st_id") == ctx["regSt_id"],
                       ["st_id", "st_co_id", "st_co_avail", "st_reg"])

        u4 = Operation("u4",
                       [InputParameter("regSt_course", int)],
                       OperationType.WRITE,
                       course_t,
                       [],
                       ["co_id", "log_id", "co_st_cnt_log"])

        s1_lt = LocalTransaction([s1], s1.params)
        u1_lt = LocalTransaction([u1], u1.params)
        u3_lt = LocalTransaction([u3], u3.params)
        u4_lt = LocalTransaction([u4], u4.params)

        getSt = BusinessTransaction("getSt", [s1_lt], [InputParameter("getSt_id", int)])
        setSt = BusinessTransaction("setSt", [u1_lt], [InputParameter("setSt_id", int)])
        regSt = BusinessTransaction("regSt", [u3_lt, u4_lt], [InputParameter("regSt_id", int), InputParameter("regSt_course", int)])

        ms = Microservice("course_management", [getSt, setSt, regSt])

        system = System([ms])

        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        print(topological_paths_for_cycles)
        verify_cycles(topological_paths_for_cycles, system)