import time
from unittest import TestCase

from core.system_processor import verify_cycles, get_cycles_and_dag_paths, generate_system_graph_image, \
    set_self_loop_budget
from domain.domain_model import *
from parser.system_parser import create_graph_from_system


class Test(TestCase):

    def test(self):
        print(time.process_time())


        contestant_t = Table("contestant_t", [Column("id", int), Column("number", int)])

        votes_t = Table("contestant_t", [Column("vote_id", int),
                                         Column("phone_number", int),
                                         Column("state", str),
                                         Column("contestant_number", int),
                                         Column("created", int)])

        locations_t = Table("locations_t", [Column("id", int), Column("area_code", str)])

        checkContestant = Operation("checkContestant",
                                    [InputParameter("contestant_number", int)],
                                    OperationType.READ,
                                    contestant_t,
                                    lambda ctx: contestant_t.column("number") == ctx["contestant_number"],
                                    ["number"])

        checkVoter = Operation("checkVoter",
                                    [InputParameter("phone_number", int)],
                                    OperationType.READ,
                                    votes_t,
                                    lambda ctx: votes_t.column("phone_number") == ctx["phone_number"],
                                    ["phone_number"])

        checkState = Operation("checkState",
                               [InputParameter("area_code", str)],
                               OperationType.READ,
                               locations_t,
                               lambda ctx: locations_t.column("area_code") == ctx["area_code"],
                               ["area_code"])

        insertVote = Operation("insertVote",
                               [InputParameter("vote_id", int),
                                InputParameter("phone_number", int),
                                InputParameter("state", str),
                                InputParameter("contestant_number", int),
                                InputParameter("created", int)],
                               OperationType.WRITE,
                               votes_t,
                               lambda ctx: votes_t.column("vote_id") == ctx["vote_id"],
                               ["vote_id", "phone_number", "state", "contestant_number", "created"])

        #vote = LocalTransaction([checkContestant, checkVoter, checkState, insertVote], checkContestant.params + checkVoter.params + checkState.params + insertVote.params)
        #vote_bt = BusinessTransaction("vote_bt", [vote], vote.params)

        checkContestant_lt = LocalTransaction([checkContestant], checkContestant.params)
        checkVoter_lt = LocalTransaction([checkVoter], checkVoter.params)
        checkState_lt = LocalTransaction([checkState], checkState.params)
        insertVote_lt = LocalTransaction([insertVote], insertVote.params)

        vote_bt = BusinessTransaction("vote_bt",
                                      [checkContestant_lt, checkVoter_lt, checkState_lt, insertVote_lt],
                                      checkContestant_lt.params + checkVoter_lt.params + checkState_lt.params + insertVote_lt.params)

        ms = Microservice("voter", [vote_bt])


        system = System([ms])
        graph = create_graph_from_system(system)
        set_self_loop_budget(2)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        print(cycles)
        print("nodes: " + str(len(graph.nodes())))
        verify_cycles(topological_paths_for_cycles, system)
        #generate_system_graph_image(graph)