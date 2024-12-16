import time
from unittest import TestCase

from core.system_processor import get_cycles_and_dag_paths, verify_cycles, generate_system_graph_image, \
    set_self_loop_budget
from domain.domain_model import *
from parser.system_parser import create_graph_from_system

non_conflict = []

followers_t = Table("followers_t", [Column("f1", int), Column("f2", int)])
follows_t = Table("follows_t", [Column("f1", int), Column("f2", int)])
user_t = Table("user_t", [Column("uid", int), Column("name", str)])
tweet_t = Table("tweet_t", [Column("uid", int)])
added_tweet_t = Table("added_tweet_t", [Column("uid", int), Column("text", int), Column("createdate", int)])

getFollowers = Operation("getFollowers",
                       [InputParameter("uid", int), OutputParameter("f2", int)],
                       OperationType.READ,
                       followers_t,
                       lambda ctx: followers_t.column("f1") == ctx["uid"],
                       ["f1", "f2"])

getFollowerNames = Operation("getFollowerNames",
                       [InputParameter("f2", int),
                        OutputParameter("uid", int),
                        OutputParameter("name", int)],
                       OperationType.READ,
                       user_t,
                       lambda ctx: user_t.column("uid") == ctx["f2"],
                       ["uid", "name"])

getTweet = Operation("getTweet",
                       [InputParameter("tid", int)],
                       OperationType.READ,
                       tweet_t,
                       lambda ctx: tweet_t.column("id") == ctx["tid"],
                       ["id"])

getFollowing = Operation("getFollowing",
                       [InputParameter("uid", int), OutputParameter("f2", int)],
                       OperationType.READ,
                       follows_t,
                       lambda ctx: follows_t.column("f1") == ctx["uid"],
                       ["f1", "f2"])

getTweets = Operation("getTweets",
                       [InputParameter("f2", int)],
                       OperationType.READ,
                       tweet_t,
                       lambda ctx: tweet_t.column("uid") == ctx["f2"],
                       ["uid"])

getTweets_2 = Operation("getTweets",
                       [InputParameter("uid", int)],
                       OperationType.READ,
                       tweet_t,
                       lambda ctx: tweet_t.column("uid") == ctx["uid"],
                       ["uid"])

insertTweet = Operation("insertTweet",
                       [InputParameter("tid", int)],
                       OperationType.WRITE,
                       added_tweet_t,
                       [],
                       ["uid", "text", "createdate"])

# list not supported, only one - to find an anomaly we only need one

def get_followers():

    getFollowers_lt = LocalTransaction([getFollowers], getFollowers.params)
    getFollowerNames_lt = LocalTransaction([getFollowerNames], getFollowerNames.params)

    return BusinessTransaction("get_followers_bt",
                               [getFollowers_lt, getFollowerNames_lt],
                               [InputParameter("uid", int)])


def get_tweet():

    getTweet_lt = LocalTransaction([getTweet], getTweet.params)

    return BusinessTransaction("get_tweet_bt",
                               [getTweet_lt],
                               [InputParameter("uid", int)])

def get_GetTweetsFromFollowing():

    getFollowing_lt = LocalTransaction([getFollowing], getFollowing.params)
    getTweets_lt = LocalTransaction([getTweets], getTweets.params)

    return BusinessTransaction("GetTweetsFromFollowing",
                               [getFollowing_lt, getTweets_lt],
                               [InputParameter("uid", int)])

def get_GetUserTweets():

    getTweets_lt = LocalTransaction([getTweets_2], getTweets_2.params)

    return BusinessTransaction("GetUserTweets",
                               [getTweets_lt],
                               [InputParameter("uid", int)])

def get_InsertTweet():

    insertTweet_lt = LocalTransaction([insertTweet], insertTweet.params)

    return BusinessTransaction("insertTweet",
                               [insertTweet_lt],
                               [InputParameter("tid", int)])


class Test(TestCase):

    def test(self):
        print(time.process_time())

        followers_bt = get_followers()
        get_tweet_bt = get_tweet()
        get_tweets_from_following_bt = get_GetTweetsFromFollowing()
        get_user_tweets_bt = get_GetUserTweets()
        insertTweet_bt = get_InsertTweet()

        ms = Microservice("ms", [followers_bt, get_tweet_bt, get_tweets_from_following_bt, get_user_tweets_bt, insertTweet_bt])

        system = System([ms], non_conflict)

        set_self_loop_budget(2)
        graph = create_graph_from_system(system)
        cycles, paths, topological_paths_for_cycles, _ = get_cycles_and_dag_paths(graph)
        print(cycles)
        print("nodes: " + str(len(graph.nodes())))
        # print(len(topological_paths_for_cycles))
        # print(topological_paths_for_cycles)
        verify_cycles(topological_paths_for_cycles, system)
        # generate_system_graph_image(graph)