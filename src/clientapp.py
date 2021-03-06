import leaderelector as le
import socket
import constants as const
import time


def elect_leader():
    current_host_name = socket.gethostbyname(socket.gethostname())
    leader_elector = le.LeaderEelector(const.LEADER_ELECTION_ROOT_ZNODE, const.CANDIDATE_NODE_PREFIX)
    leader_elector.try_elect_leader(leader_election_callback, current_host_name)

def leader_election_callback(leader_node, leader_node_value):
    print("The current leader node is: {}, node_value:{}".format(leader_node, leader_node_value))

if __name__ == '__main__':
    print("Starting the leader election")
    elect_leader()

    # Wait for the leaderelection and the change to happen
    while True:
        time.sleep(10)
