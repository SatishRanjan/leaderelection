LEADER_ELECTION_ROOT_ZNODE = "/leaderelection"
CANDIDATE_NODE_PREFIX = "/candidate_"
'''
    If zookeeper is running in ensemble(Quorum) then this will be the comma separated ip:port for the zookeeper server nodes
    e.g. "127.0.0.1:2181,127.0.0.1:3000"
'''
ZOO_KEEPER_SERVERS_IP_PORT = "127.0.0.1:2181"