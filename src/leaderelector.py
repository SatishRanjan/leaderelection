import kazoo.client as kzcl
import kazoo.retry as kzretry
import kazoo.exceptions as ke
import constants as const
import os

'''
    Leader election algorithm:
    1. Create an emphemeral sequential node
    2. Try to elect leader, by choosing a node with the lowest sequence number
    3. If not the leader, then set watch on the node with next lower index

    e.g. Out of these 3 candidate nodes /leaderelection/candidate_0000000001, /leaderelection/candidate_0000000002, /leaderelection/candidate_0000000003
        /leaderelection/candidate_0000000001 = leader
        /leaderelection/candidate_0000000002 watches for deletion of /leaderelection/candidate_0000000001
        /leaderelection/candidate_0000000003 watches for deletion of /leaderelection/candidate_0000000002
'''
class LeaderElector():
    def __init__(self, leader_election_znode_root_path, candidate_node_name_prefix):       
        self.configure_kazoo_client()
        #The leader will be creating ehemeral sequential znode under this root path       
        self.leader_election_znode_root_path = leader_election_znode_root_path

        #The ehemeral node name prefix for the leader candidate's created ehemeral sequential znode
        self.candidate_node_name_prefix = candidate_node_name_prefix

        #The callback for the client interested in monitoring change in the leader
        #This is the call function (aka. method pointer/delegate) passed to the leader elector which will be invoked 
        #in the case of leader node change, this will be passed as part of the try_elect_leader function call on this object for the object reusability purposes
        self.leader_election_callback = None

        #The ehemeral sequential node created for the leader election
        self.ephemeral_node_path = "" 
    
    def configure_kazoo_client(self):
        retry_policy = kzretry.KazooRetry(max_tries=1000, delay=0.5, backoff=2)       
        self.kzclient = kzcl.KazooClient(const.ZOO_KEEPER_SERVERS_IP_PORT, connection_retry=retry_policy)
        self.kzclient.start()  
    
    def try_elect_leader(self, leader_election_callback, candidate_node_value):
        try:
            #The node value to be stored in the leader candidate znode
            self.candidate_node_value = candidate_node_value
            self.create_ephemeral_node_if_not_exists(candidate_node_value)
            self.leader_election_callback = leader_election_callback

            print("The created cadidate leader node path is: {}".format(self.ephemeral_node_path))
            #Get the list of candidate nodes under /leaderelection
            #e.g [candidate_0000000002, candidate_0000000001, candidate_0000000003
            candidate_nodes = self.kzclient.get_children(self.leader_election_znode_root_path)        
            if candidate_nodes is None or len(candidate_nodes) < 1:
                print("Nodes count should be  >= 1, at least there should be one active leader candidate, exiting the application!")           
                os._exit(0)

            #Sort the candidate nodes for the candidates with minimum sequential node appear on the top
            #e.g [candidate_0000000001, candidate_0000000002, candidate_0000000003
            candidate_nodes.sort()
            print("Sorted candidate nodes for the leader election: {}".format(candidate_nodes))

            # If the created ephemeral node path by this leaderelctor object is the node with smallest sequence number
            # Then this candidate node is the leader and doesn't watch any other candidate node
            # e.g if this object created /leaderelection/candidate_0000000001 ehemeral sequence node, 
            # then the server/process/thread holding this object will be the leader
            # Else, if this object created /leaderelection/candidate_0000000002 node then it'll follow the current leader node /leaderelection/candidate_0000000001    
            node_to_watch = ""
            if self.ephemeral_node_path.endswith(candidate_nodes[0]):
                print("This node is the leader: {}".format(self.ephemeral_node_path))
                node_to_watch = self.ephemeral_node_path
                
            else:                                                                  
                # this_candidate_node_name = candidate_0000000002
                # retrived by taking last 10 chracter "0000000002" from ehemeral node created by this object /leaderelection/candidate_0000000002 
                # and prefixed by "candidate_" which reults in "candidate_0000000002"           
                this_candidate_node_name = "candidate_" + self.ephemeral_node_path[len(self.ephemeral_node_path)-10:]

                # Get the index location at which this candidate node is located in the list of the candidate nodes
                this_candidate_node_index = candidate_nodes.index(this_candidate_node_name)

                # watch for the candidate node located at the next lower index e.g. candidate "candidate_0000000003" will watch "candidate_0000000001"
                # candidate "candidate_0000000002" will watch "candidate_0000000001"
                # and broker "candidate_0000000001" will not watch any other candidate node, as it's the leader
                node_to_watch_index = this_candidate_node_index - 1

                # Construct the node path that will be watched by this leaderelector candidate
                node_being_followed = self.leader_election_znode_root_path +'/'+ candidate_nodes[node_to_watch_index]
                node_to_watch = node_being_followed

            # Set a watch on the candidate node by providing a watch function "watch_for_delete", which will be called when the canidate node changes
            self.kzclient.get(node_to_watch, self.watch_for_delete)

            # Notify the client interested in change in the leader election by calling the provided client callback function and by passing the 
            # current leader node path as the value if it exists
            if self.leader_election_callback != None:
                    self.leader_election_callback(self.leader_election_znode_root_path + "/" + candidate_nodes[0], self.get_node_value(self.leader_election_znode_root_path + "/" + candidate_nodes[0]))
        except:
            self.kzclient.stop()

    # The callback function passed to the kazoo client to get notified when the node being followed in deleted/value changed
    def watch_for_delete(self, event):
        print("There's a change event in the leader node event_type:{}".format(event.type))

        # If the node being followed had been deleted, redo the leader election again
        if event.type == "DELETED":
            self.try_elect_leader(self.leader_election_callback, self.candidate_node_value)

    def create_ephemeral_node_if_not_exists(self, candidate_node_value):
        if self.ephemeral_node_path == "":
            self.ephemeral_node_path = self.kzclient.create(
                self.leader_election_znode_root_path + self.candidate_node_name_prefix, 
                candidate_node_value.encode('utf-8'), ephemeral=True, sequence=True, makepath=True)

    def get_node_value(self, node_path):
        if self.kzclient.exists(node_path):       
            data, _ = self.kzclient.get(node_path)
            if data is None:
                return ""
            return data.decode("utf-8")
        else:
            return ""

   