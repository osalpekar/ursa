import ray

@ray.remote
class Graph(object):
    """
    This object contains reference and connection information for a graph.

    Fields:
    oid_dictionary -- the dictionary mapping the unique identifier of a node to
                      the ObjectID of the Node object stored in the Ray object
                      store.
    adjacency_list -- the dictionary mapping the unique identifier of a node to
                      an ObjectID of the set of connections within the graph.
                      The set of connections is built asynchronously.
    inter_graph_connections -- the dictionary mapping the unique identifier of
                               a node to the ObjectID of the set of connections
                               between graphs. The set of connections between
                               graphs is a dictionary {graph_id -> other_graph_key}
                               and is built asynchronously.
    """
    
    def __init__(self):
        """
        The constructor for the Graph object. Initializes all graph data.
        """
        self.oid_dictionary = {}
        self.adjacency_list = {}
        self.inter_graph_connections = {}
        
    def insert_node_into_graph(self, key, oid, adjacency_list, connections_to_other_graphs):
        """
        Inserts the data for a node into the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        oid -- the Ray ObjectID for the Node object referenced by key.
        adjacency_list -- the list of connections within this graph.
        """
        if type(connections_to_other_graphs) is not dict:
            raise ValueError("Connections to other graphs require destination graph to be specified.")

        self.oid_dictionary[key] = ray.put(oid)
        if not key in self.adjacency_list:
            self.adjacency_list[key] = ray.put(set(adjacency_list))
        else:
            self.adjacency_list[key] = _add_to_adj_list.remote(self.adjacency_list[key], set(adjacency_list))

        if not key in self.inter_graph_connections:
            self.inter_graph_connections[key] = {}
        
        for other_graph_id in connections_to_other_graphs:
            if not other_graph_id in self.inter_graph_connections[key]:
                try:
                    self.inter_graph_connections[key][other_graph_id] = ray.put(set([connections_to_other_graphs[other_graph_id]]))
                except TypeError:
                    self.inter_graph_connections[key][other_graph_id] = ray.put(set(connections_to_other_graphs[other_graph_id]))
            else:
                self.inter_graph_connections[key][other_graph_id] = _add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], connections_to_other_graphs[other_graph_id])
    
    def add_new_adjacent_node(self, key, adjacent_node_key):
        """
        Adds a new connection to the adjacency_list for the key provided.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        adjacent_node_key -- the unique identifier of the new connection to be
                             added.
        """
        if not key in self.adjacency_list:
            self.adjacency_list[key] = set([adjacent_node_key])
        else:
            self.adjacency_list[key] = _add_to_adj_list.remote(self.adjacency_list[key], adjacent_node_key)
        
    def create_inter_graph_connection(self, key):
        """
        Initializes the inter_graph_connections for a given identifier.
        """
        self.inter_graph_connections[key] = {}
        
    def add_inter_graph_connection(self, key, other_graph_id, new_connection):
        """
        Adds a single new connection to another graph. Because all connections
        are bi-directed, connections are created from the other graph to this
        one also.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the name of the graph for the new connection.
        new_connection -- the identifier of the node for the new connection.
        """
        if not key in self.inter_graph_connections:
            self.create_inter_graph_connection(key)

        if not other_graph_id in self.inter_graph_connections[key]:
            self.inter_graph_connections[key][other_graph_id] = ray.put(set([new_connection]))
        else:
            self.inter_graph_connections[key][other_graph_id] = _add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], set([new_connection]))

    def add_multiple_inter_graph_connections(self, key, other_graph_id, new_connection_list):
        """
        Adds a multiple new connections to another graph. Because all
        connections are bi-directed, connections are created from the other
        graph to this one also.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.
        other_graph_id -- the name of the graph for the new connection.
        new_connection_list -- the list of identifiers of the node for the new
                               connection.
        """
        if not key in self.inter_graph_connections:
            self.create_inter_graph_connection(key)

        if not other_graph_id in self.inter_graph_connections[key]:
            self.inter_graph_connections[key][other_graph_id] = ray.put(set(new_connection_list))
        else:
            self.inter_graph_connections[key][other_graph_id] = _add_to_adj_list.remote(self.inter_graph_connections[key][other_graph_id], set(new_connection_list))

    def node_exists(self, key):
        """
        Determines if a node exists in the graph.

        Keyword arguments:
        key -- the unique identifier of the node in the graph.

        Returns:
        If node exists in graph, returns true, otherwise false.
        """
        return key in self.oid_dictionary

    def get_oid_dictionary(self, key = ""):
        """
        Gets the ObjectID of the Node requested. If none requested, returns the
        full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            return self.oid_dictionary
        else:
            return self.oid_dictionary[key]
    
    def get_adjacency_list(self, key = ""):
        """
        Gets the connections within this graph of the Node requested. If none
        requested, returns the full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            return self.adjacency_list
        else:
            return self.adjacency_list[key]
        
    def get_inter_graph_connections(self, key = "", other_graph_id = ""):
        """
        Gets the connections to other graphs of the Node requested. If none
        requested, returns the full dictionary.

        Keyword arguments:
        key -- the unique identifier of the node in the graph (default = "").
        """
        if key == "":
            return self.inter_graph_connections
        elif other_graph_id == "":
            return self.inter_graph_connections[key]
        else:
            raise ValueError("Not yet implemented: Getting inter-graph connections for a specific graph")
            return self.inter_graph_connections[key][other_graph_id]

@ray.remote
def _add_to_adj_list(adj_list, other_key):
    """
    Adds one or multiple keys to the list provided. This can add to both the
    adjacency list and the connections between graphs.

    The need for this stems from trying to make updates to the graph as
    asynchronous as possible.

    Keyword arguments:
    adj_list -- the list of connections to append to.
    other_key -- a set of connections or a single value to add to adj_list.

    Returns:
    The updated list containing the newly added value(s).
    """
    try:
        adj_list.update(set([other_key]))
    except TypeError:
        adj_list.update(set(other_key))
    
    return adj_list