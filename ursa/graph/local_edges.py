import ray
import numpy as np
from .utils import _filter_remote

MAX_SUBLIST_SIZE = 10


class LocalEdges(object):
    """
    This object contains a list of edges (both local and in Ray's object store)
    corresponding to a node.

    @field edges: List of OIDs (each OID represents a list of edges in Ray).
    @field buf: List of edges not yet flushed to Ray.
    """
    def __init__(self, *edges):
        """Constructor for LocalEdges class

        @param edges: One or more edges to initialize this object.
        """
        self.edges = \
            [obj for obj in edges if type(obj) is ray.local_scheduler.ObjectID]

        self.buf = \
            np.array([obj for obj in edges
                      if type(obj) is not ray.local_scheduler.ObjectID])
        if len(self.buf) > MAX_SUBLIST_SIZE:
            self.edges.append(ray.put(self.buf))
            self.buf = np.array([])

    def append(self, *values):
        """Append new values to the edge list.

        @param values: Values are edge objects.

        @return: A list of edges including the new appended values.
        """
        temp_edges = self.edges[:]
        temp_edges.extend([val for val in values
                           if type(val) is ray.local_scheduler.ObjectID])
        temp_buf = \
            np.append(self.buf,
                      [val for val in values
                       if type(val) is not ray.local_scheduler.ObjectID])

        if len(temp_buf) > MAX_SUBLIST_SIZE:
            self.prune_buffer()
            self.buf.sort()
            temp_edges.append(ray.put(self.buf))
            temp_buf = np.array([])

        temp_edges.extend(temp_buf)
        return temp_edges

    def filter(self, filterfn):
        """Apply a filter function to the list of edges.

        @param filterfn: The filter function to be applied to the list of
                         edges.
        @return: The filtered list of edges.
        """
        new_edges = [_filter_remote.remote(filterfn, chunk)
                     for chunk in self.edges]
        new_buf = np.array(filter(filterfn, self.buf))
        new_edges.extend(new_buf)

        return new_edges

    def prune_buffer(self):
        temp_set = set()
        temp_buf = np.array([])

        for edge in self.buf:
            if edge in temp_set:
                continue
            temp_set.add(edge)
            temp_buf.append(edge)

        self.buf = temp_buf
        temp_set = None

    def prune_edges(self):
        temp_set = set()
        temp_edge_list = np.array([])

        for list_oid in self.edges:
            for edge in ray.get(list_oid):
                if edge in temp_set:
                    continue
                temp_set.add(edge)
                temp_edge_list.append(edge)

        self.edges = np.array([])
        for i in xrange(0, len(temp_edge_list), MAX_SUBLIST_SIZE):
            x_oid = ray.put(list[i:i + MAX_SUBLIST_SIZE])
            self.edges.append(x_oid)

        temp_set = None
        temp_edges_list = None

    @ray.remote
    def get_global_sampling(self):
        sample_list = np.array([])
        num_sublist_samples = 0.01 * MAX_SUBLIST_SIZE
        for list_oid in self.edges:
            np.append(sample_list,
                      np.random.choice(list_oid, num_sublist_samples))
        sample_list.sort()
        return sample_list

    @ray.remote
    def get_partitions(self):
        indices = [i*(0.01 * MAX_SUBLIST_SIZE * len(self.edges)) for i in range(1,len(self.edges))]
        partitions_list = []
        global_sample_list_oid = self.get_global_sampling
        return [global_sample_list_oid[i] for i in indices]

    @ray.remote
    def partition_sublists(self):

        partion_bounds = self.get_partitions()
        
        # case statement to compare each value in the list to the bound
        # at this point you have list of sublists
        # return that list of sublists

    @ray.remote
    def merge_common_partitions(self):
        for list_oid in self.local_edges:
            partitions_oid = partition_sublists()
