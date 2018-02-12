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
        self.edges = []
        self.buf = np.array([])
        for obj in edges:
            if type(obj) is ray.local_scheduler.ObjectID:
                self.edges.append(obj)
            elif type(obj) is list:
                for item in obj:
                    if type(item) is ray.local_scheduler.ObjectID:
                        self.edges.append(item)
                    else:
                        self.buf = np.append(self.buf, item)
            else:
                self.buf = np.append(self.buf, obj)

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
            temp_buf = self.prune_buffer(temp_buf)
            temp_buf.sort()
            temp_edges.append(ray.put(temp_buf))
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

    def prune_buffer(self, input_buffer):
        temp_set = set()
        temp_buf = np.array([])

        for edge in input_buffer:
            if edge in temp_set:
                continue
            temp_set.add(edge)
            temp_buf.append(edge)

        temp_set = None
        return temp_buf

    @ray.remote
    def get_global_sampling(self):
        sample_list = np.array([])
        num_sublist_samples = 0.01 * MAX_SUBLIST_SIZE
        for list_oid in self.edges:
            sample_list = np.append(sample_list,
                                    np.random.choice(list_oid, 
                                                     num_sublist_samples))
        sample_list.sort()
        return sample_list

    @ray.remote
    def get_partitions(self):
        indices = [i*(0.01 * MAX_SUBLIST_SIZE)
                   for i in range(1, len(self.edges))]
        global_sample_list_oid = self.get_global_sampling
        return [global_sample_list_oid[i] for i in indices]

    @ray.remote
    def partition_sublists(self, list_oid):
        partion_bounds = self.get_partitions()
        partitioned_sublist = [[] for i in range(len(partitions_bounds + 1))]
        for edge in list_oid:
            if edge.destination <= partition_bounds[0]:
                partitioned_sublist[0].append(edge)
            if edge.destination > partition_bounds[-1]:
                partitioned_sublist[-1].append(edge)
            for i in range(1, len(partition_bounds) - 1):
                if edge.destination > partition_bounds[i] and
                   edge.destination <= partition_bounds[i + 1]:
                    paritioned_sublist[i].append(edge)
        partition_oids = np.array([])
        for sublist in partitioned_sublist:
            partition_oids = np.append(partition_oids,
                                       ray.put(sublist))
        return partition_oids

        # case statement to compare each value in the list to the bound
        # at this point you have list of sublists
        # return that list of sublists

    @ray.remote
    def merge_common_partitions(self):
        # TODO: How to send this function to a remote object - is it just pass
        # oid as argument?
        merged_oid_groupings = []
        new_local_edges = np.array([])
        for list_oid in self.local_edges:
            merged_oid_groupings.append(self.partition_sublists.remote(list_oid))

        for i in range(len(merged_oid_groupings[0])):
            new_partition = np.array([])
            for j in range(len(merged_oid_groupings)):
                new_partition = np.append(new_partition,
                                          ray.get(merged_oid_groupings[j][i]))
                new_partition_oid = ray.put(new_partition)
                np.append(new_local_edges, new_partition_oid)
        self.edges = new_local_edges
