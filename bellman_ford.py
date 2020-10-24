"""
CPSC 5520-01, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: JungBok Cho
:Version: 1.0
"""
import math


class Bellman(object):
    """
    Detect a negative cycle to get arbitrage profit in graph
    using Bellman ford algorithm
    """

    def __init__(self):
        """ Constructor of Bellman class """
        self.graph = {}

    def addEdge(self, curr1, curr2, currRatio):
        """
        Add edges to the graph

        :param curr1: First Currency to add
        :param curr2: Second Currency to add
        :param currRatio: Currency ratio
        """
        if curr1 not in self.graph.keys():
            self.graph[curr1] = {curr2: -currRatio}
        else:
            self.graph[curr1][curr2] = -currRatio
        if curr2 not in self.graph.keys():
            self.graph[curr2] = {curr1: currRatio}
        else:
            self.graph[curr2][curr1] = currRatio

    def removeEdge(self, curr1, curr2):
        """
        Remove edges from the graph

        :param curr1: First currency to remove
        :param curr2: Second currency to remove
        """
        try:
            del self.graph[curr1][curr2]
            del self.graph[curr2][curr1]
        except:
            return

    def getCurrencyRatio(self, curr1, curr2):
        """
        Getter for currency ratio

        :param curr1: First currency to find
        :param curr2: Second currency to find
        :return: Return the requested currencies' ratio
        """
        return self.graph[curr1][curr2]

    def getCurrencies(self):
        """
        Getter for list of existing currencies

        :return: Return list of existing currencies
        """
        return list(self.graph.keys())

    def shortest_paths(self, start_vertex, tolerance=1e-12):
        """
        Find the shortest paths (sum of edge weights) from start_vertex to every other vertex.
        Also detect if there are negative cycles and report one of them.
        Edges may be negative.

        :param tolerance: only if a path is more than tolerance better will it be relaxed
        :param start_vertex: start of all paths
        :return: distance, predecessor, negative_cycle
            distance:       dictionary keyed by vertex of shortest distance from start_vertex to that vertex
            predecessor:    dictionary keyed by vertex of previous vertex in shortest path from start_vertex
            negative_cycle: None if no negative cycle, otherwise an edge, (u,v), in one such cycle
        """
        currencies = self.graph.keys()

        # Dictionary to store lowest sum of edge weights
        dist = {v: float("Inf") for v in currencies}

        # Dictionary to store shortest paths of negative cycles
        prev = {}

        dist[start_vertex] = 0
        prev[start_vertex] = None

        # Relax all edges |V| - 1 times
        for _ in range(len(currencies) - 1):
            # Update dist value and parent index of the adjacent vertices of
            # the picked vertex
            for u, uValue in self.graph.items():
                for v, w in uValue.items():
                    w = self.moneyToLog(w)
                    if dist[u] != float("Inf") and dist[u] + w < dist[v]:
                        if dist[v] - (dist[u] + w) >= tolerance:
                            dist[v] = dist[u] + w
                            prev[v] = u

        neg_edge = None  # Last edge in the negative cycle

        # Detect the negative cycle
        for u, uValue in self.graph.items():
            for v, w in uValue.items():
                w = self.moneyToLog(w)
                if dist[u] != float("Inf") and dist[u] + w < dist[v]:
                    neg_edge = (v, u)
                    return dist, prev, neg_edge

        return dist, prev, neg_edge

    @staticmethod
    def moneyToLog(w):
        """ Change currency ratio to log number"""
        if w < 0:  # currency 1 -> currency 2
            return -math.log(abs(w), 10)
        else:  # currency 2 -> currency 1
            return math.log(w, 10)
