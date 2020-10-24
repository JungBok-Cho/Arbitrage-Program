"""
CPSC 5520-01, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: JungBok Cho
:Version: 1.0
"""
import selectors
import socket
import sys
from datetime import datetime
import fxp_bytes_subscriber
from bellman_ford import Bellman

BUF_SZ = 4096  # Default socket.recv buffer size
CHECK_INTERVAL = 0.2  # Seconds between checking for failure timeout
TIMEOUT = 60  # Control timeout (in seconds)


class Lab3(object):
    """
    This class is built to listen to currency exchange rates from a price feed
    and print out a message whenever there is an arbitrage opportunity available
    """

    def __init__(self, gcd_address):
        """
        Constructor of Lab3 class

        :param gcd_address: Publisher's host and port
        """
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        self.listener, self.listener_address = self.start_a_listener()
        self.selector = selectors.DefaultSelector()
        self.latestTime = datetime(1970, 1, 1)  # Latest time we received quote
        self.currTimeStamp = {}  # Dictionary to store quote and its receipt time
        self.bellmanObj = Bellman()  # Object of Bellman class
        self.clockCheckStop = datetime.now()  # Time to check timeout

    def run(self):
        """ Start to get quotes and find arbitrage """
        print('starting up on {} port {}'.format(*self.listener_address))
        self.selector.register(self.listener, selectors.EVENT_READ)

        # Serialize our listener's host and port
        serializedAdd = fxp_bytes_subscriber.serialize_address(
            self.listener_address[0], self.listener_address[1])

        # Contact with Publisher
        self.listener.sendto(serializedAdd, self.gcd_address)

        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                data = self.receive_message()
                self.removeOldQuote()
                self.createGraph(data)
                self.arbitrage()
            self.checkTimeout()

    @staticmethod
    def start_a_listener():
        """
        Create a listener socket

        :return: Return listener socket and its socket address
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(('localhost', 0))
        return listener, listener.getsockname()

    def receive_message(self):
        """
        Receive messages from a publisher

        :return: Return unmarshalled message
        """
        try:
            self.clockCheckStop = datetime.now()
            data = self.listener.recvfrom(BUF_SZ)
            return fxp_bytes_subscriber.unmarshal_message(data[0])
        except ConnectionError as err:
            # a ConnectionError means it will never succeed
            print('closing: {}'.format(err))
            return
        except Exception as err:
            # other exceptions we assume are due to being non-blocking;
            # we expect them to succeed in future
            print('failed {}'.format(err))
            return

    def removeOldQuote(self):
        """ Remove outdated quotes """
        if self.currTimeStamp:
            for key, value in list(self.currTimeStamp.items()):
                if 1.5 < (datetime.utcnow() - value).total_seconds():
                    curr1 = key[0:3]
                    curr2 = key[3:6]
                    print('removing stale quote for (\'{}\', \'{}\')'.
                          format(curr1, curr2))
                    self.bellmanObj.removeEdge(curr1, curr2)
                    del self.currTimeStamp[key]

    def createGraph(self, arr):
        """
        Create a graph

        :param arr: list of quote structures
        """
        for x in arr:
            print(x)
            currTime = datetime.strptime(x[0:26], '%Y-%m-%d %H:%M:%S.%f')
            if self.latestTime <= currTime:
                self.latestTime = currTime
                curr1 = x[27:30]
                curr2 = x[31:34]
                currRatio = float(x[35:])
                self.currTimeStamp[curr1 + curr2] = currTime
                self.bellmanObj.addEdge(curr1, curr2, currRatio)
            else:
                print('ignoring out-of-sequence message')

    def arbitrage(self):
        """ Detect arbitrage """
        # Iteratively find arbitrage with existing currencies
        for x in self.bellmanObj.getCurrencies():
            dist, prev, neg_edge = self.bellmanObj.shortest_paths(x)
            if neg_edge is not None:  # Operate if arbitrage is found
                isNegCycle = self.getCycle(dist, prev, neg_edge)
                if isNegCycle:
                    break

    def getCycle(self, dist, prev, neg_edge):
        """
        Find a negative cycle

        :param dist: Dictionary keyed by vertex of shortest distance from
                     start_vertex to that vertex
        :param prev: Dictionary keyed by vertex of previous vertex in shortest
                     path from start_vertex
        :param neg_edge: An edge, (u,v), to find the negative cycle
        :return: Return printCycle function's result
        """
        cycle = []       # To store a negative cycle
        v = neg_edge[1]  # Ending node in the cycle

        # Start to find the cycle
        while True:
            if v is None:
                return False
            cycle.append(v)
            # To break possible infinity loop
            if len(cycle) > len(dist):
                break
            if v == neg_edge[1] and len(cycle) > 1:
                break
            v = prev[v]

        # Check if the ending price of arbitrage has reasonable profit
        if cycle[0] != cycle[len(cycle) - 1]:
            return False

        cycle.reverse()
        return self.printCycle(cycle)

    def printCycle(self, cycle):
        """
        Print out the negative cycle

        :param cycle: The detected negative cycle
        :return: Return True if the profit is reasonable.
                 Otherwise, return False
        """
        arbitrageResult = '\tstart with {} 100\n'.format(cycle[0])
        currentPrice = 100
        for i in range(len(cycle) - 1):
            ratio = self.bellmanObj.getCurrencyRatio(cycle[i], cycle[i + 1])
            if ratio < 0:  # currency 1 -> currency 2
                ratio = abs(ratio)
            else:  # currency 2 -> currency 1
                ratio = 1 / ratio
            currentPrice = ratio * currentPrice
            arbitrageResult += '\texchange {} for {} at {} --> {} {}\n'.format(
                cycle[i], cycle[i + 1], ratio, cycle[i + 1], currentPrice)

        print('ARBITRAGE:')
        print(arbitrageResult)
        return True

    def checkTimeout(self):
        """ Check if the publisher stops sending messages longer than 1 minute """
        if TIMEOUT <= (datetime.now() - self.clockCheckStop).total_seconds():
            print('Didn\'t received messages for 1 minute - Program ends')
            exit(0)


if __name__ == '__main__':
    if not 3 == len(sys.argv):
        print("Usage: python3 lab3.py PublisherHOST PublisherPORT")
        exit(1)

    lab3 = Lab3(sys.argv[1:3])
    lab3.run()
