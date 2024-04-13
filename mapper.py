import grpc
import mapper_pb2
import mapper_pb2_grpc
from concurrent import futures
import sys
import time
from math import sqrt
from random import uniform
import numpy as np

mapperId = 1
recieverPort = '50051'

class Point():
    def __init__(self, x, y):
        self.x = x
        self.y = y
    def euclidean(self, point):
        return sqrt((self.x - point.x) ** 2 + (self.y - point.y) ** 2)
    def __str__(self):
        return f'{self.x},{self.y}'

class KMClusteringMapper():
    def __init__(self, mapperId, points, centroids, reducerCount, centroidCount):
        self.points = points
        self.centroids = centroids
        self.centroidCount = centroidCount
        self.mapperId = mapperId
        self.reducerCount = reducerCount
    def map(self):
        self.clusters = tuple([] for _ in range(len(self.centroids)))
        for point in self.points:
            centroidid = np.argmin(list(map(point.euclidean, self.centroids)))
            self.clusters[centroidid].append(point)
        return self.clusters
    def partition(self):
        partitionfd = [open(f'Mappers/M{self.mapperId}/partition_{partitionId + 1}.txt', 'w') for partitionId in range(self.reducerCount)]
        clusterlen = 0
        for clusterId, cluster in enumerate(self.clusters):
            for pointId, point in enumerate(cluster):
                partitionfd[(clusterlen + pointId) % self.reducerCount].write(f'{clusterId},{point}\n')
            clusterlen += len(cluster)
        map(lambda x: x.close(), partitionfd)

def readPoints(file):
    points = []
    with open(file) as pointfile:
        for point in pointfile:
            x1, y1 = map(float, point.strip().split(','))
            points.append(Point(x1, y1))
    return points

def writeDump(logs):
    with open('dumps.txt', 'a') as file:
        file.write(logs + '\n')
    print(logs)

class MapperServicer(mapper_pb2_grpc.MapperServicer):
    def Map(self, request, context):
        rep = mapper_pb2.MapReply()
        try:
            points = readPoints(f'Input/M{mapperId}.txt')
            centroids = readPoints(f'centroids.txt')
            kmeans = KMClusteringMapper(mapperId, points, centroids, request.ReducerCount, request.CentroidCount)
            writeDump(f'Mapper {mapperId}: mapping')
            kmeans.map()
            writeDump(f'Mapper {mapperId}: partitioning')
            kmeans.partition()
            print(f'Mapper {mapperId}: completed')
            rep.Success = True
        except Exception as e:
            print(f"Mapper {mapperId}: failed, {e}")
            rep.Success = False
        return rep

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapper_pb2_grpc.add_MapperServicer_to_server(MapperServicer(), server)
    server.add_insecure_port("[::]:" + recieverPort)
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    try:
        mapperId = sys.argv[1]
        recieverPort = sys.argv[2]
        print(f'Mapper {mapperId}: start with port {recieverPort}')
        serve()
        print('mapper ended')
    except Exception as e:
        print(f'Mapper {id} is closed, due to interrupt', e)
