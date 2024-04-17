import grpc
import reducer_pb2
import reducer_pb2_grpc
from concurrent import futures
import numpy as np
import sys
from random import random

reducerId = 1
recieverPort = '50051'

class KMClusteringReducer():
    def __init__(self, reducerId, partitionClusters, centroidCount):
        self.reducerId = reducerId
        self.partitionClusters = partitionClusters
        self.centroidCount = centroidCount
    def shuffleandsort(self):
        self.clusters = [[] for _ in range(self.centroidCount)]
        for point in self.partitionClusters:
            point = tuple(map(float, point.strip().split(',')))
            self.clusters[int(point[0])].append(point)
        return self.clusters
    def reduce(self):
        centroids = []
        for cluster in self.clusters:
            if cluster == []:
                continue
            centroid = list(np.mean(cluster, axis=0))
            centroid[0] = int(centroid[0])
            centroid = tuple(map(str, centroid))
            centroids.append(centroid)
        with open(f'Reducers/R{self.reducerId}.txt', 'w') as file:
            for centroid in centroids:
                file.write(f"{','.join(centroid)}\n")
        return list(map(lambda x: ','.join(map(str, x)), centroids))

def writeDump(logs):
    with open('dumps.txt', 'a') as file:
        file.write(logs + '\n')
    print(logs)

class ReducerServicer(reducer_pb2_grpc.ReducerServicer):
    def Reduce(self, request, context):
        rep = reducer_pb2.ReduceReply()
        try:
            kmeans = KMClusteringReducer(reducerId, request.PartitionClusters, request.CentroidCount)
            writeDump(f'Reducer {reducerId}: shuffling and sorting')
            kmeans.shuffleandsort()
            writeDump(f'Reducer {reducerId}: reducing')
            cen = kmeans.reduce()
            rep.centroids.extend(cen)
            # rep.Success = True
            if random() >= 0.5:
                rep.Success = True
            else:
                rep.Success = False
        except Exception as e:
            print(f'Reducer {reducerId}: failed, {e}')
            rep.Success = False
        return rep

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reducer_pb2_grpc.add_ReducerServicer_to_server(ReducerServicer(), server)
    server.add_insecure_port(f"[::]:{recieverPort}")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    try:
        reducerId = sys.argv[1]
        recieverPort = sys.argv[2]
        print(f'Reducer {reducerId}: start with port {recieverPort}')
        serve()
    except Exception as e:
        print(f'Reducer {id}, {e}')
