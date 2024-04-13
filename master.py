import grpc
import mapper_pb2
import mapper_pb2_grpc
import reducer_pb2
import reducer_pb2_grpc
import subprocess
import time
from random import uniform
from concurrent import futures
import os
import shutil
import glob
import numpy as np

startingPort = 55060
mrTimeOut = 5
mapperHandler = None
reducerHandler = None
precision = 5

class MapperHandler:
    def __init__(self, mapperCount, reducerCount, centroidCount):
        self.processes = {}
        self.ports = {}
        self.reducerCount = reducerCount
        self.mapperCount = mapperCount
        self.centroidCount = centroidCount
        self.createMapper()
        self.channelList = dict((key, grpc.insecure_channel(f'localhost:{port}', options=[('grpc.default_timeout_ms', 2000)])) for key, port in self.ports.items())
        self.stubList = dict((key, mapper_pb2_grpc.MapperStub(channel)) for key, channel in self.channelList.items())
    def map(self):
        req = mapper_pb2.MapRequest()
        req.ReducerCount = self.reducerCount
        req.CentroidCount = self.centroidCount
        def parallelMapping(mapperId):
            while True:
                rep = exceptionalGRPCcall(self.stubList[mapperId].Map, req, mapperId, 'Mapper')
                if rep == None:
                    writeDump(f'Master: mapper {mapperId} failed to responed, respawning mapper')
                    self.spawnProcess(mapperId)
                elif rep.Success:
                    return rep
                else:
                    writeDump(f'Master: mapper {mapperId} failed to complete, resending request to mapper')
        with futures.ThreadPoolExecutor(max_workers = 1) as executor:
            results = executor.map(parallelMapping, self.stubList.keys())
        return results
    def createMapper(self):
        global startingPort
        for mapperId in range(self.mapperCount):
            startingPort += 1
            self.processes[str(mapperId + 1)] = subprocess.Popen(['python3', 'mapper.py', str(mapperId + 1), str(startingPort)])
            self.ports[str(mapperId + 1)] = startingPort
    def spawnProcess(self, mapperId):
        startingPort += 1
        self.processes[mapperId].terminate()
        self.processes[mapperId] = subprocess.Popen(['python3', 'mapper.py', mapperId, str(startingPort)])
        self.ports[mapperId] = startingPort
        self.channelList[mapperId] = grpc.insecure_channel(f'localhost:{startingPort}', options=[('grpc.default_timeout_ms', 2000)])
        self.stubList[mapperId] = mapper_pb2_grpc.MapperStub(self.channelList[mapperId])
    def killAll(self):
        for process in self.processes.values():
            process.terminate()
            # process.kill()

class ReducerHandler:
    def __init__(self, reducerCount, centroidCount):
        self.processes = {}
        self.ports = {}
        self.reducerCount = reducerCount
        self.centroidCount = centroidCount
        self.centroids = [[] for _ in range(centroidCount)]
        self.createReducer()
        self.channelList = dict((key, grpc.insecure_channel(f'localhost:{port}', options=[('grpc.default_timeout_ms', 2000)])) for key, port in self.ports.items())
        self.stubList = dict((key, reducer_pb2_grpc.ReducerStub(channel)) for key, channel in self.channelList.items())
    def reduce(self):
        def parallelReducing(reducerId):
            req = reducer_pb2.ReduceRequest()
            for filepath in glob.glob(f'Mappers/M*/partition_{reducerId}.txt'):
                with open(filepath) as file:
                    req.PartitionClusters.extend(file.readlines())
            req.CentroidCount = self.centroidCount
            while True:
                rep = exceptionalGRPCcall(self.stubList[reducerId].Reduce, req, reducerId, 'Reducer')
                if rep == None:
                    writeDump(f'Master: reducer {reducerId} failed to responed, respawning reducer')
                    self.spawnProcess(reducerId)
                elif rep.Success:
                    return rep
                else:
                    writeDump(f'Master: reducer {reducerId} failed to complete, resending request to reducer')
        with futures.ThreadPoolExecutor(max_workers = 1) as executor:
            results = executor.map(parallelReducing, self.stubList.keys())
        for result in results:
            if result and result.Success:
                for centroid in result.centroids:
                    centroid = list(map(float, centroid.strip().split(',')))
                    centroid[0] = int(centroid[0])
                    self.centroids[centroid[0]].append(centroid)
    def centroidCompilation(self):
        testCentroid1 = []
        with open('centroids.txt') as file:
            for centroid in file.readlines():
                centroid = list(map(float, centroid.strip().split(',')))
                testCentroid1.append(centroid)
        testCentroid2 = []
        with open('centroids.txt', 'w') as file:
            for centroidCluster in self.centroids:
                centroid = np.mean(centroidCluster, axis=0)[1:]
                testCentroid2.append(centroid)
                file.write(f"{','.join(map(str, centroid))}\n")
        writeDump(f'Master: new Centroids {testCentroid2}')
        if np.sum(np.abs(np.subtract(testCentroid1, testCentroid2)) > pow(10, -precision)) > 0:
            return False
        else:
            return True
    def createReducer(self):
        global startingPort
        for ReducerId in range(self.reducerCount):
            startingPort += 1
            self.processes[str(ReducerId + 1)] = subprocess.Popen(['python3', 'reducer.py', str(ReducerId + 1), str(startingPort)])
            self.ports[str(ReducerId + 1)] = startingPort
    def spawnProcess(self, reducerId):
        startingPort += 1
        self.processes[reducerId].terminate()
        self.processes[reducerId] = subprocess.Popen(['python3', 'reducer.py', reducerId, str(startingPort)])
        self.ports[reducerId] = startingPort
        self.channelList[reducerId] = grpc.insecure_channel(f'localhost:{startingPort}', options=[('grpc.default_timeout_ms', 2000)])
        self.stubList[reducerId] = reducer_pb2_grpc.ReducerStub(self.channelList[reducerId])
    def killAll(self):
        for process in self.processes.values():
            process.terminate()
            # process.kill()

def exceptionalGRPCcall(rpc, req, Id, type):
    try:
        writeDump(f'Master: sending GRPC calls to {type} {Id}')
        rep = rpc(req, timeout=mrTimeOut)
        writeDump(f'Master: {type} {Id} result {rep.Success}')
        return rep
    except Exception as e:
        writeDump(f'Master: Error occurred while sending RPC to {type} {Id}, {e}')
        return None

def readPoints(file):
    points = []
    with open(file) as pointfile:
        for point in pointfile:
            x1, y1 = map(float, point.strip().split(','))
            # x1, y1 = point.strip().split(',')
            points.append((x1, y1))
    return points

def generateRandomCentroids(points, centroidCount):
    minVal, maxVal = float('inf'), float('-inf')
    for point in points:
        minVal = min(min(point[0], minVal), point[1])
        maxVal = max(max(point[0], maxVal), point[1])
    centroids = [(uniform(minVal, maxVal), uniform(minVal, maxVal)) for _ in range(centroidCount)]
    with open('centroids.txt', 'w') as file:
        for centroid in centroids:
            file.write(f'{",".join(map(str, centroid))}\n')
    writeDump(f"Master: random Centroid {centroids}")
    return centroids

def writeDump(logs):
    with open('dumps.txt', 'a') as file:
        file.write(logs + '\n')
    print(logs)

def createShards(points, mapperCount):
    for file in glob.glob("Input/M*.txt") + glob.glob('Reducers/*'):
        os.remove(file)
    for folder in glob.glob('Mappers/*'):
        shutil.rmtree(folder)
    for mapperId in range(mapperCount):
        os.mkdir(f'Mappers/M{mapperId + 1}')
        with open(f'Input/M{mapperId + 1}.txt', 'w') as file:
            for point in points[mapperId::mapperCount]:
                file.write(f'{",".join(map(str, point))}\n')

def serve():
    mapperCount = input('number of mappers (M)')
    reducerCount = input('number of reducers (R)')
    centroidCount = input('number of centroids (K)')
    iterationCount = input('number of iterations for K-Means')
    # mapperCount = 1
    # reducerCount = 1
    # centroidCount = 2
    # iterationCount = 20
    points = readPoints('Input/points.txt')
    generateRandomCentroids(points, centroidCount)
    createShards(points, mapperCount)
    global mapperHandler, reducerHandler
    mapperHandler = MapperHandler(mapperCount, reducerCount, centroidCount)
    reducerHandler = ReducerHandler(reducerCount, centroidCount)
    open('dumps.txt', 'w').close()
    time.sleep(2)
    for iterCount in range(iterationCount):
        writeDump(f'Master: Iteration {iterCount} start')
        mapperHandler.map()
        reducerHandler.reduce()
        if reducerHandler.centroidCompilation():
            writeDump(f"Master: On Iteration {iterCount} Centroids converges with precision {precision}")
            break
    mapperHandler.killAll()
    reducerHandler.killAll()
    writeDump('Master: centroids calculated')

if __name__ == "__main__":
    try:
        writeDump(f'Master: starts')
        serve()
    except Exception as e:
        if mapperHandler:
            mapperHandler.killAll()
        if reducerHandler:
            reducerHandler.killAll()
        print('Interrupt', e)