K-Means Clustering using MapReduce Framework

## Requirements
## For installing the required dependencies
```bash
    pip install grpcio grpcio-tools
```
## For generating the proto files
```bash
    python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. mapper.proto
    python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. reducer.proto
```
-------------------------------------------------------------------------------------------------------------------------

## master.py

Functionality:

Initialization:
    Accepts user input for the number of mappers, reducers, centroids, and iterations for K-Means clustering.
    Reads input data points from the file {points.txt}.
    Generates random initial centroids and distributes input data points among mapper processes.

Mapper Handling:
    Creates mapper processes and manages their lifecycle.
    Communicates with mapper processes via gRPC for mapping tasks.
    Handles fault tolerance by respawning failed mapper processes.

Reducer Handling:
    Creates reducer processes and manages their lifecycle.
    Communicates with reducer processes via gRPC for reducing tasks.
    Handles fault tolerance by respawning failed reducer processes.

Iteration Execution:
    Iterates through the K-Means algorithm until convergence or a fixed number of iterations.
    Executes mapping and reducing tasks for each iteration.
    Monitors the convergence of centroids and terminates iterations accordingly.

Logging:
Logs the execution progress and status updates into the {dumps.txt} file for debugging and monitoring purposes.

-------------------------------------------------------------------------------------------------------------------------

## mapper.py

Functionality:

Initialization:
    Accepts command-line arguments for mapper ID and port number to listen for incoming gRPC requests.
    Creates a gRPC server to handle incoming requests from the master.

Mapping:
    Reads input data points from the corresponding input file (Input/M{mapperId}.txt).
    Calculates the Euclidean distance between each data point and centroids.
    Assigns each data point to the nearest centroid and forms clusters.

Partitioning:
    Divides the clusters into partitions based on the number of reducers.
    Writes the partitioned data into separate files (Mappers/M{mapperId}/partition_{partitionId + 1}.txt).

Communication:
    Responds to the master with a success or failure message indicating the completion status of the mapping and partitioning tasks.

-------------------------------------------------------------------------------------------------------------------------
## reducer.py

Functionality:

Initialization:
    Accepts command-line arguments for reducer ID and port number to listen for incoming gRPC requests.
    Creates a gRPC server to handle incoming requests from the master.

Shuffling and Sorting:
    Reads partitioned clusters data from the master processes.
    Sorts the data and organizes it into clusters based on centroid IDs.

Reducing:
    Calculates new centroids for each cluster by computing the mean of all points in the cluster.
    Handles empty clusters by randomly selecting a point from the existing clusters.
    Writes the updated centroids to the output file (Reducers/R{reducerId}.txt).
    Returns the updated centroids to the master.

Communication:
    Respond to the master with the list of centroids and a success or failure message indicating the completion status of the reducing task.

--------------------------------------------------------------------------------------------------------------------------
Fault Tolerance:
    Handles exceptions gracefully and returns a failure message if an error occurs during execution.

Logging:
    Logs the execution progress and status updates into the {dumps.txt} file for debugging and monitoring purposes.

-------------------------------------------------------------------------------------------------------------------------

