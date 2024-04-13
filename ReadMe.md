For installing the required dependencies
    pip install grpcio grpcio-tools
For generating the proto files
    python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. mapper.proto
    python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. reducer.proto