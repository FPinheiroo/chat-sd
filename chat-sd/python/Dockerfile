FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir pyzmq grpcio-tools
COPY proto/chat.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. chat.proto
COPY python/ .
