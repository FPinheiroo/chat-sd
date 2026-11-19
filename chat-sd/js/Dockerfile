FROM node:20-bookworm-slim
WORKDIR /app
COPY js/package.json .
RUN npm install --omit=dev
COPY proto/chat.proto .
COPY js/ .
