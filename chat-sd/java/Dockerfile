FROM maven:3.9-eclipse-temurin-17
WORKDIR /app
COPY java/pom.xml .
COPY proto/chat.proto src/main/proto/chat.proto
COPY java/src/main/java src/main/java
RUN mvn -q compile
RUN mvn -q dependency:copy-dependencies -DoutputDirectory=lib
