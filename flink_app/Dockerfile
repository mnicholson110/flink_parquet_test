FROM maven:latest AS build
WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn clean package -Dskiptests

FROM flink:1.20
WORKDIR /opt/flink/userlib

COPY --from=build /app/target/flink-aggregation-app.jar .
