FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace

COPY pom.xml .
RUN mvn -q -Dmaven.test.skip=true dependency:go-offline

COPY src ./src
RUN mvn -q -Dmaven.test.skip=true package

FROM eclipse-temurin:17-jre
WORKDIR /app

COPY --from=build /workspace/target/minikafka-0.1.0-SNAPSHOT.jar /app/minikafka.jar

EXPOSE 8080 9092
VOLUME ["/app/data"]

ENTRYPOINT ["java", "-jar", "/app/minikafka.jar"]
