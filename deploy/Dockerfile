FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:21-jre
COPY --from=build /src/worker-app/target/worker-app-0.1.0.jar /app.jar
USER 10001
ENTRYPOINT ["java", "-jar", "/app.jar"]
