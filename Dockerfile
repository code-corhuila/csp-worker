FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY worker-app/target/worker-app-0.1.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
