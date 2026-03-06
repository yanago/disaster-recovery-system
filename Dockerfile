# Build stage
FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /app

COPY pom.xml .
RUN apk add --no-cache maven && \
    mvn -B dependency:go-offline -DskipTests

COPY src ./src
RUN mvn -B package -DskipTests -Dmaven.test.skip=true

# Runtime stage
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

RUN apk add --no-cache dumb-init
COPY --from=build /app/target/disaster-recovery-system-0.1.0-SNAPSHOT.jar app.jar

ENV HTTP_PORT=8080
ENV RECEIVER_PORT=9090
ENV WAREHOUSE_PATH=/data/warehouse
ENV INIT_DEMO_DATA=true
ENV DEMO_EVENTS=50000
ENV ENABLE_RECEIVER=true

EXPOSE 8080 9090

USER 1000:1000
ENTRYPOINT ["dumb-init", "--", "java", "-jar", "app.jar"]
