# Multi-stage build: image cuối chỉ ~200MB (JRE + jar), không chứa Maven/source
FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
WORKDIR /build
COPY pom.xml .
COPY common-contracts/pom.xml common-contracts/
COPY outbox-support/pom.xml outbox-support/
COPY market-gateway/pom.xml market-gateway/
COPY pricing-risk-service/pom.xml pricing-risk-service/
COPY order-service/pom.xml order-service/
COPY matching-engine/pom.xml matching-engine/
RUN mvn -q dependency:go-offline -pl ${MODULE} -am || true
COPY . .
RUN mvn -q clean package -pl ${MODULE} -am -DskipTests

FROM eclipse-temurin:17-jre
ARG MODULE
COPY --from=build /build/${MODULE}/target/*.jar /app/app.jar
# Container-aware JVM: giới hạn heap theo cgroup limit của K8s
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC -XX:MaxGCPauseMillis=50"
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
