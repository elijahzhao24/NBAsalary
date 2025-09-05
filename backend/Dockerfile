# ---------- build stage ----------
FROM maven:3.9.4-eclipse-temurin-17 AS build
WORKDIR /app

COPY pom.xml ./
COPY .mvn .mvn
RUN mvn -q -DskipTests dependency:go-offline

COPY src ./src
RUN mvn -q -DskipTests package

# ---------- runtime stage ----------
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# copy jar from build stage (assumes single jar in target)
COPY --from=build /app/target/*.jar app.jar

# small performance tweak
ENV JAVA_TOOL_OPTIONS="-Djava.security.egd=file:/dev/./urandom"

# let platform inject PORT env var (Spring reads server.port=${PORT:8080})
EXPOSE 8080
ENTRYPOINT ["sh","-c","java $JAVA_TOOL_OPTIONS -jar /app/app.jar"]
