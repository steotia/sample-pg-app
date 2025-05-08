# TOIL to get the right image working
FROM maven:3.8-openjdk-17 AS builder
RUN git clone https://github.com/GoogleCloudPlatform/pgadapter.git /pgadapter
WORKDIR /pgadapter
RUN mvn package -DskipTests

# Find the actual JAR file created
RUN find /pgadapter/target -name "pgadapter-*.jar" | grep -v "sources\|javadoc"

FROM openjdk:17-slim
# Copy the JAR file from the builder stage
COPY --from=builder /pgadapter/target/pgadapter-*.jar /pgadapter.jar
# Create an entrypoint script that correctly handles arguments
RUN echo '#!/bin/sh' > /entrypoint.sh && \
    echo 'java -jar /pgadapter.jar "$@"' >> /entrypoint.sh && \
    chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]