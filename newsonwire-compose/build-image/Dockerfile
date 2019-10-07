FROM maven:3.6.0-jdk-8-slim

# Copy the app folder including pom.xml
COPY ./app /app

# Resolve and download all dependencies: this will be done only if the pom.xml has any changes
RUN mvn verify clean --fail-never -f /app

# Pack everything into a .jar
RUN mvn package -f /app

# Set all environment variable 
ENV CONFIG_FOLDER=/app/config \
    DATA_FOLDER=/app/data \
    GOLDENS_FOLDER=/app/goldens \ 
    DEBUG_FOLDER=/app/debug \
    DATASETS_FOLDER=/app/csv \
    RESULTS_FOLDER=/app/results

# Set working dir
WORKDIR /app/target

# Install Dockerize that allows to wait for a service ready
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*
ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

# Publish port on wich will be displayed spark application
EXPOSE 4040
