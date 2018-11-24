# Use an official Python runtime as a parent image
FROM maven:3.6.0-jdk-8-slim

# Set the working directory to /app
WORKDIR /app

# Copy the pom.xml
ADD pom.xml /app

# Resolve and Download all dependencies: this will be done only if the pom.xml has any changes
RUN mvn verify clean --fail-never

# Copy source code and configs 
COPY ./src /app/src
# COPY ./config /app/config

# Pack everything into a .jar
RUN mvn package
# RUN mvn dependency:copy-dependencies

ENV CONFIG_FOLDER=/app/config
ENV DATA_FOLDER=/app/data
ENV GOLDENS_FOLDER=/app/goldens
	
WORKDIR target

# Run the jar
CMD ["java","-jar","-Dlogs=/app/logs","pages-classification-alpha-0.1.jar"]