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

# Copy script for awaiting db ready
COPY ./wait-for-it.sh /

# Publish port on wich will be displayed spark application
EXPOSE 4040

# Default command by Luigi (overwriteable at runtime)
CMD ["java","-jar","-Xms24g","-Xmx50g","-Dlogs=/app/logs","-Dcli=true","-Dforce-crawling=false","pages-classification-alpha-0.1-jar-with-dependencies.jar"]



