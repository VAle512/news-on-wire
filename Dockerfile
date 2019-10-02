FROM openjdk:8

COPY ./target/pages-classification-alpha-0.1.jar /pages-classification-alpha-0.1.jar
COPY ./src /app/src #temporary added this folder, while we solve log4j bug
# COPY ./config /app/config

ENV CONFIG_FOLDER=/app/config
ENV DATA_FOLDER=/app/data
ENV GOLDENS_FOLDER=/app/goldens
ENV DEBUG_FOLDER=/app/debug
ENV DATASETS_FOLDER=/app/csv
ENV RESULTS_FOLDER=/app/results
	
EXPOSE 4040

# Run the jar
CMD ["java","-jar","-Dlogs=/app/logs","-Dcli=true","-Dforce-crawling=false","/pages-classification-alpha-0.1.jar"]

