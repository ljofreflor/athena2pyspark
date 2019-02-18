# docker build -t athena2pyspark .
FROM ubuntu:14.04.5

RUN apt-get update --fix-missing -yq && apt-get upgrade -yq 

RUN apt-get install zip -yq

COPY ./athena2pyspark athena2pyspark

CMD cd /athena2pyspark; zip athena2pyspark.zip ./*; mv athena2pyspark.zip /athena2pyspark.zip