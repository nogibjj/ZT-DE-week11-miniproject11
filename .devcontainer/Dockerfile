# Install Java (required for Spark)
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Download and install Apache Spark
ENV APACHE_SPARK_VERSION=3.1.2
RUN curl -sL --retry 3 \
  "https://downloads.apache.org/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop3.2.tgz" \
  | gunzip \
  | tar x -C /usr/ \
  && mv /usr/spark-${APACHE_SPARK_VERSION}-bin-hadoop3.2 /usr/spark

# Set environment variables for Spark
ENV SPARK_HOME=/usr/spark
ENV PATH=$PATH:$SPARK_HOME/bin
