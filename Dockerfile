FROM apache/spark:3.5.9
USER root
# JDBC drivers for extraction (SQL Server, Postgres, MySQL) + Delta-free, plain Parquet output
ARG MVN=https://repo1.maven.org/maven2
ADD ${MVN}/com/microsoft/sqlserver/mssql-jdbc/12.8.1.jre11/mssql-jdbc-12.8.1.jre11.jar /opt/spark/jars/
ADD ${MVN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/
ADD ${MVN}/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar /opt/spark/jars/
# S3A (MinIO): versions must match the Hadoop 3.3.4 bundled with Spark 3.5.9
ADD ${MVN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD ${MVN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar && mkdir -p /data /apps /spark-events && chown -R spark:spark /data /apps /spark-events
USER spark
