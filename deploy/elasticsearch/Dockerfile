FROM docker.elastic.co/elasticsearch/elasticsearch:7.7.0
RUN mkdir /var/local/config
COPY ./es_schema.sh /var/local/config/es_schema.sh
RUN source /var/local/config/es_schema.sh
EXPOSE 9200