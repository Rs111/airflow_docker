from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from ssl import create_default_context

# from elasticsearch import Elasticsearch

# if implemented correctly, this class would be available to DAGs (from airflow.hooks.elasticsearch_hook import ElasticsearchHook)
# class ElasticsearchHook(BaseHook, LoggingMixin):
#     """
#     hook to interact with ElasticSearch
#     """
#     x = 2
