"""Helper functions for the fraud_users_disabler DAG"""
from datetime import datetime, timedelta
import requests

ES_URL = "https://vpc-prod-backend-logging-wmfzcc4k7dknnwgzkm5gnua2o4.us-east-1.es.amazonaws.com"
es_session = requests.Session()


def es_search(query, index):
    """Execute a search query to the ElasticSearch Cluster"""
    res = es_session.get(
        ES_URL + f"/{index}/_search",
        json=query
    ).json()
    return res


def es_scan(query, index, scroll="5m"):
    """Wrapper to scroll through pages of search results"""
    resp = es_session.post(
        ES_URL + f"/{index}/_search?scroll={scroll}",
        json=query
    ).json()
    scroll_id = resp.get("_scroll_id")

    try:
        while scroll_id and resp["hits"]["hits"]:
            for hit in resp["hits"]["hits"]:
                yield hit

            resp = es_session.get(
                ES_URL + "/_search/scroll",
                json={"scroll_id": scroll_id, "scroll": scroll}
            ).json()
            scroll_id = resp.get("_scroll_id")

    finally:
        if scroll_id:
            es_session.delete(
                ES_URL + "/_search/scroll",
                json={"scroll_id": scroll_id}
            )


def find_users_sharing_callkit_tokens(start_ts: datetime, end_ts: datetime, min_count=100):
    """A generator of usernames tied to callkit_push_tokens that are shared by more than `min_count` times.
    Note the produced list of usernames are not necessarily unique
    """
    index = []
    index_date = start_ts.date()
    while index_date <= end_ts.date():
        index.append("logstash-tn_requests_log-medium-" + index_date.strftime("%Y.%m.%d"))
        index_date += timedelta(days=1)
    index = ",".join(index)

    filters = [
        {"term": {"client_type.keyword": {"value": "TN_IOS_FREE"}}},
        {"term": {"route_name.keyword": {"value": "SessionsController_update"}}},
        {"exists": {"field": "username"}},
        {"range": {
            "@timestamp": {
                "format": "strict_date_optional_time",
                "gte": start_ts.isoformat(),
                "lte": end_ts.isoformat()
            }
        }}
    ]

    # Step 1: Find all callkit_push_tokens that appear in more than `min_count` docs
    # FIXME: this will only return 10000 callkit tokens
    query_shared_callkit_tokens = {
        "size": "0",  # only aggregations are needed
        "aggs": {
            "freq_callkit_token": {
                "terms": {
                    "field": "request_params.callkit_push_token.keyword",
                    "min_doc_count": min_count,
                    "size": 10000
                },
                "aggs": {
                    "user_count": {"cardinality": {"field": "username.keyword", "precision_threshold": min_count * 10}}
                }
            }
        },
        "query": {
            "bool": {
              "must": filters + [{"exists": {"field": "request_params.callkit_push_token"}}]
            }
        }
    }

    res = es_search(query_shared_callkit_tokens, index)
    callkit_tokens = [
        bucket["key"]
        for bucket in res["aggregations"]["freq_callkit_token"]["buckets"]
        if bucket["user_count"]["value"] >= min_count
    ]

    # Step 2: Look up usernames corresponding to the callkit_push_tokens retrieved above
    # FIXME: if there are more than 65536 callkit_tokens, the `terms` query will fail and we must
    # break it down into smaller batches
    query_username = {
        "size": 5000,  # batch size
        "_source": ["username"],
        "query": {
            "bool": {
              "must": filters + [{"terms": {"request_params.callkit_push_token": callkit_tokens}}]
            }
        }
    }

    for doc in es_scan(query_username, index):
        yield doc["_source"]["username"]
