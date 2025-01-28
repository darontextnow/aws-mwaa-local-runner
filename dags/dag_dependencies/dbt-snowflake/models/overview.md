{% docs overview %}

## TextNow Data Model Documentation

The data models are organized under textnow/models in the panel on the left:

- core
Data models that are shared across an entire business are grouped under core.

- growth
Data models that are specific to growth

- monetization
Data models that are specific to monetization

- metrics
Metrics and KPIs

- staging [DSE internal]
The intermediate transformations that connect raw data sources and the final models under core, growth and metrics. Normally you wouldn't need to query directly from these tables, and the structure can evolve over time and is subject to change.

- support [DSE internal]
Other miscellaneous supporting tables.

{% enddocs %}
