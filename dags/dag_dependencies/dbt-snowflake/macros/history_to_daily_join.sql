/* {#
Create a join condition with source('support', 'dates') for a history table
to generate daily rows between `from_ts` and `to_ts`. `to_ts`
can be NULL, which represents infinity into future.

Example usage:

With `hist`

id |    from_ts          |       to_ts
---+---------------------+--------------------
 1 | 2020-01-01 04:00:00 | 2020-01-03 14:00:00
 2 | 2020-01-02 04:00:00 | 2020-01-03 00:00:00

After the join:

 date_utc   | id |    from_ts          |       to_ts
------------+----+---------------------+--------------------
 2020-01-01 |  1 | 2020-01-01 04:00:00 | 2020-01-03 14:00:00
 2020-01-02 |  1 | 2020-01-01 04:00:00 | 2020-01-03 14:00:00
 2020-01-03 |  1 | 2020-01-01 04:00:00 | 2020-01-03 14:00:00
 2020-01-02 |  2 | 2020-01-02 04:00:00 | 2020-01-03 00:00:00

```sql
SELECT
    date_utc,
    hist.*
FROM hist
{{ history_to_daily_join('hist.from_ts', 'hist.to_ts') }}
WHERE (date_utc <= GETDATE())
```

Notes:
If periods [A, B] and [C, D] overlap, then A <= D AND B >= C

For right-opened intervals [A, B) and [C, D) we change <= to <

where
    A = from_ts, B = to_ts or sysdate
    C = date_utc, D = dateadd(day, 1, date_utc)

#} */

{% macro history_to_daily_join(from_ts, to_ts) -%}

JOIN {{ source('support', 'dates') }} AS dates ON
    ({{ from_ts }} < DATEADD(DAY, 1, dates.date_utc))
    AND (COALESCE({{ to_ts }}, CURRENT_TIMESTAMP) > dates.date_utc)

{%- endmacro %}
