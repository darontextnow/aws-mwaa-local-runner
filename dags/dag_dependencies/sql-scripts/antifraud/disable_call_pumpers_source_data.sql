WITH info AS (
    SELECT to_contact_value,
        --  Term cost is the total cost of these calls i.e. rate * the sum of call duration
        SUM(term_cost) AS term_cost,
        --  sum of call duration in seconds
        SUM(callduration) AS sum__call_duration,
        COUNT(*) AS count,
        COUNT(DISTINCT(username)) AS users,
        --  Term rate is the cost per minute of the call - on redash it sometimes shows 0.0 but that because it is being rounded down from something like 0.031
        term_rate AS term_rate,
        --  Unique user ratio is ratio of the number of calls divided by unique/distinct usernames
        --  This gives you a sense of how many distinct users were making the calls
        COUNT(*)/ COUNT(DISTINCT(username)) AS unique_user_ratio,
        AVG(callduration) AS avg__call_duration
        FROM core.oncallend_callbacks
        WHERE
            (created_at >= '{{ execution_date.strftime("%Y-%m-%dT%H:%M:%S") }}')
            AND (created_at < '{{ next_execution_date.strftime("%Y-%m-%dT%H:%M:%S") }}')
            AND (to_country_code = '1')
            --  Currently domestic is considered US, Canada, and Puerto Rico
            AND (to_region_code IN ('US', 'CA', 'PR'))
            AND (termstatus = 'sip:200')
            AND (call_direction = 'outgoing')
            AND (term_cost != 0)
            AND (NOT to_contact_value regexp '(\\+?1)?(8(00|33|44|55|66|77|88)[2-9]\\d{6})')  -- exclude tollfree number
        GROUP BY to_contact_value, term_rate
        ORDER BY term_cost DESC LIMIT 10
),
call_pumping AS (
    SELECT to_contact_value
    FROM info
    WHERE CASE
        -- Term Cost is how much TextNow got charged for these/this call[s]
        WHEN term_cost > 20 THEN 1
        -- OR when the number of users who called ONE specific number is greater than 10
        -- NOTE: The 10 users are textnow and that one specific number can be in network or out of network [most likely out of network]
        -- This query follows an OR structure - that means if the number of users is LESS than 10 than just get rid of it
        -- IF it is greater than 10 then follow down the chain of logic and keep filtering out
        -- This 10 number was just decided
        WHEN users < 10 THEN 0
        -- If average call duration to that specific number is greater than 1k seconds/16.67 minutes [this number was just decided]
        WHEN info.avg__call_duration < 1000 THEN 0
        -- OR the number of calls to that specific number is greater than 250 [this number is slightly high]
        -- Note think of public numbers like radio stations [yes I know this is opposite of what I wrote above]
        WHEN COUNT < 250 AND term_rate = 0.00 THEN 0
        -- Filter out any calls that costed less than $5 - cause who cares
        WHEN term_cost < 5 THEN 0
        ELSE 1
    END = 1
)
SELECT DISTINCT username
FROM core.oncallend_callbacks
WHERE
    (created_at >= '{{ execution_date.strftime("%Y-%m-%dT%H:%M:%S") }}')
    AND (created_at < '{{ next_execution_date.strftime("%Y-%m-%dT%H:%M:%S") }}')
    AND (call_direction = 'outgoing')
    AND (to_contact_value IN (SELECT to_contact_value FROM call_pumping))
