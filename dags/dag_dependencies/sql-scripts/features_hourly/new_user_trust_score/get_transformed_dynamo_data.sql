SELECT
    OBJECT_CONSTRUCT(
        'user_id', OBJECT_CONSTRUCT('S', user_id),
        'user_attribute', OBJECT_CONSTRUCT('S', user_attribute),
        'likelihood_of_disable', OBJECT_CONSTRUCT('N', CAST(likelihood_of_disable AS VARCHAR)),
        'positive_factors', OBJECT_CONSTRUCT('S', positive_factors),
        'negative_factors', OBJECT_CONSTRUCT('S', negative_factors),
        'trust_score', OBJECT_CONSTRUCT('N', CAST(trust_score AS VARCHAR))
    ) AS json_data
FROM (
    SELECT
        username AS user_id,
        'new_user_trust_score_v2' AS user_attribute,
        likelihood_of_disable,
        positive_factors,
        negative_factors,
        trust_score
    FROM prod.analytics.new_user_trust_scores
    WHERE
        (inserted_timestamp = '{{ ts }}')
        AND (username IS NOT NULL)

    UNION ALL SELECT
        u.user_id_hex AS user_id,
        'new_user_trust_score_v2' AS user_attribute,
        n.likelihood_of_disable,
        n.positive_factors,
        n.negative_factors,
        n.trust_score
    FROM prod.analytics.new_user_trust_scores n
    LEFT JOIN core.users u ON (n.username = u.username)
    WHERE
        (inserted_timestamp = '{{ ts }}')
        AND (n.username IS NOT NULL)
)
