"""This DAG is responsible for computing User Sets.

- A user set is a set of user accounts (usernames).
- User accounts are assigned to the same set if they shared the same device IDs at some point in
  the past, as tracked by Adjust. By extension, a user set is only defined on mobile, and
  technically contains both user accounts and devices.
- User sets are disjoint, a user account / device belongs to one and only one user set, and once
  they are assigned they will not change set (with very limited exceptions).
- Our mobile DAU is defined as number of unique user sets being active on each day. If a set
  contains one or more disabled user account, the entire user set is marked as “bad” and thus
  excluded from DAU count.

Data has to be processed chronologically and one day at a time. If we need to recompute data on date X, all dates
after X have to be reprocessed as well.

See also the dau_process_helper DAG which clears recent runs every day.
"""
from airflow_utils import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dag_dependencies.operators.s3_to_snowflake_deferrable_operator import S3ToSnowflakeDeferrableOperator
from dag_dependencies.operators.dq_checks_operator import DQChecksOperator
from dag_dependencies.constants import DAG_DEFAULT_CATCHUP, AIRFLOW_STAGING_KEY_PFX, DAG_RUN_DATE_1_DAY_LAG
from dag_dependencies.dq_checks_defs import DAUSourceTablesDQChecks
from de_utils.slack.alerts import alert_on_failure
from de_utils.constants import SF_BUCKET, SF_CONN_ID, STAGE_SF, SF_WH_MEDIUM
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2023, 8, 6),
    schedule=None,  # triggered by dau_processing_helper_sf.py
    default_args={
        "owner": "DE Team",
        "retry_delay": timedelta(minutes=3),
        "retries": 3,
        "wait_for_downstream": True,
        "on_failure_callback": alert_on_failure
    },
    max_active_runs=1,
    catchup=DAG_DEFAULT_CATCHUP
)
def dau_processing_sf():
    output_filenames = ["user_set", "device_set", "daily_active_users"]
    s3_input_key = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/inputs/data"
    s3_output_dir = f"{AIRFLOW_STAGING_KEY_PFX}/{{{{ task.dag_id }}}}/{{{{ ds }}}}/outputs/"

    @task
    def repopulate_user_device_master(ds: str = None, data_interval_start: datetime = None, task=None):
        """Repopulates user_device_master table for current date with current values."""
        from de_utils.tndbo import get_dbo
        dbo = get_dbo(SF_CONN_ID)
        task.log.info(f"Deleting previously inserted values for date '{ds}' in table 'dau.user_device_master'")
        dbo.execute(f"DELETE FROM dau.user_device_master WHERE (date_utc = '{ds}'::DATE)")
        task.log.info(f"Populating values for date '{ds}' in table 'dau.user_device_master'")
        next_ds = str(data_interval_start.date() + timedelta(days=1))  # airflow next_ds does not work on triggered runs
        dbo.execute_in_parallel(get_user_device_master_sqls(ds, next_ds), warehouse=SF_WH_MEDIUM)

    @task
    def unload_single_dau_to_s3(s3_input_key: str, ds: str = None, task=None):
        """Executes all the queries to populate single_dau tables.
        The final query in get_dau_sqls is the COPY statement which unloads final single_dau data to S3.
        This unload will replace the previous run's file if it exists.
        Note: query_groupings are sent to Snowflake in parallel.
        """
        from de_utils.tndbo import get_dbo
        dbo = get_dbo(SF_CONN_ID)
        for query_grouping in get_dau_sqls(ds, s3_input_key):
            task.log.info(f"Running group of queries to repopulate single dau table for date '{ds}'")
            dbo.execute_in_parallel(query_grouping, warehouse=SF_WH_MEDIUM)

    @task
    def process_dads(s3_input_key, s3_output_dir, ds: str = None, task=None):
        """Process Daily Active Sets
        Collect input file and fill in `user_dict`, `device_dict` and `graph`
        user_dict` is the mapping between usernames and set_uuids
        device_dict` is the mapping between adids and set_uuids
        graph` simply depicts all user-device relationships on the query run date
        """
        user_dict, device_dict = {}, {}
        task.log.info(f"Retrieving graph for file: s3://{SF_BUCKET}/{s3_input_key}")
        graph = get_graph(user_dict, device_dict, s3_input_key)
        new_user_df, new_device_df, new_set_df = get_networkx_dads(graph, user_dict, device_dict, ds)
        task.log.info("Writing DADS results to S3.")
        for df, fname in zip([new_user_df, new_device_df, new_set_df], output_filenames):
            df.to_csv(f"s3://{SF_BUCKET}/{s3_output_dir}{fname}", sep="\t", index=False, header=False)

    # The DAG is written so that it can handle only one day of data.
    # Therefore, any new set_uuid that we haven't seen before must have cohort_utc == date_utc.
    # In other occasions (e.g. back-filling) the proper logic is to find min(date_utc) as cohort_utc
    #     for each set_uuid/client_type
    # Notes: Keep DELETE together with INSERT in same task. Don't need to defer as these queries are fast enough
    insert_set_cohort = SQLExecuteQueryOperator(
        task_id="run_set_cohort_queries",
        conn_id=SF_CONN_ID,
        sql=["DELETE FROM dau.set_cohort WHERE (cohort_utc >= '{{ ds }}'::DATE)",
             """INSERT INTO dau.set_cohort
                SELECT s.set_uuid, s.client_type, s.date_utc AS cohort_utc, s.created_at, s.adid, s.username
                FROM (
                    SELECT m.date_utc, m.client_type, ds.set_uuid, m.adid, m.username, m.created_at
                    FROM dau.device_set ds
                    JOIN dau.user_device_master m ON (ds.adid = m.adid)
                    WHERE (date_utc = '{{ ds }}'::DATE)
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ds.set_uuid, m.client_type ORDER BY m.created_at) = 1
                ) AS s
                LEFT JOIN dau.set_cohort c ON (s.set_uuid = c.set_uuid) AND (s.client_type = c.client_type)
                WHERE (c.set_uuid IS NULL)
        """],
        hook_params={"warehouse": SF_WH_MEDIUM}
    )

    run_dau_dq_checks = DQChecksOperator(
        task_id="run_dbt_dau_source_tables_dq_checks",
        dq_checks_instance=DAUSourceTablesDQChecks(),
        run_date=DAG_RUN_DATE_1_DAY_LAG
    )

    load_file_tasks = []
    for filename in output_filenames:
        load_file_tasks.append(S3ToSnowflakeDeferrableOperator(
            task_id=f"load_{filename}",
            table=filename,
            schema="dau",
            s3_loc=f"{s3_output_dir}{filename}",
            file_format=r"(TYPE = CSV FIELD_DELIMITER = '\t' DATE_FORMAT = AUTO)",
            load_type=S3ToSnowflakeDeferrableOperator.LoadType.KEEP_EXISTING,
            deferrable=False  # need this to run faster. Helps not to defer. There isn't much other running at this time
        ))

    (repopulate_user_device_master() >> unload_single_dau_to_s3(s3_input_key) >>
     process_dads(s3_input_key, s3_output_dir) >> load_file_tasks >> insert_set_cohort >> run_dau_dq_checks)


def get_user_device_master_sqls(ds: str, next_ds: str):
    """Returns sql queries which populates dau user_device_master table with current values.

    Android DAU inflated since early Nov 2019 as a result of an increase in sessions less than 1 second
    according to Adjust. Theory is that something is triggering Adjust/Leanplum tracking when the user
    actually isn't running the application. Since Adjust is starting tracking in the onResume lifecycle call,
    it should be triggered from UI.  We've seen in the past that Ogury has initialized zero pixel UI to conduct
    their user tracking.
    The timelines of Ogury release seem to have corresponded with the increase in sessions < 1sec.
    The update to Ogury  to fix the wakelock issue seems to also correspond with a decrease (but not complete
    elimination of) those sessions.  We suspect there either remains further issues with Ogury, or other third
    party SDKs are taking similar approaches.
    Here we work around the issue by taking the intersection of our firehose tracking (core.sessions)
    when client_type = TN_ANDROID and version >= 6.45 and date >= '2019-10-27'.
    """
    return [
        f"""
        INSERT INTO dau.user_device_master
        WITH firehose_android AS (
            SELECT DISTINCT username
            FROM prod.core.sessions
            WHERE 
                (created_at >= '2019-10-27')
                AND (created_at >= '{ds}'::TIMESTAMP_NTZ)
                AND (created_at < '{next_ds}'::TIMESTAMP_NTZ)
                AND (client_type = 'TN_ANDROID')
                AND (client_version >= '6.45')
        )
        SELECT
            s.created_at::DATE AS date_utc,
            TRIM(s.username) AS username,
            s.adid,
            a.client_type,
            'adjust.sessions' AS source,
            MIN(s.created_at) AS created_at
        FROM prod.adjust.sessions_with_pi s
        JOIN prod.adjust.apps a ON (s.app_name = a.app_name)
        WHERE 
            (s.created_at >= '{ds}'::TIMESTAMP_NTZ)
            AND (s.created_at < '{next_ds}'::TIMESTAMP_NTZ)
            AND (TRIM(NVL(s.username, '')) <> '')
            AND (IFF((a.client_type = 'TN_ANDROID') AND (s.app_version >= '6.45'),
                  s.username IN (SELECT username FROM firehose_android), TRUE))
        GROUP BY 1, 2, 3, 4, 5
        """,
        f"""
        INSERT INTO dau.user_device_master
        SELECT
            r.created_at::DATE AS date_utc,
            TRIM(r.username) AS username,
            r.adid,
            a.client_type,
            'adjust.registrations' AS source,
            MIN(r.created_at) AS created_at
        FROM prod.adjust.registrations r
        JOIN prod.adjust.apps a ON (r.app_name = a.app_name)
        WHERE 
            (r.created_at >= '{ds}'::TIMESTAMP_NTZ)
            AND (r.created_at < '{next_ds}'::TIMESTAMP_NTZ)
            AND (TRIM(NVL(r.username, '')) <> '')
        GROUP BY 1, 2, 3, 4, 5
        """,
        f"""
        INSERT INTO dau.user_device_master
        SELECT
            l.created_at::DATE AS date_utc,
            TRIM(l.username) AS username,
            l.adid,
            a.client_type,
            'adjust.logins' AS source,
            MIN(l.created_at) AS created_at
        FROM prod.adjust.logins_with_pi l
        JOIN prod.adjust.apps a ON (l.app_name = a.app_name)
        WHERE
            (l.created_at >= '{ds}'::TIMESTAMP_NTZ)
            AND (l.created_at < '{next_ds}'::TIMESTAMP_NTZ)
            AND (TRIM(NVL(l.username, '')) <> '')
        GROUP BY 1, 2, 3, 4, 5
        """,
        f"""
        INSERT INTO dau.user_device_master
        WITH adjust_device_ios AS (
            SELECT DISTINCT i.installed_at, i.adid, a.client_type, i.idfv
            FROM prod.adjust.installs_with_pi i
            JOIN prod.adjust.apps a ON (i.app_name = a.app_name)
            WHERE 
              (idfv <> '')
              AND (idfv <> '00000000-0000-0000-0000-000000000000')
              AND (idfv <> '0000-0000')
        ),
        pp_data AS (
            SELECT DISTINCT
                created_at::DATE AS date_utc,
                TRIM("client_details.client_data.user_data.username") AS username,
                "client_details.ios_bonus_data.idfv" AS idfv,
                'TN_IOS_FREE' AS client_type
            FROM prod.party_planner_realtime.applifecyclechanged
            WHERE 
                (created_date = '{ds}'::DATE)
                AND ("client_details.client_data.client_platform" = 'IOS')
                AND (TRIM(NVL("client_details.client_data.user_data.username", '')) <> '')
                AND (TRIM(NVL("client_details.client_data.user_data.username", '')) NOT LIKE '%@%')
                AND (TRIM(NVL("client_details.client_data.user_data.username", '')) NOT LIKE '%-%')
                AND ("client_details.ios_bonus_data.idfv" <> '00000000-0000-0000-0000-000000000000')
                AND ("client_details.ios_bonus_data.idfv" <> '0000-0000')
                AND ("client_details.ios_bonus_data.idfv" <> '')
        ),
        session_data AS (
            SELECT
                created_at::DATE AS date_utc,
                TRIM(username) AS username,
                client_type,
                MIN(created_at) AS created_at
            FROM prod.core.sessions
            WHERE 
                (created_at >= '{ds}'::TIMESTAMP_NTZ)
                AND (created_at < '{next_ds}'::TIMESTAMP_NTZ)
                AND (TRIM(NVL(username, '')) <> '')
                AND (client_type IN ('TN_IOS_FREE', 'TN_IOS'))
            GROUP BY 1, 2, 3
        )
        SELECT
            s.date_utc,
            s.username,
            dd.adid,
            s.client_type,
            'core.sessions (ios)' AS source,
            MIN(s.created_at) AS created_at
        FROM session_data s
        JOIN pp_data pp ON 
            (s.username = pp.username) 
            AND (s.date_utc = pp.date_utc) 
            AND (s.client_type = pp.client_type)
        JOIN adjust_device_ios dd ON
            (dd.idfv = pp.idfv)
            AND (dd.installed_at < s.date_utc + INTERVAL '1 DAY')
            AND (s.client_type = dd.client_type)
        GROUP BY 1, 2, 3, 4, 5
        """,
        f"""
        INSERT INTO dau.user_device_master
        WITH adjust_device_android AS (
            SELECT DISTINCT i.installed_at, i.adid, a.client_type, i.gps_adid
            FROM prod.adjust.installs_with_pi i
            JOIN prod.adjust.apps a ON (i.app_name = a.app_name)
            WHERE 
                (i.gps_adid <> '')
                AND (i.gps_adid <> '00000000-0000-0000-0000-000000000000')
                AND (i.gps_adid <> '0000-0000')
        ),
        pp_data AS (
            SELECT DISTINCT
                created_at::DATE AS date_utc,
                TRIM("client_details.client_data.user_data.username") AS username,
                "client_details.android_bonus_data.google_play_services_advertising_id" AS gps_adid,
                CASE WHEN "client_details.client_data.brand" = 'BRAND_2NDLINE' THEN '2L_ANDROID'
                     WHEN "client_details.client_data.brand" = 'BRAND_TEXTNOW' THEN 'TN_ANDROID'
                     ELSE 'UNKNOWN' --UNKNOWN never shows up in 2023
                END AS client_type
            FROM prod.party_planner_realtime.applifecyclechanged
            WHERE 
                (created_date = '{ds}'::DATE)
                AND ("client_details.client_data.client_platform" = 'CP_ANDROID')
                AND (TRIM(NVL("client_details.client_data.user_data.username", '')) <> '')
                AND (TRIM(NVL("client_details.client_data.user_data.username", '')) NOT LIKE '%@%')
                AND (TRIM(NVL("client_details.client_data.user_data.username", '')) NOT LIKE '%-%')
                AND ("client_details.android_bonus_data.google_play_services_advertising_id" <> 
                    '00000000-0000-0000-0000-000000000000')
                AND ("client_details.android_bonus_data.google_play_services_advertising_id" <> '0000-0000')
                AND ("client_details.android_bonus_data.google_play_services_advertising_id" <> '')
        ),
        session_data AS (
            SELECT
                created_at::DATE AS date_utc,
                TRIM(username) AS username,
                client_type,
                MIN(created_at) AS created_at
            FROM prod.core.sessions
            WHERE 
                (created_at >= '{ds}'::TIMESTAMP_NTZ)
                AND (created_at < '{next_ds}'::TIMESTAMP_NTZ)
                AND (TRIM(NVL(username, '')) <> '')
                AND (client_type IN ('TN_ANDROID', '2L_ANDROID'))
            GROUP BY 1, 2, 3
        )
        SELECT
            s.date_utc,
            s.username,
            dd.ADID,
            s.client_type,
            'core.sessions (android)' AS source,
            MIN(s.created_at) AS created_at
        FROM session_data s
        JOIN pp_data pp ON
            (s.username = pp.username)
            AND (s.date_utc = pp.date_utc)
            AND (s.client_type = pp.client_type)
        JOIN adjust_device_android dd ON
            (dd.gps_adid = pp.gps_adid)
            AND (dd.installed_at < s.date_utc + INTERVAL '1 DAY')
            AND (s.client_type = dd.client_type)
        GROUP BY 1, 2, 3, 4, 5
        """
    ]


def get_dau_sqls(ds: str, s3_input_key: str):
    """Processing 1:1 user <-> device pairs entirely in rs without running them through networkx.
    This is an optimization which cuts down the data going through by approximately 4x.

    These queries are highly tuned to work well with the table diststyle.
    Hashing huge tables like user_set / device_set is expensive so the query will run a lot faster
    if user_device_master is hashed and effectively sf is doing `user_set right join user_device_master`

    We first construct the set that has 1:1 mapping between username and adid (dau_temp_single)
    There are 3 cases in dau_temp_single:
      1. user_set and device_set are both null -> generate uuid, update user_set/device_set/daily_active_users
      2. one of them is null                   -> insert into daily_active_users
      3. both of them exist                    -> insert into daily_active_users

    Rows outside of dau_temp_single contains n:n mappings and are unloaded into S3 for next stage of processing.
    """
    return [
        [
            f"DELETE FROM dau.daily_active_users WHERE (date_utc >= '{ds}'::DATE)",
            f"DELETE FROM dau.device_set WHERE (processed_at >= '{ds}'::DATE)",
            f"DELETE FROM dau.user_set WHERE (processed_at >= '{ds}'::DATE)"
        ],
        [
            f"""
            CREATE TEMP TABLE dau_temp AS
            SELECT DISTINCT
                m.username,
                m.adid,
                u.set_uuid AS user_set,
                d.set_uuid AS device_set
            FROM dau.user_device_master m
            LEFT JOIN dau.user_set u ON (m.username = u.username)
            LEFT JOIN dau.device_set d ON (m.adid = d.adid)
            WHERE 
                (date_utc = '{ds}'::DATE)
                AND (m.username <> '')
                AND (m.adid <> '')
            """
        ],
        [
            """
            CREATE TEMP TABLE single_user AS
            WITH single_user_device AS (
                SELECT adid
                FROM dau_temp
                GROUP BY adid
                HAVING (COUNT(DISTINCT username) = 1)
            )
            SELECT dt.username
            FROM dau_temp dt
            LEFT JOIN single_user_device sud USING (adid)
            GROUP BY 1
            HAVING (COUNT(DISTINCT dt.adid) = 1) AND (COUNT(sud.adid) = 1)
            """
        ],
        [
            """
            CREATE TEMP TABLE dau_temp_single AS
            SELECT 
                dau_temp.username,
                dau_temp.adid,
                dau_temp.user_set,
                dau_temp.device_set
            FROM dau_temp
            JOIN single_user USING (username)
            """
        ],
        [
            # both user_set and device_set is null
            """
            CREATE TEMP TABLE new_sets AS
            SELECT
                username,
                adid,
                UUID_STRING('6ba7b812-9dad-11d1-80b4-00c04fd430c8', username) AS set_uuid
            FROM dau_temp_single
            WHERE (user_set IS NULL) AND (device_set IS NULL)
            """
        ],
        [
            # insert new sets (both user_set and device_set IS NULL) and records that have device_set, but no user_set
            f"""
            INSERT INTO dau.user_set
            SELECT DISTINCT username, set_uuid, '{ds}' AS processed_at FROM new_sets
            UNION ALL SELECT DISTINCT username,  device_set, '{ds}' AS processed_at
            FROM dau_temp_single
            WHERE (device_set IS NOT NULL) AND (user_set IS NULL)
            """,
            # insert new sets (both user_set and device_set IS NULL) and records that have user_set, but no device_set
            f"""
            INSERT INTO dau.device_set
            SELECT DISTINCT adid, set_uuid, '{ds}' AS processed_at FROM new_sets
            UNION ALL SELECT DISTINCT adid, user_set, '{ds}' AS processed_at
            FROM dau_temp_single
            WHERE (user_set IS NOT NULL) AND (device_set IS NULL)
            """,
            # insert every combination
            f"""
            INSERT INTO dau.daily_active_users
            SELECT set_uuid, '{ds}' AS date_utc, 1.0 as dau FROM new_sets
            UNION ALL SELECT DISTINCT user_set AS set_uuid, '{ds}' AS date_utc, 1.0 AS dau
            FROM dau_temp_single
            WHERE (user_set IS NOT NULL) AND (device_set IS NULL)
            UNION ALL SELECT DISTINCT device_set AS set_uuid, '{ds}' AS date_utc, 1.0 as dau
            FROM dau_temp_single
            WHERE (device_set IS NOT NULL) AND (user_set IS NULL)
            UNION ALL SELECT DISTINCT user_set AS set_uuid, '{ds}' AS date_utc, 1.0 AS dau
            FROM dau_temp_single
            WHERE (user_set = device_set)
            UNION ALL SELECT DISTINCT user_set AS set_uuid, '{ds}' AS date_utc, 0.5 AS dau
            FROM dau_temp_single
            WHERE (user_set <> device_set)
            UNION ALL SELECT DISTINCT device_set AS set_uuid, '{ds}' AS date_utc, 0.5 AS dau
            FROM dau_temp_single
            WHERE (user_set <> device_set)
            """
        ],
        [
            f"""
            COPY INTO @{STAGE_SF}/{s3_input_key}
            FROM (
                SELECT dau_temp.username, dau_temp.adid, dau_temp.user_set, dau_temp.device_set
                FROM dau_temp
                LEFT JOIN single_user ON (dau_temp.username = single_user.username)
                WHERE single_user.username IS NULL
            )
            FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\t' NULL_IF = () COMPRESSION = NONE)
            OVERWRITE = TRUE SINGLE = TRUE MAX_FILE_SIZE = 5000000000
            """
        ]
    ]


def get_graph(user_dict: dict, device_dict: dict, s3_input_key: str):
    from de_utils.aws import S3Path
    import csv
    import networkx as nx

    graph = nx.Graph()
    contents = S3Path(f"s3://{SF_BUCKET}/{s3_input_key}").read()
    contents = contents[:-1] if contents.endswith("\n") else contents  # remove final newline so no empty row at end.
    data = csv.reader(contents.split("\n"), delimiter="\t")
    for username, adid, user_set, adid_set in data:
        graph.add_edge(username, adid)
        user_dict[username] = user_set or None
        device_dict[adid] = adid_set or None
    return graph


def get_networkx_dads(graph, user_dict, device_dict, date) -> tuple:
    """Assign a set UUID to all the connected components of `graph`, return an updated `user_dict` and `device_dict`

    `user_dict` is the mapping between usernames and set_uuids
    `device_dict` is the mapping between adids and set_uuids
    """
    import networkx as nx
    import uuid
    from pandas import DataFrame

    user_temp, device_temp, set_temp = [], [], []

    for component in nx.connected_components(graph):
        subgraph = graph.subgraph(component)
        output_sets = set()
        # Discover pre-assigned set_uuids within connected component
        for node in component:
            output_sets.add(user_dict.get(node))
            output_sets.add(device_dict.get(node))
        output_sets.discard(None)

        if not output_sets:  # if no pre-existing uuids is present

            # uuid is generated from the smallest username in the set
            first_username = min(node for node in component if node in user_dict)
            # if the following line is updated, make sure you also update rs UDF `f_userset_uuid()`
            new_set_uuid = str(uuid.uuid5(uuid.NAMESPACE_OID, first_username))

            set_temp.append([new_set_uuid, date, 1.0])
            for node in component:
                if node in user_dict:
                    user_temp.append([node, new_set_uuid, date])
                else:
                    device_temp.append([node, new_set_uuid, date])
        else:
            # Otherwise, assign closest (based on BFS) set_uuid to a given node
            # if the node does not have one already
            weight = 1.0 / len(output_sets)
            for set_id in output_sets:
                set_temp.append([set_id, date, weight])
            for node in component:
                if user_dict.get(node) is None and device_dict.get(node) is None:
                    for _, v in nx.bfs_edges(subgraph, node):
                        set_assign = user_dict.get(v) or device_dict.get(v)
                        if set_assign is not None:
                            if node in user_dict:
                                user_temp.append([node, set_assign, date])
                            else:
                                device_temp.append([node, set_assign, date])
                            break

    new_user_df = DataFrame(user_temp)
    new_device_df = DataFrame(device_temp)
    new_set_df = DataFrame(set_temp)
    return new_user_df, new_device_df, new_set_df


dau_processing_sf()

if __name__ == "__main__":
    # dau_processing_sf().test(execution_date="2023-08-13")
    dau_processing_sf().test_dq_checks(execution_date="2023-08-10")
