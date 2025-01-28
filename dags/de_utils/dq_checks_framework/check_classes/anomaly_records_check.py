from de_utils.dq_checks_framework.check_classes.check import Check
from de_utils.dq_checks_framework.enums import Status


class AnomalyRecordsCheck(Check):
    """
    This is a special check class for:
        Sending Anomaly alerts to given Slack channel.
        Outputting anomaly metadata to special log reporting table core.dq_checks_anomaly_records.

    This check will not fail like other checks and raise red/yellow alerts.
    It's special purpose is for alerting upstream owners of data of potential fraud or bad data.

    Args:
        slack_channel (string): #channel to send alert to if anomaly records are found.
        name (string): A unique, descriptive name for the anomaly records.
        column_name (string): The name of the column or column expr where anomaly value is sourced from.
        where_clause_expr (string): The WHERE clause that filters out only the anomaly records from the table.
        pkey_columns (List[string]): A list of columns that make up the unique/primary key for the table.
        anomaly_input_object (BaseInput): The input object that contains the anomaly records.
        table_name (string): The name of the table the anomaly records were sourced from.
            Default is to use value from BaseDQChecks implementation table_name arg.
    """
    def __init__(
            self,
            slack_channel: str,
            name: str,
            column_name: str,
            where_clause_expr: str,
            pkey_columns: list[str],
            anomaly_input_object,
            table_name: str = None
    ):
        # set yellow expr and warning to appease validation checks.
        super().__init__(name=name, description="", column_name=column_name, value=None, todo=[],
                         yellow_expr="None", yellow_warning="None")

        self.channel = slack_channel
        self.table_name = table_name
        self.where_clause_expr = where_clause_expr
        self.pkey_columns = pkey_columns
        self.anomaly_input_object = anomaly_input_object
        self.write_details = False  # Don't output details to reporting details table
        self.write_anomaly_records = True  # Output details to reporting anomalies records table
        self.status = Status.GREEN  # Not using status with this check. Set to GREEN to appease BaseDQChecks class.

    # Overrides
    def run_check(self):
        self.send_alert()

    # Overrides
    # can't find a # noinspection for PyCharm for this one.
    def send_alert(self):
        from de_utils.slack.alerts import alert_anomalies_detected
        if self.anomaly_input_object.vals == [[]]:
            return  # no anomaly records to alert about

        contents = [",".join(self.anomaly_input_object.cols)]
        for row in self.anomaly_input_object.vals:
            contents.append(",".join([str(col) for col in row]))
        contents = "\n".join(contents)

        print(f"Anomalies Alert sent for Anomaly Check: {self.name} with csv content of:")
        print(contents)
        alert_anomalies_detected(
            name=self.name,
            table_name=self.table_name,
            column_name=self.column_name,
            filter_expr=self.where_clause_expr,
            file_contents=contents,
            channel=self.channel,
            env=self._env
        )

    def get_output_rows(self) -> list[list[str]] | None:
        """Returns row like details to be outputted to anomalies reporting table.
        If no anomaly records were found, returns []
        """
        rows = []
        for row in self.anomaly_input_object:
            if not row:
                return []  # no anomaly records
            value = row[self.column_name]
            filter_exprs = [f"(CAST({col} AS STRING) = '{row[col]}')" for col in self.pkey_columns]
            rows.append([value, " AND ".join(filter_exprs)])
        return rows
