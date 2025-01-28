from airflow.operators.trigger_dagrun import TriggerDagRunOperator as _TriggerDagRunOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException


class TriggerDagRunOperator(_TriggerDagRunOperator):
    """Custom version of this operator."""

    def __init__(
        self,
        *,
        force_status_to_success: bool = False,
        **kwargs,
    ):
        self.force_status_to_success = force_status_to_success
        super().__init__(**kwargs)

    # override to implement force_status_to_success
    def execute(self, context: Context):
        try:
            super().execute(context)
        except AirflowException as e:
            if self.force_status_to_success and "failed with failed states" in str(e):
                # overriding here for convenience. If the downstream triggered DAG fails, it will be in a failed state.
                # There is no reason to have both this trigger AND the downstream DAG in failed state.
                self.log.info("force_status_to_success flag is True. Ignoring failure of downstream triggered task.")
                return
