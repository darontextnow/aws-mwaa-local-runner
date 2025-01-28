"""CSV (pandas based) specific implementation of BaseInput abstract class.
This is not ready for production use. Running pandas on Airflow workers will only work if CSV is small enough
to fit undo one worker.
Only expand on this if we find we have a real use case for this.
Otherwise, may need a pyspark version of this instead to run on cluster.
"""
from de_utils.dq_checks_framework.inputs._base_input import BaseInput


class CSVInput(BaseInput):
    """Reads CSV data from any local or s3 path and converts the results to an InputObject to be used in DQ Checks.

    This a first, minimal attempt at being able to retrieve data from a csv source.
    It currently only supports a single, very basic, simple csv (comma only) file.
    More work and testing is required should we have a use case for more complex csv parsing.

    Args:
        path (str): The path to local or s3 location to retrieve the data from.
        See parent class for definition of additional args.
    """
    def __init__(
            self,
            path: str,
            *args,
            **kwargs
    ):
        self.path = path
        super().__init__(source="csv file(s)", code=path, *args, **kwargs)
        kwargs.pop("name")
        kwargs.pop("alias")
        self._csv_kwargs = kwargs

    def get_input(self, params: dict = None):
        import pandas as pd
        from de_utils.dq_checks_framework.inputs._input_object import InputObject
        data = pd.read_csv(self.path, **self._csv_kwargs)
        cols = data.columns.tolist()
        vals = data.values.tolist()
        return InputObject(cols=cols, vals=vals)
