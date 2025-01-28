class _DictRow:
    """Row object that allows col value retrieval by index and by column name.
    This class is meant only for use by InputObject class.
    """
    def __init__(self, row: list[any], cols: list[str]):
        new_row = {}
        for idx, val in enumerate(row):
            name = cols[idx]
            new_row[name] = val
        self._row = new_row

    def __getitem__(self, x):
        if isinstance(x, str):
            return self._row[x]
        else:
            return list(self._row.values())[x]

    def get(self, x):
        return self._row.get(x)

    def copy(self):
        return self._row.copy()

    def __str__(self):
        return str(self._row)


class InputObject(list):
    """Python object to hold input data retrieved by an input implementation.
    This object is used by the DQCheck class as input data for the data quality check classes.

    Args:
        cols (List[str]): The list of column names. Allows us to refer to each value in vals list by name
            in addition to index position in list.
        vals (List[List[Any]]): A list corresponding to rows of lists of values of any data type in the same order
            corresponding to the cols list.
    """
    def __init__(self, cols: list[str], vals: list[list[any]]):
        self._cols = cols
        self._vals = vals
        super().__init__(vals)

    @property
    def cols(self) -> list[str]:
        return self._cols

    @property
    def vals(self) -> list[list[any]]:
        return self._vals

    # Override
    def __getitem__(self, x: int):
        return _DictRow(row=self._vals[x], cols=self._cols)
