"""Base class for all implementations of Input.
A goal of this abstract class is to ensure all metadata required by results object is included with each implementation.
This class can also supply shared methods as needed by various implementations.
"""
from abc import ABC, abstractmethod


class BaseInput(ABC):
    """Base abstract class for retrieving and converting input data into a standard format used by the DQCheck class.

    Args:
        name (string): The distinct name of this input. Example: "latest impressions"
        alias (string): The alias to use for referencing this input in the Checks class.
            aliases will be used as an instance attribute name for retrieving the data from the DQChecks class.
            Thus, alias must be named with only letters, numbers, and underscores and cannot start with a number.
        source (string): The source where data will be retrieved from. Included in report tables.
        code (str): The SQL statement or code used to retrieve the data used in DQ Checks.
    """
    def __init__(self, name: str, alias: str, source: str, code: str = None):
        self._name = name
        self._alias = alias
        self._source = source
        self._code = code

        self._validate_alias()

    @property
    def name(self) -> str:
        return self._name

    @property
    def alias(self) -> str:
        return self._alias

    @alias.setter
    def alias(self, alias: str):
        """Allows BaseDQChecks class to set this attr after processing inputs"""
        self._alias = alias

    @property
    def source(self) -> str:
        return self._source

    @property
    def code(self):
        return self._code

    """Method implementation should return an InputObject that can be used in the DQCheck class."""
    @abstractmethod
    def get_input(self, params: dict):
        """params (dict of runtime parameters) will be passed by BaseDQChecks class for those inputs that can
        use parameters.
        """
        pass

    def _validate_alias(self):
        """Validate that the given alias name will be able to be used by setattr method later after input has been
        processed. Saves time and allows for clear messaging.
        """
        import re
        if self.alias.startswith(("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")) \
                or not re.match("^[A-Za-z0-9_]*$", self.alias):
            msg = (f"The alias '{self.alias}' does not conform to rules for alias naming. It can only contain letters,"
                   f" numbers, and underscores and cannot start with a number.")
            raise ValueError(msg)
