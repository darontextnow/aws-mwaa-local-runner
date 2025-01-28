"""Module containing all enums that can be used by multiple DE/DS applications."""
from enum import Enum


class Env(Enum):
    PROD = "prod"           # for AWS prod account
    DEV = "dev"             # for AWS dev account
    GHA = "gha"             # for GitHub Actions runs
    TOOLING = "tooling"     # for Tooling AWS account


class Compression(Enum):
    GZ = ".gz"
    SNAPPY = ".snappy"
    NONE = None
