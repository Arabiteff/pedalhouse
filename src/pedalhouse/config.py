"""One place for every name and path, so labs, jobs and tests agree."""

from dataclasses import dataclass

DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "pedalhouse_dev"

LANDING_VOLUME = "landing"          # raw files land here (the "lake")
CHECKPOINT_VOLUME = "checkpoints"   # Auto Loader checkpoints for the notebook labs
PLAYGROUND_VOLUME = "playground"    # scratch space for lab exercises

CITIES = ("lisbon", "montreal", "seoul")


@dataclass(frozen=True)
class LakehousePaths:
    """Every path and table name for one catalog/schema pair."""

    catalog: str = DEFAULT_CATALOG
    schema: str = DEFAULT_SCHEMA

    @property
    def fq_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def volume(self, name: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{name}"

    @property
    def landing(self) -> str:
        return self.volume(LANDING_VOLUME)

    @property
    def checkpoints(self) -> str:
        return self.volume(CHECKPOINT_VOLUME)

    @property
    def playground(self) -> str:
        return self.volume(PLAYGROUND_VOLUME)


def setup_sql(paths: LakehousePaths) -> list[str]:
    """The DDL that creates the schema and volumes. Idempotent."""
    return [
        f"CREATE SCHEMA IF NOT EXISTS {paths.fq_schema} "
        "COMMENT 'Pedalhouse bike-share lakehouse'",
        f"CREATE VOLUME IF NOT EXISTS {paths.fq_schema}.{LANDING_VOLUME} "
        "COMMENT 'Raw files from the three cities (the lake part of the lakehouse)'",
        f"CREATE VOLUME IF NOT EXISTS {paths.fq_schema}.{CHECKPOINT_VOLUME} "
        "COMMENT 'Streaming checkpoints for the notebook labs'",
        f"CREATE VOLUME IF NOT EXISTS {paths.fq_schema}.{PLAYGROUND_VOLUME} "
        "COMMENT 'Scratch space for exercises'",
    ]
