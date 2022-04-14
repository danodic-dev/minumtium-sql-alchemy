from __future__ import annotations

from abc import ABC, abstractmethod


class MigrationVersion(ABC):
    """
    Abstracts a database migration.
    """

    @abstractmethod
    def get_version(self) -> int:
        ...

    @abstractmethod
    def do(self, engine) -> None:
        ...

    @abstractmethod
    def undo(self, engine) -> None:
        ...

    def __lt__(self, other: MigrationVersion):
        return self.get_version() < other.get_version()
