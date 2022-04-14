from typing import List

import pytest
from sqlalchemy import Table, MetaData, Column, Integer, text, inspect

from minumtium_sql_alchemy.migrations import has_version_table, get_database_version, \
    MigrationVersion, run_migrations, apply_migrations, update_database_version, MIGRATION_TABLE_NAME


def test_has_version_table(database_with_version_table):
    assert has_version_table(database_with_version_table)


def test_has_version_table_no_table(database):
    assert not has_version_table(database)


def test_get_database_version(database_with_version_table):
    assert get_database_version(database_with_version_table) == 0


def test_run_migrations(mock_migration, mock_table_name, database):
    run_migrations([mock_migration], database)
    assert inspect(database).has_table(mock_table_name)


def test_apply_any_migrations(some_migrations, mock_table_name, another_mock_table_name, database):
    run_migrations(some_migrations, database)
    assert inspect(database).has_table(mock_table_name)
    assert inspect(database).has_table(another_mock_table_name)


def test_apply_some_migrations(some_migrations, mock_table_name, another_mock_table_name, database_with_version_table):
    apply_migrations(database_with_version_table, some_migrations)
    assert not inspect(database_with_version_table).has_table(mock_table_name)
    assert inspect(database_with_version_table).has_table(another_mock_table_name)
    assert get_database_version(database_with_version_table) == 1


def test_apply_some_migrations_no_new_migrations(mock_migration, mock_table_name, another_mock_table_name,
                                                 database_with_version_table):
    update_database_version(database_with_version_table, 5)
    apply_migrations(database_with_version_table, [mock_migration])
    assert not inspect(database_with_version_table).has_table(mock_table_name)
    assert get_database_version(database_with_version_table) == 5


def test_update_database_version(database_with_version_table):
    update_database_version(database_with_version_table, 5)
    assert get_database_version(database_with_version_table) == 5


@pytest.fixture()
def mock_table_name() -> str:
    return 'test_table'


@pytest.fixture()
def another_mock_table_name() -> str:
    return 'another_test_table'


@pytest.fixture()
def some_migrations(another_mock_table_name, mock_migration) -> List[MigrationVersion]:
    class AnotherMockMigration(MigrationVersion):
        def get_version(self) -> int:
            return 1

        def do(self, engine) -> None:
            meta = MetaData()

            table = Table(
                another_mock_table_name, meta,
                Column('just_a_column', Integer)
            )
            meta.create_all(engine)

        def undo(self, engine) -> None:
            pass

    return [mock_migration, AnotherMockMigration()]


@pytest.fixture()
def mock_migration(mock_table_name) -> MigrationVersion:
    class MockMigration(MigrationVersion):
        def get_version(self) -> int:
            return 0

        def do(self, engine) -> None:
            meta = MetaData()

            table = Table(
                mock_table_name, meta,
                Column('just_a_column', Integer)
            )
            meta.create_all(engine)

        def undo(self, engine) -> None:
            pass

    return MockMigration()


@pytest.fixture(scope='function')
def database_with_version_table(database):
    def create_version_table(database):
        meta = MetaData()
        Table(MIGRATION_TABLE_NAME, meta, Column('version', Integer))
        meta.create_all(database)

    def insert_version(database, version):
        with database.connect() as connection:
            with connection.begin():
                statement = text(f"INSERT INTO {MIGRATION_TABLE_NAME} VALUES(:version)")
                connection.execute(statement, {'version': version})

    create_version_table(database)
    insert_version(database, 0)
    return database
