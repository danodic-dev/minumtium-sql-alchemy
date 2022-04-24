from __future__ import annotations
import threading
from typing import Dict, List

from minumtium.infra.database import DatabaseAdapter, DataFetchException, DataNotFoundException, DataInsertException
from pydantic import BaseModel
from sqlalchemy import Table, MetaData, func, create_engine, cast, String
from sqlalchemy.engine import Engine

from minumtium_sqlite.migrations import apply_migrations


class MinumtiumSQLiteAdapterConfig(BaseModel):
    schema_name: str = None


class MinumtiumSQLiteAdapter(DatabaseAdapter):

    def __init__(self, config: MinumtiumSQLiteAdapterConfig, table_name: str, engine: Engine = None):
        self.engine = self.initialize(config, engine)
        self.metadata_obj = MetaData(bind=self.engine)
        self.table_name = table_name
        self.table = Table(table_name, self.metadata_obj, autoload=True)

        self.cast_columns = self._setup_cast_columns(self.table)
        self.summary_columns_value = None

    def initialize(self, config: MinumtiumSQLiteAdapterConfig, engine: Engine = None):
        engine = engine or self._create_engine()
        self._migrate(engine, schema=config.schema_name)
        return engine

    @staticmethod
    def _migrate(engine: Engine, schema: str):
        apply_migrations(engine, schema)

    @staticmethod
    def _create_engine():
        return create_engine("sqlite://")

    @staticmethod
    def _setup_cast_columns(table):
        columns = []
        for name, column in table.c.items():
            if name in ['id', 'timestamp']:
                columns.append(cast(column, String()))
                continue
            columns.append(column)
        return columns

    @staticmethod
    def _setup_summary_columns(table):
        return [cast(table.c.id, String()),
                table.c.title,
                table.c.author,
                cast(table.c.timestamp, String())]

    @staticmethod
    def _cast_results(query_results):
        return [dict(result) for result in query_results]

    @property
    def summary_columns(self):
        if not self.summary_columns_value:
            self.summary_columns_value = self._setup_summary_columns()
        return self.summary_columns_value

    def find_by_id(self, id: str):
        statement = (self.table
                     .select()
                     .where(self.table.c.id == int(id))
                     .with_only_columns(self.cast_columns))

        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    result = connection.execute(statement).mappings().first()
                except Exception as e:
                    raise DataFetchException(f'Error running query: {str(e)}') from e

        if not result:
            raise DataNotFoundException(f'No data found at {self.table_name} for id: {id}')

        return dict(result)

    def find_by_criteria(self, criteria: Dict):
        def create_query(query_criteria: Dict):
            query = self.table.select()
            for column, value in query_criteria.items():
                column = getattr(self.table.c, column)
                query = query.where(column == value)
            query = query.with_only_columns(self.cast_columns)
            return query

        statement = create_query(criteria)

        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    results = connection.execute(statement).mappings().all()
                except Exception as e:
                    raise DataFetchException(f'Could not select by criteria: {str(criteria)}') from e

        if not results:
            raise DataNotFoundException(f'No data found for the following criteria: {str(criteria)}')

        return self._cast_results(results)

    def insert(self, data: Dict) -> str:
        statement = self.table.insert(data)

        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    result = connection.execute(statement)
                except Exception as e:
                    raise DataInsertException(f'An error has happened inserting into: {self.table_name}') from e

        return str(result.inserted_primary_key[0])

    def all(self, limit: int = None, skip: int = None, sort_by: str = None):
        statement = (self.table.select()
                     .limit(limit)
                     .offset(skip)
                     .with_only_columns(self.cast_columns))
        if sort_by:
            column = getattr(self.table.c, sort_by)
            statement = statement.order_by(column.desc())

        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    results = connection.execute(statement)
                except Exception as e:
                    raise DataFetchException(f'An error has happened selecting from: {self.table_name}') from e

        if results is None:
            raise DataNotFoundException(f'No data found at {self.table_name}.')

        return self._cast_results(results.mappings().all())

    def _project_summary_fields(self, projection):
        columns = []
        for field in projection:
            if field in ['id', 'timestamp']:
                columns.append(cast(getattr(self.table.c, field), String()))
                continue
            columns.append(getattr(self.table.c, field))
        return columns

    def summary(self, projection: List[str], limit: int = 10, sort_by: str = None):
        columns = self._project_summary_fields(projection)
        statement = (self.table.select()
                     .with_only_columns(columns)
                     .limit(limit))
        if sort_by:
            column = getattr(self.table.c, sort_by)
            statement = statement.order_by(column.desc())

        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    results = connection.execute(statement)
                except Exception as e:
                    raise DataFetchException(
                        f'An error has happened getting the summary from: {self.table_name}') from e

        if results is None:
            raise DataNotFoundException(f'No data found at {self.table_name}.')

        return self._cast_results(results.mappings().all())

    def delete(self, id: str) -> None:
        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    statement = self.table.delete().where(self.table.c.id == id)
                    connection.execute(statement)
                except Exception as e:
                    raise DataFetchException(f'An error has happened deleting the id: {id}') from e

    def count(self) -> int:
        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    count_column = func.count(self.table.c.id)
                    statement = self.table.select().with_only_columns(count_column)
                    return connection.execute(statement).scalar()
                except Exception as e:
                    raise DataFetchException(f'An error has happened getting the count from: {self.table_name}') from e

    def truncate(self):
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(f"DELETE FROM {self.table}")
