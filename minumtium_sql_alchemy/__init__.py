from __future__ import annotations
import threading
from typing import Dict, List

from minumtium.infra.database import DatabaseAdapter, DataFetchException, DataNotFoundException, DataInsertException
from sqlalchemy import Table, MetaData, func, create_engine

from minumtium_sql_alchemy.migrations import apply_migrations


class SqlAlchemyAdapter(DatabaseAdapter):
    metadata_obj = None
    migrated: bool = False

    def __init__(self, config: Dict, table_name: str, engine=None):
        self.engine = engine
        super().__init__(config, table_name)

        self._setup_metadata(self.engine)
        self.table_name = table_name
        self.table = Table(table_name, SqlAlchemyAdapter.metadata_obj, autoload=True)

    def initialize(self, config: Dict, collection_name: str):
        engine = self.engine or EngineFactory.get_engine(config['engine'], config)
        self._migrate()
        return engine

    def _setup_metadata(self, engine):
        if SqlAlchemyAdapter.metadata_obj is None:
            SqlAlchemyAdapter.metadata_obj = MetaData(bind=engine)

    def _migrate(self):
        if not SqlAlchemyAdapter.migrated:
            apply_migrations(self.engine)

    @staticmethod
    def _cast_id(value):
        value['id'] = str(value['id'])
        return value

    def find_by_id(self, id: str):
        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    statement = self.table.select().where(self.table.c.id == int(id))
                    result = connection.execute(statement).mappings().first()
                except Exception as e:
                    raise DataFetchException() from e

        if result is None:
            raise DataNotFoundException(f'No data found at {self.table_name} for id: {id}')

        return SqlAlchemyAdapter._cast_id(dict(result))

    def find_by_criteria(self, criteria: Dict):
        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    statement = self.table.select()
                    for column, value in criteria.items():
                        statement = statement.where(getattr(self.table.c, column) == value)
                    result = connection.execute(statement).mappings().all()
                except Exception as e:
                    raise DataFetchException() from e

                if not result:
                    raise DataNotFoundException(f'No data found for the following criteria: {str(criteria)}')

        return [self._cast_id(dict(value)) for value in result]

    def insert(self, data: Dict) -> str:
        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    statement = self.table.insert(data)
                    result = connection.execute(statement)
                    return str(result.lastrowid)
                except Exception as e:
                    raise DataInsertException(f'An error has happened inserting into: {self.table_name}') from e

    def all(self, limit: int = None, skip: int = None):
        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    statement = self.table.select().order_by(self.table.c.timestamp.desc()).limit(limit).offset(skip)
                    result = connection.execute(statement)
                except Exception as e:
                    raise DataFetchException(f'An error has happened selecting from: {self.table_name}') from e

        if result is None:
            raise DataNotFoundException(f'No data found at {self.table_name}.')

        return [self._cast_id(dict(value)) for value in result.mappings().all()]

    def summary(self, projection: List[str], limit: int = 10):
        with self.engine.connect() as connection:
            with connection.begin():
                try:
                    columns = [getattr(self.table.c, field) for field in projection]
                    order_by_column = self.table.c.timestamp.desc()
                    statement = self.table.select().with_only_columns(columns).limit(limit).order_by(order_by_column)
                    result = connection.execute(statement)
                except Exception as e:
                    raise DataFetchException(
                        f'An error has happened getting the summary from: {self.table_name}') from e

        if result is None:
            raise DataNotFoundException(f'No data found at {self.table_name}.')

        return [self._cast_id(dict(value)) for value in result.mappings().all()]

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


class EngineFactory:
    engines = {}
    lock = threading.Lock()

    @staticmethod
    def get_engine(engine_type: str, config=None):
        engine = engine_type.strip().upper()
        if engine not in EngineFactory.engines:
            try:
                EngineFactory.engines[engine] = {
                    'SQLITE_MEMORY': EngineFactory.create_sqlite_memory,
                    'POSTGRES': EngineFactory.create_postgres
                }[engine]()
            except KeyError as e:
                raise InvalidRelationalDatabaseType(f'Invalid engine type: {engine_type}') from e
        return EngineFactory.engines[engine]

    @staticmethod
    def create_sqlite_memory():
        return create_engine("sqlite://")

    @staticmethod
    def create_postgres(config: Dict):
        try:
            import pg8000
        except ImportError:
            raise InvalidRelationalDatabaseType(
                'Install the package pg8000 in order to use the POSTGRES driver.')

        username = config['username']
        password = config['password']
        host = config['host']
        port = config['port']
        dbname = config['dbname']
        return create_engine(
            f"postgresql+pg8000://{username}:{password}@{host}:{port}/{dbname}")


class InvalidRelationalDatabaseType(Exception):
    ...
