# Minumtium SQL Alchemy

A relational database adapter for the [minumtium](https://github.com/danodic-dev/minumtium) library.

### What can I use it for?

It is used to provide access to data using relational databases using
the [minumtium](https://github.com/danodic-dev/minumtium) library.

## Usage

Install it using your favorite package manager:

```commandline
pip install minumtium-sql-alchemy
```

```commandline
pipenv install minumtium-sql-alchemy
```

```commandline
poetry install minumtium-sql-alchemy
```

Then, provide it to your minumtium Service:

```python
from minumtium.modules.idm import IdmService, UserRepository
from minumtium_sql_alchemy_adapter import SqlAlchemyAdapter
from minumtium_simple_jwt_auth import SimpleJwtAuthentication

db_adapter = SqlAlchemyAdapter({'engine': 'sqlite_memory'}, 'posts')
auth_adapter = SimpleJwtAuthentication(configuration={
    'jwt_key': 'not a reliable key, change that quickly',
    'session_duration_hours': 1})

users_repository = UserRepository(db_adapter)
idm_service = IdmService(auth_adapter, users_repository)

idm_service.authenticate('jao', 'batata')
```