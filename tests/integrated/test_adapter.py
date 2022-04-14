import copy
from datetime import datetime
from typing import List, Dict

import pytest
from minumtium.infra.database import DataNotFoundException
from sqlalchemy import text, inspect

from minumtium_sql_alchemy import SqlAlchemyAdapter
from minumtium_sql_alchemy.migrations import MIGRATION_TABLE_NAME
from minumtium_sql_alchemy.migrations.versions.version_1 import Version1, USERS_TABLE_NAME, POSTS_TABLE_NAME


def test_adapter_initialize(adapter: SqlAlchemyAdapter):
    assert inspect(adapter.engine).has_table(MIGRATION_TABLE_NAME)
    assert inspect(adapter.engine).has_table(USERS_TABLE_NAME)
    assert inspect(adapter.engine).has_table(POSTS_TABLE_NAME)
    with adapter.engine.connect() as connection:
        with connection.begin():
            result = connection.execute(
                text(f'SELECT version FROM {MIGRATION_TABLE_NAME}')
            ).mappings().first()
            assert result['version'] == Version1().get_version()


def test_adapter_find_by_id(adapter_with_data: SqlAlchemyAdapter):
    with adapter_with_data.engine.connect() as connection:
        with connection.begin():
            result = connection.execute(
                text(f'SELECT * FROM {MIGRATION_TABLE_NAME}')
            ).mappings().first()
            assert result['version'] == Version1().get_version()


def test_find_by_id(adapter_with_data: SqlAlchemyAdapter):
    data = adapter_with_data.find_by_id('2')
    assert data == {'id': '2',
                    'title': 'This is the third post',
                    'author': 'danodic',
                    'timestamp': datetime(2022, 2, 22, 10, 22, 22, 222222),
                    'body': 'This is a sample post.'}


def test_find_by_id_invalid_id(adapter_with_data: SqlAlchemyAdapter):
    with pytest.raises(DataNotFoundException) as e:
        adapter_with_data.find_by_id('200')
        assert e.type is DataNotFoundException
        assert e.value.args[0] == f'No data found at posts for id: -1'


def test_find_by_criteria(adapter_with_data: SqlAlchemyAdapter):
    data = adapter_with_data.find_by_criteria({'body': 'This is a different criteria.'})
    assert data == [{'id': '8',
                     'title': 'This is the ninetieth post',
                     'author': 'danodic',
                     'timestamp': datetime(2022, 2, 22, 4, 22, 22, 222222),
                     'body': 'This is a different criteria.'},
                    {'id': '9',
                     'title': 'This is the tenth post',
                     'author': 'danodic',
                     'timestamp': datetime(2022, 2, 22, 3, 22, 22, 222222),
                     'body': 'This is a different criteria.'}]


def test_find_by_criteria_invalid_criteria(adapter_with_data: SqlAlchemyAdapter):
    criteria = {'body': 'This is an invalid criteria.'}
    with pytest.raises(DataNotFoundException) as e:
        adapter_with_data.find_by_criteria(criteria)
        assert e.type is DataNotFoundException
        assert e.value.args[0] == f'No data found for the following criteria: {str(criteria)}'


def test_insert(adapter_with_data: SqlAlchemyAdapter):
    data = {'title': 'This is the tenth post',
            'author': 'danodic',
            'timestamp': datetime(2022, 2, 22, 3, 22, 22, 222222),
            'body': 'This is an inserted entry.'}

    inserted_id = adapter_with_data.insert(data)

    with adapter_with_data.engine.connect() as connection:
        with connection.begin():
            result = dict(connection.execute(
                text(f'SELECT * FROM posts WHERE id = :id'), {'id': inserted_id}
            ).mappings().first())
            data['id'] = str(inserted_id)
            data['timestamp'] = str(data['timestamp'])
            result['id'] = str(result['id'])
            assert result == data


def test_all(adapter_with_data: SqlAlchemyAdapter, posts_data: List[Dict]):
    results = adapter_with_data.all()
    assert len(results) == 10

    data = [cast_id(copy.deepcopy(value)) for value in posts_data]
    assert results == data


def test_all_limit(adapter_with_data: SqlAlchemyAdapter, posts_data: List[Dict]):
    results = adapter_with_data.all(limit=2)
    assert len(results) == 2

    data = [cast_id(copy.deepcopy(value)) for value in posts_data[:2]]
    assert results == data


def test_all_skip(adapter_with_data: SqlAlchemyAdapter):
    results = adapter_with_data.all(skip=2)
    assert len(results) == 8
    assert results[0]['id'] == '2'
    assert results[7]['id'] == '9'


def test_all_skip_and_limit(adapter_with_data: SqlAlchemyAdapter, posts_data: List[Dict]):
    results = adapter_with_data.all(limit=2, skip=2)
    assert len(results) == 2

    data = [cast_id(copy.deepcopy(value)) for value in posts_data[2:4]]
    assert results == data


def test_summary(adapter_with_data: SqlAlchemyAdapter):
    posts = adapter_with_data.summary(projection=['id', 'title'])
    assert posts[0] == {'id': '0', 'title': 'This is the first post'}
    assert posts[1] == {'id': '1', 'title': 'This is the second post'}
    assert posts[9] == {'id': '9', 'title': 'This is the tenth post'}


def test_summary_limit(adapter_with_data: SqlAlchemyAdapter):
    first_post, second_post = adapter_with_data.summary(projection=['id', 'title'], limit=2)
    assert first_post == {'id': '0', 'title': 'This is the first post'}
    assert second_post == {'id': '1', 'title': 'This is the second post'}


def test_count(adapter_with_data: SqlAlchemyAdapter):
    assert adapter_with_data.count() == 10


def test_delete(adapter_with_data: SqlAlchemyAdapter):
    adapter_with_data.delete('0')

    with adapter_with_data.engine.connect() as connection:
        with connection.begin():
            result = dict(connection.execute(text(f'SELECT * FROM posts WHERE id=0')).mappings().all())
    assert len(result) == 0


def test_delete_no_data(adapter_with_data: SqlAlchemyAdapter):
    adapter_with_data.delete('0')

    # Should just not raise an exception
    adapter_with_data.delete('0')


def cast_id(value):
    value['id'] = str(value['id'])
    return value


@pytest.fixture()
def posts_data() -> List[Dict]:
    return [{'id': 0,
             'title': 'This is the first post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 12, 22, 22, 222222),
             'body': 'This is a sample post.'},
            {'id': 1,
             'title': 'This is the second post',
             'author': 'beutrano',
             'timestamp': datetime(2022, 2, 22, 11, 22, 22, 222222),
             'body': 'This is a sample post.'},
            {'id': 2,
             'title': 'This is the third post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 10, 22, 22, 222222),
             'body': 'This is a sample post.'},
            {'id': 3,
             'title': 'This is the fourth post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 9, 22, 22, 222222),
             'body': 'This is a sample post.'},
            {'id': 4,
             'title': 'This is the fifth post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 8, 22, 22, 222222),
             'body': 'This is a sample post.'},
            {'id': 5,
             'title': 'This is the sixth post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 7, 22, 22, 222222),
             'body': 'This is a sample post.'},
            {'id': 6,
             'title': 'This is the seventh post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 6, 22, 22, 222222),
             'body': 'This is a sample post.'},
            {'id': 7,
             'title': 'This is the eightieth post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 5, 22, 22, 222222),
             'body': 'This is a sample post.'},
            {'id': 8,
             'title': 'This is the ninetieth post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 4, 22, 22, 222222),
             'body': 'This is a different criteria.'},
            {'id': 9,
             'title': 'This is the tenth post',
             'author': 'danodic',
             'timestamp': datetime(2022, 2, 22, 3, 22, 22, 222222),
             'body': 'This is a different criteria.'}]


@pytest.fixture(scope='function')
def adapter(database) -> SqlAlchemyAdapter:
    return SqlAlchemyAdapter({}, 'posts', engine=database)


@pytest.fixture(scope='function')
def adapter_with_data(adapter, posts_data) -> SqlAlchemyAdapter:
    def insert_record(data, engine):
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text(
                    """
                    INSERT INTO posts
                    VALUES(:id, :title, :author, :body, :timestamp)
                    """), {'id': data['id'],
                           'title': data['title'],
                           'author': data['author'],
                           'body': data['body'],
                           'timestamp': data['timestamp']})

    for post in posts_data:
        insert_record(post, adapter.engine)

    return adapter
