import pytest
from sqlalchemy import create_engine


@pytest.fixture(scope='function')
def database():
    return create_engine("sqlite://", echo=True)
