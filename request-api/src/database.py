import os
from functools import cache

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@cache
def engine():
    return create_engine(os.environ['DATABASE_URL'])


@cache
def session_maker():
    return sessionmaker(autocommit=False, autoflush=False, bind=engine())
