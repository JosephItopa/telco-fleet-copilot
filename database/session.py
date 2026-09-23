"""SQLAlchemy engine and session management (PostgreSQL; SQLite for tests)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config.settings import settings

_engine_kwargs: dict = {"pool_pre_ping": True, "future": True}
if not settings.database_url.startswith("sqlite"):
    _engine_kwargs.update(pool_size=settings.db_pool_size, max_overflow=settings.db_max_overflow)

engine = create_engine(settings.database_url, **_engine_kwargs)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


def init_db() -> None:
    from database.models import Base

    Base.metadata.create_all(bind=engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
