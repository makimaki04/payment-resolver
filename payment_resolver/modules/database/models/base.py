from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Declarative base shared with the Alembic migration metadata."""
