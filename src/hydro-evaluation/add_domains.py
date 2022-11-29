import config
from models import StringTagType, DateTimeTagType
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

engine = create_engine(config.CONNECTION, echo=True, future=True)

def get_or_create(session, model, defaults=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).one_or_none()
    if instance:
        return instance, False
    else:
        kwargs |= defaults or {}
        instance = model(**kwargs)
        try:
            session.add(instance)
            session.commit()
        except Exception:  # The actual exception depends on the specific database so we catch all exceptions. This is similar to the official documentation: https://docs.sqlalchemy.org/en/latest/orm/session_transaction.html
            session.rollback()
            instance = session.query(model).filter_by(**kwargs).one()
            return instance, False
        else:
            return instance, True


with Session(engine) as session:
    print(f"Loading string tags")
    for tag_type in config.STRING_TAG_TYPES:
        tag, cr = get_or_create(session, StringTagType, name=tag_type)


    print(f"Loading datetime tags")
    for tag_type in config.DATETIME_TAG_TYPES:
        tag, cr = get_or_create(session, DateTimeTagType, name=tag_type)
    
    session.commit()
