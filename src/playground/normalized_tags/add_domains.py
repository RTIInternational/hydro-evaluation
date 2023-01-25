import config
import utils
from models import StringTagTypes, DateTimeTagTypes
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

engine = create_engine(
    config.CONNECTION,
    echo=True,
    future=True
)

with Session(engine) as session:
    print(f"Loading string tags")
    for tag_type in config.STRING_TAG_TYPES:
        tag, cr = utils.get_or_create(session, StringTagTypes, name=tag_type)


    print(f"Loading datetime tags")
    for tag_type in config.DATETIME_TAG_TYPES:
        tag, cr = utils.get_or_create(session, DateTimeTagTypes, name=tag_type)
    
    session.commit()
