from sqlalchemy import MetaData, create_engine, ForeignKey
from sqlalchemy import Column, Integer, String, Float, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

with open('/Users/arsalanjabbari/Desktop/BOOTCAMP/Book-Store-Analytics/Database/database_info.txt') as p:
    lines = (p.readlines())
    host = lines[0].strip()
    user = lines[1].strip()
    password = lines[2].strip()
    database = lines[3].strip()
    p.close()
# %%
meta = MetaData()
USERNAME = user
PASSWORD = password
SERVER = host
engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{SERVER}:3306/', echo=True)
conn = engine.connect()
database_name = 'IranKetab_scraper'
create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
conn.execute(create_database_query)

# %%
engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{SERVER}:3306/IranKetab_scraper', echo=True)
conn = engine.connect()

# %%
Base = declarative_base()


# Define SQLAlchemy table classes


class Book(Base):
    __tablename__ = 'book'

    id = Column(Integer, unique=True, primary_key=True)
    ISBN = Column(String(32))
    persian_title = Column(String(255))
    english_title = Column(String(255))
    rate = Column(Float)
    price = Column(Float)
    net_price = Column(Float)
    discount_percent = Column(Float)
    current_price = Column(Float)  # for change price in interval time
    publisher_id = Column(Integer, ForeignKey("publisher.pub_id"))
    publisher_name = Column(String(255))
    size = Column(String(32))
    num_page = Column(Integer)
    cover_type = Column(String(32))
    print_series = Column(Integer)
    solarh_py = Column(String(32))
    gregorian_py = Column(String(32))
    language = Column(String(128))


class Person(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, unique=True)
    name = Column(String(255))
    is_writer = Column(Boolean)
    is_translator = Column(Boolean)


class BookTranslator(Base):
    __tablename__ = 'book_translator'
    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("book.id"))
    translator_id = Column(Integer, ForeignKey("person.person_id"))


class BookWriter(Base):
    __tablename__ = 'book_writer'
    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("book.id"))
    writer_id = Column(Integer, ForeignKey("person.person_id"))


class PersonDescription(Base):
    __tablename__ = 'person_description'

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_id = Column(Integer, ForeignKey("person.person_id"))
    like = Column(Integer)
    description = Column(Text)


class Publisher(Base):
    __tablename__ = 'publisher'
    id = Column(Integer, primary_key=True, autoincrement=True)
    pub_id = Column(Integer, unique=True)
    name = Column(String(255))


class BookDescription(Base):
    __tablename__ = 'book_description'

    book_id = Column(Integer, ForeignKey("book.id"), primary_key=True)
    content = Column(Text)


class BookTag(Base):
    __tablename__ = 'book_tag'
    id = Column(Integer, primary_key=True, autoincrement=True)
    book_id = Column(Integer, ForeignKey("book.id"))
    tag_id = Column(Integer, ForeignKey("tag.tag_id"))


class Tag(Base):
    __tablename__ = 'tag'
    tag_id = Column(Integer, primary_key=True)
    tag_title = Column(String(64))


class Price_changes(Base):
    __tablename__ = 'Price_changes'
    id = Column(Integer, primary_key=True)
    book_id = Column(Integer, ForeignKey("book.id"))
    price = Column(Float)
    date = Column(DateTime)


# %%


Base.metadata.create_all(engine)

# %%
Session = sessionmaker(bind=engine)
session = Session()
