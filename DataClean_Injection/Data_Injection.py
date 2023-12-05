# Import necessary libraries and modules
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from tabulate import tabulate

#%%
# Initialize SQLAlchemy Metadata object
meta = MetaData()
# Define database connection details
USERNAME = 'root'
PASSWORD = '1393ram1393#$'
SERVER = 'localhost'
# Create a SQLAlchemy engine for connecting to the MySQL database
engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{SERVER}:3306/IranKetab_scraper1', echo=True)
conn = engine.connect()

#%%
# Create a base class for declarative models
Base = declarative_base()

#%%
# Create tables in the database based on the defined model classes
Base.metadata.create_all(engine)

#%%
# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

#%% md
# Read data from a CSV file into a Pandas DataFrame
df = pd.read_csv('cleaned_250M_data.csv')
# Display the DataFrame as a formatted table
table = tabulate(df, headers='keys',tablefmt='grid')
print(table)


#%%
# Create a session to insert data into the tables
Session = sessionmaker(bind=engine)
session = Session()

# Insert data from the DataFrame into the 'book' table
df[['id', 'ISBN', 'persian_title', 'english_title', 'rate', 'price', 'net_price', 'discount_percent', 'current_price',
    'publisher_name', 'publisher_id', 'size', 'num_page', 'cover_type',
    'print_series', 'solarh_py', 'gregorian_py']].to_sql('book', con=engine, if_exists='append', index=False)

# Insert data from the DataFrame into person table
df[['id', 'person_id', 'name', 'book_id', 'role']].to_sql('person', con=engine, if_exists='append', index=False)

# Insert data from the DataFrame into person_description table
df[['id', 'person_id', 'description']].to_sql('person_description', con=engine, if_exists='append', index=False)

# Insert data from the DataFrame into publisher table
df[['id', 'publisher_name', 'publisher_id']].to_sql('publisher', con=engine, if_exists='append', index=False)

# Insert data from the DataFrame into book_description table
df[['book_id', 'description']].to_sql('book_description', con=engine, if_exists='append', index=False)

# Insert data from the DataFrame into book_tag table
df[['book_id', 'tag_id']].to_sql('book_tag', con=engine, if_exists='append', index=False)

# Insert data from the DataFrame into tag table
df[['tag_id', 'tag_title']].to_sql('tag', con=engine, if_exists='append', index=False)

# Insert data from the DataFrame into book_translator table
df[['book_id', 'translator_id']].to_sql('book_translator', con=engine, if_exists='append', index=False)

# Insert data from the DataFrame into book_writer table
df[['book_id', 'writer_id']].to_sql('book_writer', con=engine, if_exists='append', index=False)

# Commit changes and close the session
session.commit()
session.close()

