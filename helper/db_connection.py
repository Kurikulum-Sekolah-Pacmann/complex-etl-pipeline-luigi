from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

def postgres_engine():
    """
    Helper function untuk melakukan koneksi antara Pandas
    dengan PostgreSQL. Sesuaikan username, password,
    host, dan database name dengan milik masing - masing
    """
    engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

    return engine