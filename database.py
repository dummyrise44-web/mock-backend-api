from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

# Use the full External Database URL from Render
DATABASE_URL = "postgresql://mock_database_user:pDBzcgGpxPZ8bPaHmwO34Oy7CBSSxLxN@dpg-d4ptatadbo4c73bhc9ug-a.singapore-postgres.render.com:5432/mock_database"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
metadata = MetaData()
