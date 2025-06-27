import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.core.database import engine, Base
from app.core.config import get_settings

settings = get_settings()

async def init_db():
    """Initialize database - create all tables"""
    print("Creating database tables...")
    
    async with engine.begin() as conn:
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
        
        # Verify connection
        result = await conn.execute(text("SELECT version()"))
        version = result.scalar()
        print(f"Connected to PostgreSQL: {version}")
        
    print("Database initialized successfully!")

async def drop_db():
    """Drop all tables - use with caution!"""
    print("Dropping all tables...")
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        
    print("All tables dropped!")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "drop":
        asyncio.run(drop_db())
    else:
        asyncio.run(init_db())