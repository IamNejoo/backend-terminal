import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy import text
from app.core.database import engine

async def test_connection():
    """Test database connection"""
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT 1"))
            print("✓ Database connection successful!")
            
            # Test PostgreSQL version
            result = await conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"✓ PostgreSQL version: {version}")
            
            # List databases
            result = await conn.execute(
                text("SELECT datname FROM pg_database WHERE datistemplate = false")
            )
            databases = [row[0] for row in result]
            print(f"✓ Available databases: {databases}")
            
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(test_connection())