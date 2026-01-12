import sqlite3
import os

def migrate_db():
    db_path = "data/documents.db"
    
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if 'token' column exists
        cursor.execute("PRAGMA table_info(mcp_tokens)")
        columns = [info[1] for info in cursor.fetchall()]
        
        if 'token' not in columns:
            print("Adding 'token' column to mcp_tokens table...")
            cursor.execute("ALTER TABLE mcp_tokens ADD COLUMN token TEXT")
            conn.commit()
            print("Migration successful!")
        else:
            print("'token' column already exists.")
            
    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    migrate_db()












