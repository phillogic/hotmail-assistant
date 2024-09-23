import sqlite3
import os

# Ensure the database is created in a folder called 'data'
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'hotmail_emails.db')

# Ensure the 'data' folder exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Initialize SQLite Database, creating it in the 'data' folder
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create table to store raw email data if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bronze_emails (
            id TEXT PRIMARY KEY, 
            raw_data TEXT,
            receivedDateTime TEXT
        )
    ''')


    conn.commit()
    
    return conn
def init_silver_tables(conn):
    cursor = conn.cursor()

    # Create the Silver table to store transformed and enriched email data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS silver_emails (
    email_id TEXT PRIMARY KEY,                 -- Unique identifier for the email
    createdDateTime TEXT,                      -- When the email was created
    lastModifiedDateTime TEXT,                 -- When the email was last modified
    changeKey TEXT,                            -- Change key for email tracking
    categories TEXT,                           -- Categories (can be JSON or TEXT)
    receivedDateTime TEXT,                     -- When the email was received
    sentDateTime TEXT,                         -- When the email was sent
    hasAttachments BOOLEAN,                    -- Indicates if the email has attachments
    internetMessageId TEXT,                    -- Unique internet message ID
    subject TEXT,                              -- Email subject
    bodyPreview TEXT,                          -- Preview of the email body
    importance TEXT,                           -- Importance level (e.g., normal, high)
    parentFolderId TEXT,                       -- ID of the parent folder where the email is stored
    conversationId TEXT,                       -- Conversation ID
    conversationIndex TEXT,                    -- Conversation index for email threading
    isDeliveryReceiptRequested BOOLEAN,        -- If a delivery receipt was requested
    isReadReceiptRequested BOOLEAN,            -- If a read receipt was requested
    isRead BOOLEAN,                            -- Whether the email has been read
    isDraft BOOLEAN,                           -- Whether the email is a draft
    webLink TEXT,                              -- Web link to the email in Outlook
    inferenceClassification TEXT,              -- Classification (e.g., focused, other)
    bodyContentType TEXT,                      -- Content type of the body (e.g., text, html)
    bodyContent TEXT,                          -- Full email body
    sender_name TEXT,                          -- Name of the sender
    sender_address TEXT,                       -- Email address of the sender
    from_name TEXT,                            -- Name of the 'From' address
    from_address TEXT,                         -- Email address of the 'From' address
    toRecipients TEXT,                         -- Recipients (can be stored as JSON)
    ccRecipients TEXT,                         -- CC Recipients (can be stored as JSON)
    bccRecipients TEXT,                        -- BCC Recipients (can be stored as JSON)
    replyTo TEXT,                              -- Reply-to information (can be stored as JSON)
    flagStatus TEXT,                            -- Flag status (e.g., notFlagged, complete),
    subject_entities TEXT,                     -- JSON field for entities extracted from the subject
    body_entities TEXT                         -- JSON field for entities extracted from the body
);

    ''')

    conn.commit()



# Store raw emails in SQLite (Bronze Layer)
def store_raw_emails(conn, emails):
    cursor = conn.cursor()

    # Insert each email into the database
    for email in emails:
        cursor.execute('''
            INSERT OR IGNORE INTO bronze_emails (id, raw_data, receivedDateTime)
            VALUES (?, ?, ?)
        ''', (
            email['id'], 
            email['raw_data'],  # Store raw email data as JSON
            email['receivedDateTime']  #-- Storing receivedDateTime as ISO 8601 string (e.g., 2024-09-19T03:08:45Z)
        ))

    conn.commit()

# Get the oldest email's receivedDateTime for seeding (from bronze_emails table, using STRFTIME to interpret datetime)
def get_oldest_email_datetime(conn):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT receivedDateTime 
        FROM bronze_emails 
        ORDER BY STRFTIME('%Y-%m-%d %H:%M:%S', REPLACE(REPLACE(receivedDateTime, 'T', ' '), 'Z', '')) ASC 
        LIMIT 1
    ''')
    result = cursor.fetchone()
    return result[0] if result else None

# Get the newest email's receivedDateTime for incremental updates (from bronze_emails table, using STRFTIME to interpret datetime)
def get_newest_email_datetime(conn):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT receivedDateTime 
        FROM bronze_emails 
        ORDER BY STRFTIME('%Y-%m-%d %H:%M:%S', REPLACE(REPLACE(receivedDateTime, 'T', ' '), 'Z', '')) DESC 
        LIMIT 1
    ''')
    result = cursor.fetchone()
    return result[0] if result else None
######################### old code ###########


# Close the database connection
def close_db(conn):
    conn.close()
