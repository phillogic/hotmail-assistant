from utils.email_utils import fetch_emails_in_batches
from utils.db_utils import init_db, get_newest_email_datetime, close_db

# Main function to handle incremental updates
def main():
    # Initialize the database
    print(f"Intializing the database...")
    conn = init_db()

    # Get the newest email's receivedDateTime for incremental updates
    print(f"Getting newest data from the database...")
    newest_datetime = get_newest_email_datetime(conn)
    if newest_datetime:
        print(f"Resuming incremental updates from the newest email: Date={newest_datetime}")
    else:
        print("No previous emails found. Please run the seed script first.")
        close_db(conn)
        return

    # Fetch new emails in batches and write to the database after each batch
    try:
        # Fetch and store new emails incrementally
        print(f"Starting fetching of data ...")
        fetch_emails_in_batches(conn, batch_size=100, last_received_datetime=newest_datetime, is_seed=False)

    except Exception as e:
        print(f"An error occurred: {e}")
        close_db(conn)
        return

    # Close the database connection when done
    close_db(conn)
    print("Incremental update completed successfully.")

if __name__ == '__main__':
    main()
