
from utils.email_utils import fetch_emails_in_batches
from utils.db_utils import init_db, get_oldest_email_datetime, close_db

# Main function to handle seeding emails
def main():
    # Initialize the database
    print(f"Intializing the database...")
    conn = init_db()
    print(f"Getting oldest data from the database...")
    # Get the oldest email's receivedDateTime for seeding
    oldest_datetime = get_oldest_email_datetime(conn)
    if oldest_datetime:
        print(f"Resuming seeding from the oldest email: Date={oldest_datetime}")
    else:
        print("No previous record found. Starting a fresh seed.")

    # Fetch emails in batches and write to the database after each batch
    try:
        # Fetch and store emails, but use "<" for the seed process to get older emails
        print(f"Starting fetching of data ...")
        fetch_emails_in_batches(conn, batch_size=100, last_received_datetime=oldest_datetime, is_seed=True)

    except Exception as e:
        print(f"An error occurred: {e}")
        close_db(conn)
        return

    # Close the database connection when done
    close_db(conn)
    print("Seeding completed successfully.")

if __name__ == '__main__':
    main()
