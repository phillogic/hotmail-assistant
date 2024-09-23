# email_utils.py

import requests
import json
from .auth_utils import get_access_token
from .db_utils import store_raw_emails

# Fetch emails in batches and store them after each batch
def fetch_emails_in_batches(conn, batch_size=100, last_received_datetime=None, is_seed=False):
    """
    Fetch emails from Microsoft Graph API in batches and store them after each batch.
    This is used by both the seed and incremental scripts to handle batched fetching and storing.
    The 'is_seed' parameter determines if we are seeding (fetching older emails) or incrementing (fetching newer emails).
    """
    # Get access token (token is cached and refreshed only when necessary)
    access_token = get_access_token()

    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    url = f'https://graph.microsoft.com/v1.0/me/messages?$top={batch_size}'

    # Apply filtering for seeding or incremental updates
    if last_received_datetime:
        if not last_received_datetime.endswith('Z'):
            last_received_datetime += 'Z'  # Ensure ISO 8601 format (UTC)
        
        if is_seed:
            # Seeding: Fetch older emails (received before the last processed time)
            url += f"&$filter=receivedDateTime lt {last_received_datetime}&$orderby=receivedDateTime desc"
        else:
            # Incremental: Fetch newer emails (received after the last processed time)
            url += f"&$filter=receivedDateTime gt {last_received_datetime}"

    while url:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            response_json = response.json()
            emails = response_json.get('value', [])

            # If no emails are returned, stop the iteration
            if not emails:
                print("No more emails to fetch.")
                break

           

            # Prepare email data for insertion
            email_data = [{
                'id': email['id'],
                'raw_data': json.dumps(email),  # Store the raw email data as JSON
                'receivedDateTime': email['receivedDateTime']
            } for email in emails]
            # Process and store each batch of emails
            print(f"Processing {'seeding' if is_seed else 'incremental_fetch'} and storing {len(emails)} emails... " 
            + email_data[0]["receivedDateTime"] + " to " + email_data[-1]["receivedDateTime"])
 
            # Store the emails in the database
            store_raw_emails(conn, email_data)

            print(f"Successfully processed and stored {len(emails)} emails.")

            # Check for pagination (if there's more data to fetch)
            url = response_json.get('@odata.nextLink', None)
        else:
            raise Exception(f"Error fetching emails: {response.status_code} - {response.text}")
