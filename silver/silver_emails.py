import json
from utils.db_utils import init_db, init_silver_tables, close_db
import spacy
from bs4 import BeautifulSoup


# Load SpaCy's pre-trained NER model
nlp = spacy.load("en_core_web_md")
nlp.max_length = 1500000


# Function to clean HTML and extract plain text
def clean_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text(separator=" ")

# Function to extract entities using SpaCy
def extract_entities(text):
    if text is None:
        return []
    doc = nlp(text)
    entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]
    return entities

# Function to process both the subject and the cleaned body of the email
def ner_processing(subject, body):
    # Clean the body (remove HTML tags)
    clean_body = clean_html(body)

    # Run SpaCy on the subject and the body
    subject_entities = extract_entities(subject)
    body_entities = extract_entities(clean_body)

    return subject_entities, body_entities

    
    

# Function to process a batch of bronze emails and insert into silver table
def process_bronze_batch(cursor, batch_size=1000):
    # Select raw email data from the Bronze table in batches
    cursor.execute(f'''
        SELECT id, raw_data FROM bronze_emails
        WHERE id NOT IN (SELECT email_id FROM silver_emails)
        LIMIT {batch_size}
    ''')
    return cursor.fetchall()

# Function to extract and transform raw email data from the Bronze table to Silver table in batches
def transform_bronze_to_silver(conn, batch_size=1000):
    cursor = conn.cursor()
    count = 0
    while True:
        print ("Fetching  "+ str(count)  + " + " + str(batch_size))
        count = count + batch_size
        
        emails = process_bronze_batch(cursor, batch_size)
        print ("Processing batch")
        # If no more emails to process, break the loop
        if not emails:
            print("No more emails to process.")
            break

        # Process each email in the batch
        for email in emails:
            email_id, raw_data = email
            raw_email = json.loads(raw_data)

            # Extract fields from the raw email JSON
            createdDateTime = raw_email.get('createdDateTime')
            lastModifiedDateTime = raw_email.get('lastModifiedDateTime')
            changeKey = raw_email.get('changeKey')
            categories = json.dumps(raw_email.get('categories', []))  # Store categories as JSON
            receivedDateTime = raw_email.get('receivedDateTime')
            sentDateTime = raw_email.get('sentDateTime')
            hasAttachments = raw_email.get('hasAttachments', False)
            internetMessageId = raw_email.get('internetMessageId')
            subject = raw_email.get('subject')
            bodyPreview = raw_email.get('bodyPreview')
            importance = raw_email.get('importance')
            parentFolderId = raw_email.get('parentFolderId')
            conversationId = raw_email.get('conversationId')
            conversationIndex = raw_email.get('conversationIndex')
            isDeliveryReceiptRequested = raw_email.get('isDeliveryReceiptRequested')
            isReadReceiptRequested = raw_email.get('isReadReceiptRequested', False)
            isRead = raw_email.get('isRead', False)
            isDraft = raw_email.get('isDraft', False)
            webLink = raw_email.get('webLink')
            inferenceClassification = raw_email.get('inferenceClassification')

            # Extract the body content and content type
            body = raw_email.get('body', {})
            bodyContentType = body.get('contentType', 'text')
            bodyContent = body.get('content', '')
         
            # Process the email to extract entities from both the subject and body
            subject_entities, body_entities = ner_processing(subject, bodyContent)

            # Extract sender and from details
            sender = raw_email.get('sender', {}).get('emailAddress', {})
            sender_name = sender.get('name')
            sender_address = sender.get('address')

            from_info = raw_email.get('from', {}).get('emailAddress', {})
            from_name = from_info.get('name')
            from_address = from_info.get('address')

            # Extract recipients (toRecipients, ccRecipients, bccRecipients, and replyTo)
            toRecipients = json.dumps([recipient['emailAddress'] for recipient in raw_email.get('toRecipients', [])])
            ccRecipients = json.dumps([recipient['emailAddress'] for recipient in raw_email.get('ccRecipients', [])])
            bccRecipients = json.dumps([recipient['emailAddress'] for recipient in raw_email.get('bccRecipients', [])])
            replyTo = json.dumps([reply['emailAddress'] for reply in raw_email.get('replyTo', [])])

            # Extract flag status
            flagStatus = raw_email.get('flag', {}).get('flagStatus', 'notFlagged')
            values = (
    email_id, createdDateTime, lastModifiedDateTime, changeKey, categories, receivedDateTime, sentDateTime,
    hasAttachments, internetMessageId, subject, bodyPreview, importance, parentFolderId, conversationId,
    conversationIndex, isDeliveryReceiptRequested, isReadReceiptRequested, isRead, isDraft, webLink,
    inferenceClassification, bodyContentType, bodyContent, sender_name, sender_address, from_name,
    from_address, toRecipients, ccRecipients, bccRecipients, replyTo, flagStatus,json.dumps(subject_entities),json.dumps(body_entities)
        )
            #print(values)
            #print(f"Length of values tuple: {len(values)}")  # Should be 32

            # Insert the transformed data into the Silver table
            cursor.execute('''
                INSERT OR REPLACE INTO silver_emails (
                    email_id, createdDateTime, lastModifiedDateTime, changeKey, categories, receivedDateTime, sentDateTime, 
                    hasAttachments, internetMessageId, subject, bodyPreview, importance, parentFolderId, conversationId, 
                    conversationIndex, isDeliveryReceiptRequested, isReadReceiptRequested, isRead, isDraft, webLink, 
                    inferenceClassification, bodyContentType, bodyContent, sender_name, sender_address, from_name, 
                    from_address, toRecipients, ccRecipients, bccRecipients, replyTo, flagStatus,subject_entities,body_entities
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)
            ''', values)

        # Commit the batch to the database
        conn.commit()
        print(f"Processed {len(emails)} emails.")

# Main function to run the Silver layer transformation
def main():
    conn = init_db()
    init_silver_tables(conn)

    # Transform and load data from Bronze to Silver in batches
    transform_bronze_to_silver(conn, batch_size=100)  # Adjust batch size as needed

    close_db(conn)
    print("Silver layer processing completed successfully.")

if __name__ == '__main__':
    main()
