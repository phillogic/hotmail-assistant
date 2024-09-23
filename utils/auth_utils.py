# auth_utils.py

import msal
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
import time
import os

# MSAL Configuration

CLIENT_ID = os.getenv('HOTMAIL_CLIENT_ID')
CLIENT_SECRET = os.getenv('HOTMAIL_CLIENT_SECRET')

REDIRECT_URI = 'http://localhost:8000' 
AUTHORITY = f"https://login.microsoftonline.com/consumers"
SCOPE = ["Mail.Read,MailboxSettings.Read"]

# Global variables to cache the token and its expiration
cached_token = None
token_expiry_time = None

# Simple HTTP handler to capture the authorization code from the redirect URL
class AuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        query_components = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if 'code' in query_components:
            authorization_code = query_components['code'][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'Authorization successful! You can close this window.')
            self.server.authorization_code = authorization_code
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'Authorization failed or canceled.')



def get_authorization_code():
    app = msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)
    auth_url = app.get_authorization_request_url(SCOPE, redirect_uri=REDIRECT_URI)
    
    # Open the authorization URL in the user's default browser
    print(f"Opening the following URL in the browser for login: {auth_url}")
    webbrowser.open(auth_url)

    # Start a simple HTTP server to listen for the redirect with the authorization code
    server = HTTPServer(('localhost', 8000), AuthHandler)
    print("Waiting for the authorization code...")
    server.handle_request()
    
    return server.authorization_code
# Function to handle user authentication: get authorization code and access token
def get_access_token():
    global cached_token, token_expiry_time

    # Check if we already have a valid token and it's not expired
    if cached_token and token_expiry_time and token_expiry_time > time.time():
        return cached_token  # Return the cached token if still valid
    
    authorization_code = get_authorization_code()
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=AUTHORITY
    )
    # Step 2: Exchange Authorization Code for Access Token
    token_response = app.acquire_token_by_authorization_code(
        authorization_code, 
        scopes=SCOPE, 
        redirect_uri=REDIRECT_URI
    )
    
    if 'access_token' in token_response:
        return token_response['access_token']
    else:
        raise Exception(f"Failed to get access token: {token_response.get('error_description')}")
