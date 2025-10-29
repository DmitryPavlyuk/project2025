import os
from typing import List, Optional

from google.cloud import firestore
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.api_core.client_options import ClientOptions

class FirestoreClientFactory:
    """
    Minimal helper to create a Firestore client using OAuth Desktop flow.
    - Caches tokens to TOKEN_PATH
    - Attaches quota project to avoid CONSUMER_INVALID
    - Works with read-only or read/write scopes (IAM still enforces permissions)
    """

    def __init__(
        self,
        project: str,
        client_secret_path: str = "secrets/client_secret.json",
        token_path: str = "secrets/.oauth_token.json",
        scopes: Optional[List[str]] = None,
    ) -> None:
        if not project:
            raise ValueError("project must be provided (GCP Project ID)")
        if not os.path.exists(client_secret_path):
            raise FileNotFoundError(
                f"client_secret.json not found at: {client_secret_path}"
            )

        self.project = project
        self.client_secret_path = client_secret_path
        self.token_path = token_path
        self.scopes = scopes or ["https://www.googleapis.com/auth/datastore"]

    def _load_or_create_credentials(self) -> Credentials:
        creds = None
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, self.scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.client_secret_path, self.scopes
                )
                creds = flow.run_local_server(port=0)

            with open(self.token_path, "w") as f:
                f.write(creds.to_json())

        return creds

    def get_client(self) -> firestore.Client:
        creds = self._load_or_create_credentials()
        client_opts = ClientOptions(quota_project_id=self.project)
        return firestore.Client(
            project=self.project,
            credentials=creds,
            client_options=client_opts,
        )