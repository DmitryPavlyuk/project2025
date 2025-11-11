import os
import sys
from typing import List, Optional

from google.cloud import firestore
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.auth.transport.requests import Request
from google.api_core.client_options import ClientOptions
import google.auth

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except Exception:
    InstalledAppFlow = None  # desktop-only


def _in_colab() -> bool:
    # if launched from Colab runtime
    if "COLAB_RELEASE_TAG" in os.environ:
        return True
    try:
        import google.colab  # noqa: F401
        return True
    except Exception:
        return False


def _is_interactive_tty() -> bool:
    # Avoid launching a browser in Colab
    return sys.stdin.isatty() and sys.stdout.isatty()


class FirestoreClientFactory:
    """
    Cross-env Firestore client factory.

    Credential resolution order:
      1) GOOGLE_APPLICATION_CREDENTIALS -> Service Account
      2) Application Default Credentials (ADC) via google.auth.default()
      3) OAuth user token cache (token_path)
      4) Desktop OAuth local-server flow (browser)  [never used in Colab]

    Notes:
      - On Colab, prefer ADC (authenticate once with gcloud).
      - On Desktop, first run opens a browser; token is cached to token_path.
      - Quota project is attached when supported.
    """

    def __init__(
        self,
        project: str,
        client_secret_path: str = ".secrets/client_secret.json",
        token_path: str = ".secrets/.oauth_token.json",
        scopes: Optional[List[str]] = None,
        quota_project: Optional[str] = None,
    ) -> None:
        if not project:
            raise ValueError("project must be provided (GCP Project ID)")
        self.project = project
        self.client_secret_path = client_secret_path
        self.token_path = token_path
        self.scopes = scopes or ["https://www.googleapis.com/auth/datastore"]
        self.quota_project = quota_project or project

        token_dir = os.path.dirname(os.path.abspath(self.token_path))
        if token_dir and not os.path.exists(token_dir):
            os.makedirs(token_dir, exist_ok=True)


    def _creds_from_service_account(self):
        sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if sa_path and os.path.exists(sa_path):
            return ServiceAccountCredentials.from_service_account_file(
                sa_path, scopes=self.scopes
            )
        return None

    def _creds_from_adc(self):
        try:
            creds, _ = google.auth.default(scopes=self.scopes)
            return creds
        except Exception:
            return None

    def _creds_from_token_cache(self):
        if os.path.exists(self.token_path):
            try:
                return Credentials.from_authorized_user_file(self.token_path, self.scopes)
            except Exception:
                return None
        return None

    def _desktop_interactive_oauth(self):
        """
        Desktop-only: opens a browser for OAuth. Never called in Colab or headless.
        """
        if InstalledAppFlow is None:
            raise RuntimeError(
                "google-auth-oauthlib is missing. Install with: pip install google-auth-oauthlib"
            )
        if not os.path.exists(self.client_secret_path):
            raise FileNotFoundError(
                f"client_secret.json not found at: {self.client_secret_path}"
            )
        flow = InstalledAppFlow.from_client_secrets_file(self.client_secret_path, self.scopes)
        if not _is_interactive_tty():
            raise RuntimeError(
                "Interactive OAuth requires a TTY. In Colab or headless, use ADC:\n"
                "  - Colab: !gcloud auth login --no-launch-browser --update-adc\n"
                "  - Desktop (optional): gcloud auth application-default login"
            )
        creds = flow.run_local_server(port=0)
        # Cache token
        try:
            with open(self.token_path, "w") as f:
                f.write(creds.to_json())
        except Exception:
            pass
        return creds

    # ------------ public API ------------

    def get_client(self) -> firestore.Client:
        creds = None

        # 1) Service Account (GOOGLE_APPLICATION_CREDENTIALS)
        creds = self._creds_from_service_account()
        if not creds:
            # 2) ADC (Colab / gcloud / Workstation / Desktop)
            creds = self._creds_from_adc()

        # 3) OAuth user token cache
        if not creds:
            creds = self._creds_from_token_cache()
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                    with open(self.token_path, "w") as f:
                        f.write(creds.to_json())
                except Exception:
                    creds = None

        # 4) Desktop OAuth (browser)
        if not creds:
            if _in_colab():
                raise RuntimeError(
                    "No credentials found in Colab.\n"
                    "Run once in a cell:\n"
                    "  !gcloud auth login --no-launch-browser --update-adc\n"
                    "  !gcloud config set project {proj}\n"
                    "  !gcloud auth application-default set-quota-project {proj}\n"
                    "Then re-run your script."
                    .format(proj=self.project)
                )
            creds = self._desktop_interactive_oauth()

        client_opts = ClientOptions(quota_project_id=self.quota_project)
        return firestore.Client(
            project=self.project,
            credentials=creds,
            client_options=client_opts,
        )
