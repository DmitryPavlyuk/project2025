from services.firebase_client import FirestoreWriter 
from services.meteo_client import MeteoComposer
from services.firebase_oauth import FirestoreClientFactory

ABBR_LIST = [
    "HATMN",  # The hourly minimum of air temperature
    "HPRAB",  # The total amount of precipitation within an hour
    "HPRSL",  # The hourly average atmospheric pressure at sea level
    "HRLH",   # The hourly average relative humidity
    "HSNOW",  # The hourly average snow depth
    "HTDRY",  # The hourly average air temperature
    "HWDAV",  # The hourly average wind direction
    "HWDMX",  # The direction of the hourly maximum wind gusts
    "HWNDS",  # The hourly average wind speed
    "VSBAV",  # The hourly average meteorological visibility
    "WNS10",  # Average wind speed during the observation time
    "WPGST",  # Maximum wind gusts during the observation time
    "VSBA",   # Meteorological visibility during the observation time
    "SNOWA",  # Snow depth during the observation time
    "WNDD10", # Average wind direction during the observation time
    "PHENO",  # Atmospheric phenomena
    "PRSL",   # Atmospheric pressure at sea level during the observation time
    "RLH",    # Relative humidity during the observation time
    "TDRY",   # Air temperature during the observation time
    "LI10I",  # Number of lightning strikes with current > 10 kA
    "LICC",   # Number of cloud-cloud lightning strikes
    "LIGC",   # Number of cloud-ground lightning strikes
    "LIMAXI", # Maximum current of lightning strikes
    "LITOT",  # Total number of lightning strikes
    "HATMX",  # The hourly maximum of air temperature
    "HWSMX",  # The hourly maximum wind gusts
    "PRSS",   # Atmospheric pressure at station level during the observation time
    "SAJT",   # Apparent temperature during the observation time
    "CCTMX",  # The hourly maximum amount of cloud cover
    "UVIL",   # Ultraviolet radiation index during the observation time
]

factory = FirestoreClientFactory(
    project="project2025-bff4c",
    client_secret_path="secrets/client_secret.json",     # don't forget to add this file!!!!!!!!!
    token_path="secrets/.oauth_token.json",              # automatically generated (exp: <7 days)
    scopes=["https://www.googleapis.com/auth/datastore"],
)
db = factory.get_client()

writer = FirestoreWriter(db=db, collection_name="meteorological_operational_data")

for abbr in ABBR_LIST:
    try:
        payload = MeteoComposer(target_abbr=abbr).build_payload()
        writer.save_incremental(payload)
    except Exception as e:
        print(f"[ERR] {abbr}: {e}")