import pandas as pd
from dash import Dash, dcc, html, Input, Output
import plotly.express as px

# --- Firestore imports ---
from services.firebase_client import FirestoreReader
from services.firebase_oauth import FirestoreClientFactory

# --- Firestore setup ---
ABBR_LIST = [
    "HATMN", "HPRAB", "HPRSL", "HRLH", "HSNOW", "HTDRY",
    "HWDAV", "HWDMX", "HWNDS", "VSBAV", "WNS10", "WPGST", "VSBA",
    "SNOWA", "WNDD10", "PHENO", "PRSL", "RLH", "TDRY",
    "LI10I", "LICC", "LIGC", "LIMAXI", "LITOT", "HATMX",
    "HWSMX", "PRSS", "SAJT", "CCTMX", "UVIL",
]

factory = FirestoreClientFactory(
    project="project2025-bff4c",
    client_secret_path="secrets/client_secret.json",
    token_path="secrets/.oauth_token.json",
    scopes=["https://www.googleapis.com/auth/datastore"],
)
db = factory.get_client()
reader = FirestoreReader(ABBR_LIST, db=db, collection_name="meteorological_operational_data")

# --- Helper function to load metric data ---
def load_metric(abbr: str) -> pd.DataFrame:
    data = reader.get(abbr)
    obs = data.get("OBSERVATIONS", [])
    df = pd.DataFrame(obs)
    if df.empty:
        return pd.DataFrame()
    df["Time in Latvia"] = pd.to_datetime(df["DATETIME_LV"], errors="coerce")
    df["Value"] = pd.to_numeric(df["VALUE"], errors="coerce")
    df["LAT"] = df["GEOGR2"]
    df["LON"] = df["GEOGR1"]
    df["Station"] = df["NAME"]
    df["Description"] = data.get("EN_DESCRIPTION", abbr)
    df["Unit"] = data.get("MEASUREMENT_UNIT", "")
    return df

# --- Load initial metric ---
DEFAULT_METRIC = "HTDRY"
df = load_metric(DEFAULT_METRIC)
if df.empty:
    raise ValueError(f"No data found for {DEFAULT_METRIC}")

# --- Prepare slider times ---
unique_times = sorted(df["Time in Latvia"].dropna().unique())
time_marks = {}
for i, t in enumerate(unique_times):
    if t.hour == 0 or i == 0 or i == len(unique_times) - 1:
        time_marks[i] = t.strftime("%b %d")

# --- Dash app ---
app = Dash(__name__)
app.title = "Latvia Meteorological Map — Firestore Live Data"

# --- CSS to move labels above slider ---
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            .rc-slider-mark-text {
                transform: translateY(-35px);
                font-size: 13px;
                white-space: nowrap;
            }
            .rc-slider {
                margin-top: 30px;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""

# --- Layout ---
app.layout = html.Div([
    html.H2("🌡 Latvia Meteorological Map — Live from Firestore",
            style={"textAlign": "center"}),

    html.Div([
        html.Label("Select metric:", style={"marginRight": "10px"}),
        dcc.Dropdown(
            id="metric-dropdown",
            options=[
                {"label": "Hourly Avg Temperature (HTDRY)", "value": "HTDRY"},
                {"label": "Atmospheric Pressure (PRSL)", "value": "PRSL"},
                {"label": "Relative Humidity (RLH)", "value": "RLH"},
            ],
            value=DEFAULT_METRIC,
            clearable=False,
            style={"width": "400px"}
        )
    ], style={"textAlign": "center", "padding": "15px"}),

    html.Div([
        html.Label("Select time:", style={"marginRight": "10px"}),
        dcc.Slider(
            id="time-slider",
            min=0,
            max=len(unique_times) - 1,
            step=1,
            value=len(unique_times) - 1,
            marks=time_marks,
            tooltip={"placement": "bottom", "always_visible": False}
        )
    ], style={"padding": "40px 20px 0 20px"}),

    dcc.Graph(id="temp-map", style={"height": "85vh"})
])

@app.callback(
    Output("temp-map", "figure"),
    [Input("metric-dropdown", "value"),
     Input("time-slider", "value")]
)
def update_map(selected_metric, selected_time_index):
    df_metric = load_metric(selected_metric)
    if df_metric.empty:
        return px.scatter_mapbox(lat=[], lon=[])

    unique_times_local = sorted(df_metric["Time in Latvia"].dropna().unique())
    if selected_time_index >= len(unique_times_local):
        selected_time_index = len(unique_times_local) - 1
    selected_time = unique_times_local[selected_time_index]
    df_time = df_metric[df_metric["Time in Latvia"] == selected_time]

    # --- Adjust color and range ---
    if selected_metric == "HTDRY":
        range_color = [-20, 30]
        color_title = "Temperature (°C)"
        color_scale = [
            [0.0, "#06365a"],
            [0.25, "#0f8ca8"],
            [0.5, "#858510"],
            [0.75, "#81460a"],
            [1.0, "#770204"]
        ]
    else:
        range_color = None
        color_title = df_metric["Unit"].iloc[0] or "Value"
        color_scale = "Magma"

    # --- Build dark map with darker points ---
    fig = px.scatter_mapbox(
        df_time,
        lat="LAT",
        lon="LON",
        color="Value",
        size="Value",
        size_max=18,
        color_continuous_scale=color_scale,
        range_color=range_color,
        zoom=6,
        center={"lat": 56.8, "lon": 24.9},
        hover_name="Station",
        hover_data={
            "Value": True,
            "Time in Latvia": True,
            "LAT": False,
            "LON": False,
        },
        title=f"{df_metric['Description'].iloc[0]} — {selected_time.strftime('%Y-%m-%d %H:%M')}"
    )

    fig.update_traces(
        marker=dict(
            opacity=0.9,
            symbol="circle",
            sizemode="area",
            sizemin=6
        ),
        selector=dict(mode="markers")
    )

    fig.update_layout(
        mapbox_style="open-street-map",
        paper_bgcolor="#181616",
        plot_bgcolor="#0C0B0B",
        font_color="white",
        margin={"r": 10, "t": 60, "l": 10, "b": 10},
        coloraxis_colorbar=dict(
            title=dict(
                text=color_title,
                font=dict(color="white")
            ),
            tickfont=dict(color="white")
        )
    )

    return fig


# --- Run app ---
if __name__ == "__main__":
    app.run(debug=True)