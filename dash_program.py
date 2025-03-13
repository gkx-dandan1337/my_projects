import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output
import pandas as pd
import plotly.express as px
import pypyodbc
import time
import logging


# Configure logging
logging.basicConfig(filename="dashboard.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Set up the database connection
connection_string ="Driver={ODBC Driver 18 for SQL Server};Server=tcp:modular-server.database.windows.net,1433;Database=modular;Uid=CloudSA1c5b822c;Pwd={Givemeinternship!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

def connect_to_db(connection_string):
    start_time = time.time()
    retry_interval = 10 
    max_retry_duration = 5*60 #5 minutes
    while True:
        try:
            logging.info("Attempting to connect to the database...")
            conn = pypyodbc.connect(connection_string)
            logging.info("Returning database object...")
            return conn  # If successful, return the connection object

        except pypyodbc.Error as e:
            logging.error(f"Database connection failed: {e}")
            elapsed_time = time.time() - start_time  # Check the elapsed time
            if elapsed_time >= max_retry_duration:
                logging.error("Max retry duration reached. Exiting...")
                return None  # After 5 minutes, stop trying and return None
            else:
                logging.info(f"Retrying... Elapsed time: {elapsed_time:.2f} seconds")
                time.sleep(retry_interval)  # Wait for `retry_interval` seconds before retrying

    
def fetch_data():
    """Fetch unique security descriptions from the database."""
    conn = connect_to_db(connection_string)
    if not conn:
        return []
    query = "SELECT DISTINCT Security FROM mktdata;"
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return [row[0] for row in data] #returns the securities.

def fetch_timeseries_data(security_description):
    """Fetch time series data for the selected security."""
    conn = connect_to_db(connection_string)
    if not conn:
        return [] , [] , []
    cursor = conn.cursor()
    query = """
    SELECT Timestamp, Trades, TTA 
    FROM mktdata 
    WHERE Security = ? 
    ORDER BY Timestamp;"""
    cursor.execute(query, (security_description,))
    data = cursor.fetchall()
    conn.close()
    
    timestamps = []
    trades = []
    tta = []
    
    for row in data:
        timestamps.append(row[0])
        trades.append(row[1])
        tta.append(row[2])
    
    return timestamps, trades, tta

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])  # Add Bootstrap theme

# Layout with Bootstrap components
app.layout = html.Div([
    dbc.Container([
        dbc.Row([
            dbc.Col(html.H1("Security Trading Dashboard", className="text-center"), width=12)
        ]),
        dbc.Row([
            dbc.Col(dcc.Dropdown(
                id='security-dropdown',
                options=[{'label': sec, 'value': sec} for sec in fetch_data()],
                placeholder="Select a Security",
            ), width=12)
        ]),
        dbc.Row([
            dbc.Col(dcc.Loading(id="loading-graph-trades", type="circle", children=[dcc.Graph(id='timeseries-chart-trades')]), width=12)
        ]),
        dbc.Row([
            dbc.Col(dcc.Loading(id="loading-graph-tta", type="circle", children=[dcc.Graph(id='timeseries-chart-tta')]), width=12)
        ])
    ])
])

@app.callback(
    Output('security-dropdown', 'options'),
    Input('security-dropdown', 'value')  # Triggered whenever a security is selected
)
def update_securities(selected_security):
    """Fetch and return the list of securities each time the dropdown is updated."""
    security_options = [{'label': sec, 'value': sec} for sec in fetch_data()]
    return security_options

@app.callback(
    Output('timeseries-chart-trades', 'figure'),
    Output('timeseries-chart-tta', 'figure'),
    Input('security-dropdown', 'value')
)

def update_chart(selected_security):
    if not selected_security:
        return px.line(title="Select a security to view the chart")
    
    timestamps, trades, tta = fetch_timeseries_data(selected_security)
    
    trades_fig = px.line(x=timestamps, y=trades,
                  labels={'y': 'Trade Count', 'x': 'Date'},
                  title=f'Trades for {selected_security}')
    
    tta_fig = px.line(x=timestamps, y=tta,
                  labels={'y': 'TTA Count', 'x': 'Date'},
                  title=f'TTA for {selected_security}')
    
    return trades_fig, tta_fig


# @app.callback(
#     Output('timeseries-chart', 'figure'),
#     Input('security-dropdown', 'value')
# )
# def update_chart(selected_security):
#     if not selected_security:
#         return px.line(title="Select a security to view the chart")
    
#     timestamps, trades, tta = fetch_timeseries_data(selected_security)
    
#     fig = px.line(x=timestamps, y=[trades, tta],
#                   labels={'value': 'Count', 'x': 'Date'},
#                   title=f'Trade and TTA for {selected_security}')
#     return fig

def main():
    app.run_server(host='0.0.0.0', port=8050, debug=True)

main()