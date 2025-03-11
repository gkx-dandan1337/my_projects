import dash
from dash import dcc, html, Input, Output
import pandas as pd
import plotly.express as px
import pyodbc
import os

# Set up the database connection (modify this with your actual credentials)
connection_string ="Driver={ODBC Driver 18 for SQL Server};Server=tcp:modular-server.database.windows.net,1433;Database=modular;Uid=CloudSA1c5b822c;Pwd={Givemeinternship!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

def fetch_data():
    """Fetch unique security descriptions from the database."""
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    query = "SELECT DISTINCT Security FROM mktdata;"
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return [row[0] for row in data]

def fetch_timeseries_data(security_description):
    """Fetch time series data for the selected security."""
    conn = pyodbc.connect(connection_string)
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

# Initialize the Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Security Trading Dashboard"),
    dcc.Dropdown(
        id='security-dropdown',
        options=[{'label': sec, 'value': sec} for sec in fetch_data()],
        placeholder="Select a Security",
    ),
    dcc.Graph(id='timeseries-chart')
])

@app.callback(
    Output('timeseries-chart', 'figure'),
    Input('security-dropdown', 'value')
)
def update_chart(selected_security):
    if not selected_security:
        return px.line(title="Select a security to view the chart")
    
    timestamps, trades, tta = fetch_timeseries_data(selected_security)
    
    fig = px.line(x=timestamps, y=[trades, tta],
                  labels={'value': 'Count', 'x': 'Date'},
                  title=f'Trade and TTA for {selected_security}')
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)