import threading
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import schedule
from dash import Input, Output, dcc, html

from config import DASHBOARD_TITLE, UPDATE_INTERVAL_HOURS
from sam_api import SAMAfricaAPI, update_africa_data, create_sample_data

# --------------------------------------------------------------- #
# PRODUCTION DATA LOADING                                         #
# --------------------------------------------------------------- #

sam_api = SAMAfricaAPI()

def load_data() -> pd.DataFrame:
    """Load data with comprehensive error handling for production."""
    try:
        # Try to load existing data first
        df = sam_api.load_from_csv()
        
        if df.empty:
            print("[INFO] No existing data found. Attempting to fetch from API...")
            df = update_africa_data()
            
        if df.empty:
            print("[WARN] No data available from API. Using sample data.")
            df = create_sample_data()
        
        print(f"[INFO] Loaded {len(df)} opportunities for dashboard")
        return df
        
    except Exception as exc:
        print(f"[ERROR] Critical error in load_data: {exc}")
        print("[INFO] Falling back to sample data")
        return create_sample_data()

# Initialize data
opportunities_df: pd.DataFrame = load_data()

# --------------------------------------------------------------- #
# DASH APP SETUP                                                 #
# --------------------------------------------------------------- #

app = dash.Dash(__name__)
app.title = DASHBOARD_TITLE
server = app.server

# --------------------------------------------------------------- #
# DASHBOARD LAYOUT                                               #
# --------------------------------------------------------------- #

app.layout = html.Div([
    # Header Section
    html.Div([
        html.H1(DASHBOARD_TITLE, className="header-title"),
        html.P(
            f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            className="last-updated",
        ),
        html.Button("Refresh Data", id="refresh-btn", className="refresh-button"),
    ], className="header"),
    
    # Main Content
    html.Div([
        # Statistics Cards
        html.Div([
            html.Div([
                html.H3(id="total-opportunities", className="stat-number"),
                html.P("Total Opportunities", className="stat-label"),
            ], className="stat-card"),
            
            html.Div([
                html.H3(id="active-opportunities", className="stat-number"),
                html.P("Active Opportunities", className="stat-label"),
            ], className="stat-card"),
            
            html.Div([
                html.H3(id="countries-count", className="stat-number"),
                html.P("African Countries", className="stat-label"),
            ], className="stat-card"),
            
            html.Div([
                html.H3(id="agencies-count", className="stat-number"),
                html.P("Agencies", className="stat-label"),
            ], className="stat-card"),
        ], className="stats-container"),
        
        # Filter Controls
        html.Div([
            html.Div([
                html.Label("Country:", className="filter-label"),
                dcc.Dropdown(
                    id="country-filter",
                    multi=True,
                    placeholder="Select countries…"
                ),
            ], className="filter-item"),
            
            html.Div([
                html.Label("Agency:", className="filter-label"),
                dcc.Dropdown(
                    id="agency-filter",
                    multi=True,
                    placeholder="Select agencies…"
                ),
            ], className="filter-item"),
            
            html.Div([
                html.Label("Notice Type:", className="filter-label"),
                dcc.Dropdown(
                    id="type-filter",
                    multi=True,
                    placeholder="Select notice types…"
                ),
            ], className="filter-item"),
        ], className="filters-container"),
        
        # Charts Row 1
        html.Div([
            html.Div([dcc.Graph(id="country-chart")], className="chart-container"),
            html.Div([dcc.Graph(id="agency-chart")], className="chart-container"),
        ], className="charts-row"),
        
        # Timeline Chart
        html.Div([
            html.Div([dcc.Graph(id="timeline-chart")], className="chart-container full-width"),
        ], className="charts-row"),
        
        # Opportunities Table
        html.Div([
            html.H3("Recent Opportunities", className="section-title"),
            html.Div(id="opportunities-table"),
        ], className="table-container"),
        
    ], className="main-content"),
])

# --------------------------------------------------------------- #
# CALLBACK FUNCTIONS                                             #
# --------------------------------------------------------------- #

@app.callback(
    [
        Output("total-opportunities", "children"),
        Output("active-opportunities", "children"),
        Output("countries-count", "children"),
        Output("agencies-count", "children"),
        Output("country-filter", "options"),
        Output("agency-filter", "options"),
        Output("type-filter", "options"),
    ],
    [Input("refresh-btn", "n_clicks")],
)
def update_stats_and_filters(n_clicks: Optional[int]):
    """Update dashboard statistics and filter options."""
    global opportunities_df
    
    if n_clicks and n_clicks > 0:
        print("[INFO] Manual refresh triggered")
        try:
            opportunities_df = update_africa_data()
        except Exception as e:
            print(f"[ERROR] Refresh failed: {e}")
    
    if opportunities_df.empty:
        return "0", "0", "0", "0", [], [], []
    
    # Calculate statistics
    total_opps = len(opportunities_df)
    
    # Active opportunities (not archived)
    active_mask = (
        opportunities_df["archive_type"].isna() |
        (opportunities_df["archive_type"] == "") |
        (opportunities_df["archive_type"] == "nan")
    )
    active_opps = len(opportunities_df[active_mask])
    
    # Unique values for filters
    countries = sorted([c for c in opportunities_df["african_country"].dropna().unique() if c != "nan"])
    agencies = sorted([a for a in opportunities_df["department"].dropna().unique() if a != "nan"])
    notice_types = sorted([n for n in opportunities_df["notice_type"].dropna().unique() if n != "nan"])
    
    return (
        f"{total_opps:,}",
        f"{active_opps:,}",
        f"{len(countries):,}",
        f"{len(agencies):,}",
        [{"label": c, "value": c} for c in countries],
        [{"label": a, "value": a} for a in agencies],
        [{"label": t, "value": t} for t in notice_types],
    )


@app.callback(
    [
        Output("country-chart", "figure"),
        Output("agency-chart", "figure"),
        Output("timeline-chart", "figure"),
        Output("opportunities-table", "children"),
    ],
    [
        Input("country-filter", "value"),
        Input("agency-filter", "value"),
        Input("type-filter", "value"),
    ],
)
def update_charts_and_table(selected_countries, selected_agencies, selected_types):
    """Update charts and table based on filter selections."""
    global opportunities_df
    
    # Apply filters
    df = opportunities_df.copy()
    
    if selected_countries:
        df = df[df["african_country"].isin(selected_countries)]
    if selected_agencies:
        df = df[df["department"].isin(selected_agencies)]
    if selected_types:
        df = df[df["notice_type"].isin(selected_types)]
    
    # Handle empty dataset
    if df.empty:
        empty_fig = go.Figure().add_annotation(
            text="No data available for selected filters",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16)
        )
        return empty_fig, empty_fig, empty_fig, html.Div("No opportunities match the selected filters.")
    
    # Country Chart
    country_counts = df["african_country"].value_counts().head(10)
    country_fig = px.bar(
        x=country_counts.values,
        y=country_counts.index,
        orientation="h",
        title="Opportunities by African Country (Top 10)",
        labels={"x": "Number of Opportunities", "y": "Country"},
        color=country_counts.values,
        color_continuous_scale="Blues"
    )
    country_fig.update_layout(height=400, showlegend=False)
    
    # Agency Chart
    agency_counts = df["department"].value_counts().head(8)
    agency_fig = px.pie(
        values=agency_counts.values,
        names=[name[:30] + "..." if len(name) > 30 else name for name in agency_counts.index],
        title="Opportunities by Agency",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    agency_fig.update_layout(height=400)
    
    # Timeline Chart
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    timeline_df = df.groupby(df["posted_date"].dt.date).size().reset_index(name="count")
    
    if not timeline_df.empty:
        timeline_fig = px.line(
            timeline_df,
            x="posted_date",
            y="count",
            title="Opportunities Posted Over Time",
            labels={"posted_date": "Date", "count": "Number of Opportunities"}
        )
        timeline_fig.update_traces(line=dict(color="darkblue", width=2))
    else:
        timeline_fig = go.Figure().add_annotation(
            text="No timeline data available",
            x=0.5, y=0.5,
            showarrow=False
        )
    
    timeline_fig.update_layout(height=300)
    
    # Opportunities Table
    table_df = df[
        ["title", "department", "african_country", "posted_date", "response_date"]
    ].head(25)
    
    table_rows = []
    for _, row in table_df.iterrows():
        # Truncate long titles
        title = str(row["title"])
        display_title = title[:80] + "..." if len(title) > 80 else title
        
        # Format dates
        posted_date = ""
        if pd.notna(row["posted_date"]):
            try:
                posted_date = pd.to_datetime(row["posted_date"]).strftime("%Y-%m-%d")
            except:
                posted_date = str(row["posted_date"])[:10]
        
        table_rows.append(html.Tr([
            html.Td(display_title, title=title),  # Full title on hover
            html.Td(str(row["department"])[:40] + "..." if len(str(row["department"])) > 40 else str(row["department"])),
            html.Td(str(row["african_country"])),
            html.Td(posted_date),
            html.Td(str(row["response_date"])[:10] if pd.notna(row["response_date"]) else ""),
        ]))
    
    table = html.Table([
        html.Thead(html.Tr([
            html.Th("Title"),
            html.Th("Agency"),
            html.Th("Country"),
            html.Th("Posted Date"),
            html.Th("Response Due"),
        ])),
        html.Tbody(table_rows),
    ], className="opportunities-table")
    
    return country_fig, agency_fig, timeline_fig, table


# --------------------------------------------------------------- #
# BACKGROUND SCHEDULER                                           #
# --------------------------------------------------------------- #

def run_scheduler():
    """Background scheduler for automatic data updates."""
    schedule.every(UPDATE_INTERVAL_HOURS).hours.do(update_africa_data)
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except Exception as e:
            print(f"[ERROR] Scheduler error: {e}")
            time.sleep(300)  # Wait 5 minutes on error

# Start background scheduler
threading.Thread(target=run_scheduler, daemon=True).start()

# --------------------------------------------------------------- #
# MAIN ENTRY POINT                                              #
# --------------------------------------------------------------- #

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
