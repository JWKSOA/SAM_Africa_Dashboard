import threading
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import schedule
from dash import Input, Output, dcc, html, dash_table, callback_context

from config import DASHBOARD_TITLE, UPDATE_INTERVAL_HOURS
from sam_api import EnhancedSAMAfricaAPI, update_comprehensive_africa_data, create_enhanced_sample_data

# --------------------------------------------------------------- #
# ENHANCED PRODUCTION DATA LOADING                               #
# --------------------------------------------------------------- #

sam_api = EnhancedSAMAfricaAPI()

def load_all_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load both active and historical data."""
    try:
        # Load all data from database
        all_df = sam_api.load_from_database(active_only=False)
        
        if all_df.empty:
            print("[INFO] No existing data found. Creating sample data...")
            all_df = create_enhanced_sample_data()
        
        # Split into active and historical
        active_df = all_df[all_df['is_active'] == 1].copy() if not all_df.empty else pd.DataFrame()
        historical_df = all_df[all_df['is_active'] == 0].copy() if not all_df.empty else pd.DataFrame()
        
        print(f"[INFO] Loaded {len(active_df)} active and {len(historical_df)} historical opportunities")
        return active_df, historical_df
        
    except Exception as exc:
        print(f"[ERROR] Critical error in load_all_data: {exc}")
        sample_df = create_enhanced_sample_data()
        active_df = sample_df[sample_df['is_active'] == 1].copy()
        historical_df = sample_df[sample_df['is_active'] == 0].copy()
        return active_df, historical_df

# Initialize data
active_opportunities_df, historical_opportunities_df = load_all_data()

# --------------------------------------------------------------- #
# ENHANCED DASH APP SETUP                                       #
# --------------------------------------------------------------- #

app = dash.Dash(__name__)
app.title = DASHBOARD_TITLE
server = app.server

# --------------------------------------------------------------- #
# ENHANCED DASHBOARD LAYOUT                                     #
# --------------------------------------------------------------- #

app.layout = html.Div([
    # Enhanced Header Section
    html.Div([
        html.H1(DASHBOARD_TITLE, className="header-title"),
        html.P(
            f"Complete Historical Database | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            className="last-updated",
        ),
        html.Div([
            html.Button("Refresh Current Data", id="refresh-btn", className="refresh-button"),
            html.Button("Full Historical Sync", id="historical-sync-btn", className="historical-button"),
        ], className="button-container"),
        html.Div(id="sync-status", className="sync-status"),
    ], className="header"),
    
    # Main Content
    html.Div([
        # Enhanced Statistics Cards
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
                html.H3(id="historical-opportunities", className="stat-number"),
                html.P("Historical Opportunities", className="stat-label"),
            ], className="stat-card"),
            
            html.Div([
                html.H3(id="countries-count", className="stat-number"),
                html.P("African Countries", className="stat-label"),
            ], className="stat-card"),
            
            html.Div([
                html.H3(id="agencies-count", className="stat-number"),
                html.P("Agencies", className="stat-label"),
            ], className="stat-card"),
            
            html.Div([
                html.H3(id="total-value", className="stat-number"),
                html.P("Total Contract Value", className="stat-label"),
            ], className="stat-card"),
        ], className="stats-container"),
        
        # Enhanced Filter Controls
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
            
            html.Div([
                html.Label("Status:", className="filter-label"),
                dcc.Dropdown(
                    id="status-filter",
                    options=[
                        {"label": "Active Only", "value": "active"},
                        {"label": "Historical Only", "value": "historical"},
                        {"label": "All Opportunities", "value": "all"}
                    ],
                    value="active",
                    placeholder="Select status…"
                ),
            ], className="filter-item"),
        ], className="filters-container"),
        
        # Enhanced Charts Row 1
        html.Div([
            html.Div([dcc.Graph(id="country-chart")], className="chart-container"),
            html.Div([dcc.Graph(id="agency-chart")], className="chart-container"),
        ], className="charts-row"),
        
        # Enhanced Charts Row 2
        html.Div([
            html.Div([dcc.Graph(id="timeline-chart")], className="chart-container"),
            html.Div([dcc.Graph(id="value-chart")], className="chart-container"),
        ], className="charts-row"),
        
        # Tabbed Tables Section
        html.Div([
            dcc.Tabs(id="tables-tabs", value="recent", children=[
                dcc.Tab(label="Recent/Active Opportunities", value="recent"),
                dcc.Tab(label="Historical Database", value="historical"),
            ]),
            html.Div(id="tables-content"),
        ], className="tables-container"),
        
    ], className="main-content"),
])

# --------------------------------------------------------------- #
# ENHANCED CALLBACK FUNCTIONS                                   #
# --------------------------------------------------------------- #

@app.callback(
    [
        Output("total-opportunities", "children"),
        Output("active-opportunities", "children"),
        Output("historical-opportunities", "children"),
        Output("countries-count", "children"),
        Output("agencies-count", "children"),
        Output("total-value", "children"),
        Output("country-filter", "options"),
        Output("agency-filter", "options"),
        Output("type-filter", "options"),
        Output("sync-status", "children"),
    ],
    [
        Input("refresh-btn", "n_clicks"),
        Input("historical-sync-btn", "n_clicks"),
    ],
)
def update_stats_and_filters(refresh_clicks: Optional[int], sync_clicks: Optional[int]):
    """Update dashboard statistics and filter options with enhanced features."""
    global active_opportunities_df, historical_opportunities_df
    
    ctx = callback_context
    status_message = ""
    
    if ctx.triggered:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == "refresh-btn" and refresh_clicks and refresh_clicks > 0:
            print("[INFO] Manual refresh triggered")
            status_message = "Refreshing current data..."
            try:
                active_opportunities_df, historical_opportunities_df = load_all_data()
                status_message = "✅ Current data refreshed successfully!"
            except Exception as e:
                print(f"[ERROR] Refresh failed: {e}")
                status_message = "❌ Refresh failed. Using cached data."
        
        elif button_id == "historical-sync-btn" and sync_clicks and sync_clicks > 0:
            print("[INFO] Historical sync triggered")
            status_message = "🔄 Starting comprehensive historical data collection... This may take several minutes."
            try:
                update_comprehensive_africa_data()
                active_opportunities_df, historical_opportunities_df = load_all_data()
                status_message = f"✅ Historical sync complete! Collected {len(active_opportunities_df) + len(historical_opportunities_df)} total opportunities."
            except Exception as e:
                print(f"[ERROR] Historical sync failed: {e}")
                status_message = "❌ Historical sync failed. Please try again later."
    
    # Combine dataframes for statistics
    all_df = pd.concat([active_opportunities_df, historical_opportunities_df], ignore_index=True)
    
    if all_df.empty:
        return "0", "0", "0", "0", "0", "$0", [], [], [], status_message
    
    # Calculate enhanced statistics
    total_opps = len(all_df)
    active_opps = len(active_opportunities_df)
    historical_opps = len(historical_opportunities_df)
    
    # Calculate total contract value (parse award amounts)
    total_value = 0
    for amount_str in all_df['award_amount'].dropna():
        amount_str = str(amount_str).replace('$', '').replace(',', '')
        try:
            if amount_str and amount_str != 'nan' and amount_str != '':
                total_value += float(amount_str)
        except (ValueError, TypeError):
            continue
    
    total_value_str = f"${total_value:,.0f}" if total_value > 0 else "Not Available"
    
    # Unique values for filters
    countries = sorted([c for c in all_df["african_country"].dropna().unique() if c != "nan"])
    agencies = sorted([a for a in all_df["department"].dropna().unique() if a != "nan"])
    notice_types = sorted([n for n in all_df["notice_type"].dropna().unique() if n != "nan"])
    
    return (
        f"{total_opps:,}",
        f"{active_opps:,}",
        f"{historical_opps:,}",
        f"{len(countries):,}",
        f"{len(agencies):,}",
        total_value_str,
        [{"label": c, "value": c} for c in countries],
        [{"label": a, "value": a} for a in agencies],
        [{"label": t, "value": t} for t in notice_types],
        status_message,
    )


@app.callback(
    [
        Output("country-chart", "figure"),
        Output("agency-chart", "figure"),
        Output("timeline-chart", "figure"),
        Output("value-chart", "figure"),
    ],
    [
        Input("country-filter", "value"),
        Input("agency-filter", "value"),
        Input("type-filter", "value"),
        Input("status-filter", "value"),
    ],
)
def update_charts(selected_countries, selected_agencies, selected_types, status_filter):
    """Update all charts based on filter selections."""
    global active_opportunities_df, historical_opportunities_df
    
    # Select data based on status filter
    if status_filter == "active":
        df = active_opportunities_df.copy()
    elif status_filter == "historical":
        df = historical_opportunities_df.copy()
    else:  # "all"
        df = pd.concat([active_opportunities_df, historical_opportunities_df], ignore_index=True)
    
    # Apply filters
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
        return empty_fig, empty_fig, empty_fig, empty_fig
    
    # Enhanced Country Chart
    country_counts = df["african_country"].value_counts().head(15)
    country_fig = px.bar(
        x=country_counts.values,
        y=country_counts.index,
        orientation="h",
        title="Opportunities by African Country (Top 15)",
        labels={"x": "Number of Opportunities", "y": "Country"},
        color=country_counts.values,
        color_continuous_scale="Blues"
    )
    country_fig.update_layout(height=500, showlegend=False)
    
    # Enhanced Agency Chart
    agency_counts = df["department"].value_counts().head(10)
    agency_fig = px.pie(
        values=agency_counts.values,
        names=[name[:35] + "..." if len(name) > 35 else name for name in agency_counts.index],
        title="Opportunities by Agency (Top 10)",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    agency_fig.update_layout(height=500)
    
    # Enhanced Timeline Chart
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce")
    if not df["posted_date"].isna().all():
        df_timeline = df.dropna(subset=['posted_date'])
        timeline_df = df_timeline.groupby([df_timeline["posted_date"].dt.to_period("M"), "is_active"]).size().reset_index(name="count")
        timeline_df["posted_date"] = timeline_df["posted_date"].astype(str)
        timeline_df["status"] = timeline_df["is_active"].map({1: "Active", 0: "Historical"})
        
        timeline_fig = px.line(
            timeline_df,
            x="posted_date",
            y="count",
            color="status",
            title="Opportunities Posted Over Time (Monthly)",
            labels={"posted_date": "Month", "count": "Number of Opportunities"}
        )
        timeline_fig.update_layout(height=400)
    else:
        timeline_fig = go.Figure().add_annotation(
            text="No timeline data available",
            x=0.5, y=0.5,
            showarrow=False
        )
        timeline_fig.update_layout(height=400)
    
    # New: Contract Value Chart
    df_with_values = df[df['award_amount'].notna() & (df['award_amount'] != '') & (df['award_amount'] != 'nan')].copy()
    if not df_with_values.empty:
        # Parse award amounts
        df_with_values['award_value'] = 0
        for idx, row in df_with_values.iterrows():
            amount_str = str(row['award_amount']).replace('$', '').replace(',', '')
            try:
                df_with_values.at[idx, 'award_value'] = float(amount_str)
            except (ValueError, TypeError):
                df_with_values.at[idx, 'award_value'] = 0
        
        df_with_values = df_with_values[df_with_values['award_value'] > 0]
        
        if not df_with_values.empty:
            value_by_country = df_with_values.groupby('african_country')['award_value'].sum().sort_values(ascending=False).head(10)
            value_fig = px.bar(
                x=value_by_country.index,
                y=value_by_country.values,
                title="Total Contract Value by Country (Top 10)",
                labels={"x": "Country", "y": "Total Value ($)"}
            )
            value_fig.update_layout(height=400, xaxis_tickangle=-45)
        else:
            value_fig = go.Figure().add_annotation(
                text="No contract value data available",
                x=0.5, y=0.5,
                showarrow=False
            )
            value_fig.update_layout(height=400)
    else:
        value_fig = go.Figure().add_annotation(
            text="No contract value data available",
            x=0.5, y=0.5,
            showarrow=False
        )
        value_fig.update_layout(height=400)
    
    return country_fig, agency_fig, timeline_fig, value_fig


@app.callback(
    Output("tables-content", "children"),
    [
        Input("tables-tabs", "value"),
        Input("country-filter", "value"),
        Input("agency-filter", "value"),
        Input("type-filter", "value"),
    ],
)
def update_tables(active_tab, selected_countries, selected_agencies, selected_types):
    """Update tables based on tab selection and filters."""
    global active_opportunities_df, historical_opportunities_df
    
    # Select appropriate dataframe
    if active_tab == "recent":
        df = active_opportunities_df.copy()
        title = "Recent/Active Opportunities"
    else:
        df = historical_opportunities_df.copy()
        title = "Historical Opportunities Database"
    
    # Apply filters
    if selected_countries:
        df = df[df["african_country"].isin(selected_countries)]
    if selected_agencies:
        df = df[df["department"].isin(selected_agencies)]
    if selected_types:
        df = df[df["notice_type"].isin(selected_types)]
    
    if df.empty:
        return html.Div([
            html.H3(title, className="section-title"),
            html.P("No opportunities match the selected filters.", className="no-data-message")
        ])
    
    # Prepare table data with clickable links
    table_df = df[
        ["title", "department", "african_country", "posted_date", "response_date", "award_amount", "sam_url"]
    ].head(100).copy()  # Increased to show more data
    
    # Create enhanced table
    table_data = []
    for _, row in table_df.iterrows():
        # Truncate long titles but keep full title for hover
        title = str(row["title"])
        display_title = title[:100] + "..." if len(title) > 100 else title
        
        # Format dates
        posted_date = ""
        if pd.notna(row["posted_date"]):
            try:
                posted_date = pd.to_datetime(row["posted_date"]).strftime("%Y-%m-%d")
            except:
                posted_date = str(row["posted_date"])[:10]
        
        response_date = ""
        if pd.notna(row["response_date"]):
            try:
                response_date = pd.to_datetime(row["response_date"]).strftime("%Y-%m-%d")
            except:
                response_date = str(row["response_date"])[:10]
        
        # Format award amount
        award_amount = str(row["award_amount"]) if pd.notna(row["award_amount"]) and str(row["award_amount"]) != "nan" else "Not Disclosed"
        
        # Create SAM.gov link
        sam_url = str(row["sam_url"]) if pd.notna(row["sam_url"]) else ""
        
        table_data.append({
            "Title": display_title,
            "Agency": str(row["department"])[:50] + "..." if len(str(row["department"])) > 50 else str(row["department"]),
            "Country": str(row["african_country"]),
            "Posted Date": posted_date,
            "Response Due": response_date,
            "Award Amount": award_amount,
            "SAM.gov Link": sam_url
        })
    
    # Create enhanced table with clickable links
    columns = [
        {"name": "Title", "id": "Title"},
        {"name": "Agency", "id": "Agency"},
        {"name": "Country", "id": "Country"},
        {"name": "Posted Date", "id": "Posted Date"},
        {"name": "Response Due", "id": "Response Due"},
        {"name": "Award Amount", "id": "Award Amount"},
        {"name": "SAM.gov Link", "id": "SAM.gov Link", "type": "text", "presentation": "markdown"},
    ]
    
    # Convert URLs to markdown links
    for row in table_data:
        if row["SAM.gov Link"] and row["SAM.gov Link"] != "":
            row["SAM.gov Link"] = f"[View Opportunity]({row['SAM.gov Link']})"
        else:
            row["SAM.gov Link"] = "N/A"
    
    enhanced_table = dash_table.DataTable(
        data=table_data,
        columns=columns,
        style_cell={
            'textAlign': 'left',
            'padding': '12px',
            'fontFamily': 'Arial',
            'fontSize': '14px',
        },
        style_header={
            'backgroundColor': '#f8f9fa',
            'fontWeight': 'bold',
            'border': '1px solid #dee2e6'
        },
        style_data={
            'border': '1px solid #dee2e6'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f8f9fa'
            }
        ],
        page_size=25,
        sort_action="native",
        filter_action="native",
        export_action="csv",
        export_format="csv",
    )
    
    return html.Div([
        html.H3(f"{title} ({len(df):,} total)", className="section-title"),
        enhanced_table
    ])


# --------------------------------------------------------------- #
# ENHANCED BACKGROUND SCHEDULER                                 #
# --------------------------------------------------------------- #

def run_enhanced_scheduler():
    """Enhanced background scheduler with comprehensive data updates."""
    schedule.every(UPDATE_INTERVAL_HOURS).hours.do(lambda: load_all_data())
    schedule.every().day.at("02:00").do(update_comprehensive_africa_data)  # Daily full sync at 2 AM
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except Exception as e:
            print(f"[ERROR] Scheduler error: {e}")
            time.sleep(300)  # Wait 5 minutes on error

# Start enhanced background scheduler
threading.Thread(target=run_enhanced_scheduler, daemon=True).start()

# --------------------------------------------------------------- #
# MAIN ENTRY POINT                                              #
# --------------------------------------------------------------- #

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)