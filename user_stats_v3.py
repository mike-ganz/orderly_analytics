import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
import pytz # Import pytz for timezone handling
import hashlib # Added for password hashing

# --- Password Configuration ---
# IMPORTANT: You MUST replace the placeholder hash below with your own.
# To generate a SHA256 hash for your chosen password, run the following Python snippet
# (e.g., in a separate Python interpreter or a temporary script):
#
# import hashlib
# password_to_hash = "EXAMPLE_HERE"  # Replace with your desired password
# hashed_password = hashlib.sha256(password_to_hash.encode()).hexdigest()
# print(f"Your hashed password is: {hashed_password}")

# Then, replace the placeholder string below with the generated hash.
CORRECT_PASSWORD_HASH = "7203ca4c5e96fd5c631f3129ebb8106bd1769703f55e252bf84d0a28eff5dd2a" 
# Example for "password123": "ef92b778bafe771e89245b89ecea48c51405d74c71798ea84784d9a6dfc8370d"

def check_authentication():
    """Handles password input and updates session state."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "show_auth_success_message" not in st.session_state: # Initialize flag for success message
        st.session_state.show_auth_success_message = False

    if not st.session_state.authenticated: # Only show input if not yet authenticated
        st.sidebar.header("Dashboard Login")
        password_attempt = st.sidebar.text_input("Password", type="password", key="dashboard_password_input")

        if password_attempt:
            hashed_attempt = hashlib.sha256(password_attempt.encode()).hexdigest()
            if hashed_attempt == CORRECT_PASSWORD_HASH:
                st.session_state.authenticated = True
                st.session_state.show_auth_success_message = True # Set flag to show message on next rerun
                # Use st.rerun() to immediately reflect the authenticated state and clear the input implicitly
                st.rerun()
            else:
                st.sidebar.error("Incorrect password. Please try again.")
                # Prevent further execution on this run if a wrong password was attempted
                st.stop() 
        # If no password attempt, and not authenticated, just wait for input.
        # The st.stop() below will prevent dashboard loading.
    
    return st.session_state.authenticated

# --- Main Application Logic ---
# Perform authentication check.
# The check_authentication function will call st.stop() internally on a failed attempt
# or if no password has been entered yet and not authenticated.
if not check_authentication():
    # This part is reached if no password has been entered yet and the user is not authenticated.
    # For the very first run or if the password field is empty.
    st.warning("Please enter the password in the sidebar to access the dashboard.")
    st.stop()


# If authenticated, show a success message in the sidebar ONCE and proceed with the app.
if st.session_state.get("show_auth_success_message", False):
    st.sidebar.success("Authenticated!")
    st.session_state.show_auth_success_message = False # Reset flag so it doesn't show again

# The rest of your Streamlit application code follows here.

# Set API key and headers
API_KEY = '29aa71bb-ce89-44df-8978-82c08473f05d'
HEADERS = {'Authorization': f'Bearer {API_KEY}'}

# Define the target timezone
TARGET_TZ = pytz.timezone('US/Eastern')

# API endpoints
events_url = 'https://xlw5-kd1n-crdj.n7c.xano.io/api:-VPGC53-/app_events'
users_url = 'https://xlw5-kd1n-crdj.n7c.xano.io/api:-VPGC53-/users'

@st.cache_data # Cache the data loading and initial processing
def load_data():
    # Query the endpoints with retry logic
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            events_response = requests.get(events_url, headers=HEADERS, timeout=30)
            users_response = requests.get(users_url, headers=HEADERS, timeout=30)
            
            # Check for successful responses
            if events_response.status_code == 200 and users_response.status_code == 200:
                events_data = events_response.json()
                users_data = users_response.json()

                events_df_initial = pd.DataFrame(events_data)
                users_df_initial = pd.DataFrame(users_data)

                # Convert timestamp fields to datetime, interpret as UTC, then convert to ET
                users_df_initial['created_at'] = pd.to_datetime(users_df_initial['created_at'], unit='ms', utc=True).dt.tz_convert(TARGET_TZ)
                events_df_initial['event_occurred_at'] = pd.to_datetime(events_df_initial['event_occurred_at'], unit='ms', utc=True).dt.tz_convert(TARGET_TZ)
                # Convert deleted_date later as it needs error handling
                return users_df_initial, events_df_initial
            else:
                st.error(f"Failed to retrieve data. Status codes: {events_response.status_code}, {users_response.status_code}")
                return pd.DataFrame(), pd.DataFrame() # Return empty DataFrames on error
                
        except (requests.exceptions.ChunkedEncodingError, 
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException) as e:
            
            if attempt < max_retries - 1:  # Not the last attempt
                st.warning(f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
                continue
            else:  # Last attempt failed
                st.error(f"Failed to retrieve data after {max_retries} attempts. Error: {str(e)}")
                st.error("Please check your internet connection and try refreshing the page.")
                return pd.DataFrame(), pd.DataFrame() # Return empty DataFrames on error

# Load the data using the cached function
users_df_initial, events_df_initial = load_data()

# Check if dataframes are empty (error in loading)
if users_df_initial.empty or events_df_initial.empty:
    st.stop() # Stop execution if data loading failed

# --- Timezone-aware defaults for sidebar ---
now_et = datetime.now(TARGET_TZ)

# Sidebar controls
st.sidebar.header("Controls")
# Use .date() to pass only the date part to date_input
default_start_date = (now_et - pd.Timedelta(weeks=12)).date()
default_end_date = now_et.date()
start_date = st.sidebar.date_input("Start date", default_start_date)
end_date = st.sidebar.date_input("End date", default_end_date)
aggregation = st.sidebar.selectbox("Aggregation", ['Daily', 'Weekly', 'Monthly'], index=0)  # Default to Daily

# Chart Controls
st.sidebar.subheader("Dashboard Controls")
st.sidebar.write("📊 Use the date range and aggregation controls above to customize all visualizations.")

# Filter dataframes using timezone-naive dates from sidebar
# Convert sidebar dates to timezone-aware datetime objects for comparison
start_dt = TARGET_TZ.localize(datetime.combine(start_date, datetime.min.time()))
end_dt = TARGET_TZ.localize(datetime.combine(end_date, datetime.max.time()))

# Create copies of the initial dataframes for filtering
users_df = users_df_initial.copy()
events_df = events_df_initial.copy()

users_df = users_df[(users_df['created_at'] >= start_dt) & (users_df['created_at'] <= end_dt)]
events_df = events_df[(events_df['event_occurred_at'] >= start_dt) & (events_df['event_occurred_at'] <= end_dt)]

# Define resample frequency
freq = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'M'}[aggregation]

# --------------------------
# REDESIGNED TOP SECTION: Overview Dashboard
# --------------------------

# Calculate key metrics for overview cards
now_et = datetime.now(TARGET_TZ)
current_period_start = now_et - pd.Timedelta(days=30)  # Last 30 days
previous_period_start = now_et - pd.Timedelta(days=60)
previous_period_end = now_et - pd.Timedelta(days=30)

# Current period metrics
current_new_users = len(users_df[users_df['created_at'] >= current_period_start])
current_active_users = events_df[events_df['event_occurred_at'] >= current_period_start]['user_id'].nunique()

# Previous period metrics for comparison
previous_new_users = len(users_df[
    (users_df['created_at'] >= previous_period_start) & 
    (users_df['created_at'] < previous_period_end)
])
previous_active_users = events_df[
    (events_df['event_occurred_at'] >= previous_period_start) & 
    (events_df['event_occurred_at'] < previous_period_end)
]['user_id'].nunique()

# Calculate growth rates
new_users_growth = ((current_new_users - previous_new_users) / previous_new_users * 100) if previous_new_users > 0 else 0
active_users_growth = ((current_active_users - previous_active_users) / previous_active_users * 100) if previous_active_users > 0 else 0

# Additional key metrics
total_users = len(users_df_initial)
total_events = len(events_df_initial)
avg_events_per_user = total_events / total_users if total_users > 0 else 0

# Users with complete profiles (both token and oauth)
try:
    complete_profiles = len(users_df[
    (users_df['expo_push_token'].notna() & (users_df['expo_push_token'] != '')) &
    (users_df['oauth_options'].notna() & (users_df['oauth_options'].astype(str) != '[]'))
    ])
except:
    # Fallback calculation if there are issues with the oauth_options field
    users_with_token = users_df[users_df['expo_push_token'].notna() & (users_df['expo_push_token'] != '')]
    complete_profiles = 0
    for _, row in users_with_token.iterrows():
        try:
            oauth_val = row['oauth_options']
            if (pd.notna(oauth_val) and 
                oauth_val is not None and 
                str(oauth_val) != '[]' and 
                str(oauth_val) != 'nan'):
                complete_profiles += 1
        except:
            continue
profile_completion_rate = (complete_profiles / total_users * 100) if total_users > 0 else 0

# Create overview dashboard
st.header("📊 Dashboard Overview")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="Total Users",
        value=f"{total_users:,}",
        help="All-time registered users"
    )

with col2:
    st.metric(
        label="New Users (30d)",
        value=f"{current_new_users:,}",
        delta=f"{new_users_growth:+.1f}%" if new_users_growth != 0 else None,
        help="New users in last 30 days vs previous 30 days"
    )

with col3:
    st.metric(
        label="Active Users (30d)",
        value=f"{current_active_users:,}",
        delta=f"{active_users_growth:+.1f}%" if active_users_growth != 0 else None,
        help="Active users in last 30 days vs previous 30 days"
    )

with col4:
    st.metric(
        label="Avg Events/User",
        value=f"{avg_events_per_user:.1f}",
        help="Average events per user (all-time)"
    )

with col5:
    st.metric(
        label="Profile Completion",
        value=f"{profile_completion_rate:.1f}%",
        help="Users with both push token and OAuth setup"
    )

st.markdown("---")

# --------------------------
# REDESIGNED: User Acquisition Analysis
# --------------------------

st.header("🚀 User Acquisition Analysis")

# Aggregate total new users
total_new_users = users_df.set_index('created_at').resample(freq).size()

# Categorize users by setup completeness
def categorize_user_setup(row):
    # Handle push token check
    try:
        has_token = pd.notna(row['expo_push_token']) and row['expo_push_token'] != ''
    except:
        has_token = False
    
    # Handle oauth options check - be more careful with array-like objects
    try:
        oauth_val = row['oauth_options']
        has_oauth = (pd.notna(oauth_val) and 
                    oauth_val is not None and 
                    str(oauth_val) != '[]' and 
                    str(oauth_val) != 'nan')
    except:
        has_oauth = False
    
    if has_token and has_oauth:
        return 'Complete Setup'
    elif has_token:
        return 'Push Token Only'
    elif has_oauth:
        return 'OAuth Only'
    else:
        return 'Minimal Setup'

users_df['setup_category'] = users_df.apply(categorize_user_setup, axis=1)

# Calculate setup categories over time
setup_categories = ['Complete Setup', 'Push Token Only', 'OAuth Only', 'Minimal Setup']
setup_colors = {
    'Complete Setup': '#2ca02c',      # Green - best
    'Push Token Only': '#ff7f0e',     # Orange
    'OAuth Only': '#1f77b4',          # Blue  
    'Minimal Setup': '#d62728'        # Red - needs attention
}

# Create stacked area chart for user acquisition by setup quality
fig_acquisition = go.Figure()

for category in setup_categories:
    category_users = users_df[users_df['setup_category'] == category]
    if not category_users.empty:
        category_counts = category_users.set_index('created_at').resample(freq).size()
        # Align with total_new_users index to handle missing periods
        category_counts = category_counts.reindex(total_new_users.index, fill_value=0)
        
        fig_acquisition.add_trace(go.Scatter(
            x=category_counts.index,
            y=category_counts,
        mode='lines', 
            name=category,
            line=dict(color=setup_colors[category]),
            stackgroup='one',
            hovertemplate=f'<b>{category}</b><br>Date: %{{x}}<br>New Users: %{{y}}<extra></extra>'
        ))

fig_acquisition.update_layout(
    title=f'{aggregation} New User Acquisition by Setup Completeness',
    xaxis_title='Date',
    yaxis_title='New Users',
    width=1400,
    height=550,
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        font=dict(size=10)
    ),
    hovermode='x unified'
)

st.plotly_chart(fig_acquisition)

# Add setup quality insights
setup_summary = users_df['setup_category'].value_counts()
col1, col2, col3, col4 = st.columns(4)

for i, category in enumerate(setup_categories):
    col = [col1, col2, col3, col4][i]
    count = setup_summary.get(category, 0)
    percentage = (count / len(users_df) * 100) if len(users_df) > 0 else 0
    
    with col:
        st.metric(
            label=category,
            value=f"{count:,}",
            delta=f"{percentage:.1f}%",
            help=f"{percentage:.1f}% of all users"
        )

# --------------------------
# REDESIGNED: Referral & Channel Performance
# --------------------------

st.header("🔄 Referral & Channel Performance")

# Filter out users with no referral source
users_df_referral = users_df[users_df['referral_source'].notna() & (users_df['referral_source'] != '')].copy()

if not users_df_referral.empty:
    # Create two visualizations: absolute counts and percentages
    
    # 1. Absolute referral counts over time as stacked bar chart
    referral_counts = users_df_referral.set_index('created_at').groupby('referral_source').resample(freq).size().unstack(level=0, fill_value=0)

    fig_referral_counts = go.Figure()
    
    # Get top referral sources for better color assignment
    top_sources = users_df_referral['referral_source'].value_counts().head(8).index
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
    
    for i, source in enumerate(referral_counts.columns):
        color = colors[i % len(colors)]
        fig_referral_counts.add_trace(go.Bar(
            x=referral_counts.index,
            y=referral_counts[source],
            name=str(source),
            marker_color=color,
            hovertemplate=f'<b>{source}</b><br>Date: %{{x}}<br>New Users: %{{y}}<extra></extra>'
        ))
    
    fig_referral_counts.update_layout(
        title=f'{aggregation} New Users by Referral Source',
        xaxis_title='Date',
        yaxis_title='New Users',
        width=1400,
        height=400,
        barmode='stack',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        ),
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_referral_counts)
    
    # 2. Referral source summary cards
    referral_summary = users_df_referral['referral_source'].value_counts()
    total_referred = len(users_df_referral)
    
    st.write("**Referral Source Performance Summary**")
    cols = st.columns(min(len(referral_summary), 5))  # Max 5 columns
    
    for i, (source, count) in enumerate(referral_summary.head(5).items()):
        col = cols[i]
        percentage = (count / total_referred * 100)
        with col:
            st.metric(
                label=str(source),
                value=f"{count:,}",
                delta=f"{percentage:.1f}%",
                help=f"{percentage:.1f}% of referred users"
            )
    
else:
    st.info(f"No users with referral sources found in the selected period.")

# --------------------------
# REDESIGNED: Engagement & Activity Analysis
# --------------------------

st.header("📱 User Engagement & Activity")

# Aggregate active users
daily_active_users = events_df.set_index('event_occurred_at').resample(freq)['user_id'].nunique().reset_index(name='active_users')

# Calculate engagement metrics
def calculate_engagement_metrics(events_df, users_df, freq):
    """Calculate comprehensive engagement metrics"""
    
    # Active users by time period
    active_users_series = events_df.set_index('event_occurred_at').resample(freq)['user_id'].nunique()
    
    # New users by time period (reindex to match active users)
    new_users_series = users_df.set_index('created_at').resample(freq).size()
    
    # Align series for calculations
    aligned_active, aligned_new = active_users_series.align(new_users_series, join='outer', fill_value=0)

    # Calculate returning users (Active - New), ensuring it's not negative
    returning_users = (aligned_active - aligned_new).clip(lower=0)

    return {
        'active_users': aligned_active,
        'new_users': aligned_new,
        'returning_users': returning_users
    }

engagement_metrics = calculate_engagement_metrics(events_df, users_df, freq)

# Create comprehensive engagement dashboard
fig_engagement = go.Figure()

# Add active users
fig_engagement.add_trace(go.Scatter(
    x=engagement_metrics['active_users'].index,
    y=engagement_metrics['active_users'],
    mode='lines+markers',
    name='Total Active Users',
    line=dict(color='#1f77b4', width=3),
    yaxis='y',
    hovertemplate='<b>Active Users</b><br>Date: %{x}<br>Count: %{y}<extra></extra>'
))

# Add returning users
fig_engagement.add_trace(go.Scatter(
    x=engagement_metrics['returning_users'].index,
    y=engagement_metrics['returning_users'],
    mode='lines+markers',
        name='Returning Users',
    line=dict(color='#ff7f0e', width=2),
    yaxis='y',
    hovertemplate='<b>Returning Users</b><br>Date: %{x}<br>Count: %{y}<extra></extra>'
))

# Add new users for context
fig_engagement.add_trace(go.Scatter(
    x=engagement_metrics['new_users'].index,
    y=engagement_metrics['new_users'],
    mode='lines+markers',
    name='New Users',
    line=dict(color='#2ca02c', width=2),
    yaxis='y',
    hovertemplate='<b>New Users</b><br>Date: %{x}<br>Count: %{y}<extra></extra>'
))

fig_engagement.update_layout(
    title=f'{aggregation} User Engagement Analysis',
    xaxis_title='Date',
    yaxis_title='User Count',
    width=1400,
    height=550,
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        font=dict(size=10)
    ),
    hovermode='x unified'
)

st.plotly_chart(fig_engagement)

# Engagement insights summary
latest_data = {
    'active_users': engagement_metrics['active_users'].iloc[-1] if len(engagement_metrics['active_users']) > 0 else 0,
    'returning_users': engagement_metrics['returning_users'].iloc[-1] if len(engagement_metrics['returning_users']) > 0 else 0,
    'new_users': engagement_metrics['new_users'].iloc[-1] if len(engagement_metrics['new_users']) > 0 else 0
}

retention_ratio = (latest_data['returning_users'] / latest_data['active_users'] * 100) if latest_data['active_users'] > 0 else 0
new_user_ratio = (latest_data['new_users'] / latest_data['active_users'] * 100) if latest_data['active_users'] > 0 else 0

st.write("**Latest Period Engagement Summary**")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(
        label="Active Users",
        value=f"{latest_data['active_users']:,.0f}",
        help=f"Users with events in latest {aggregation.lower()} period"
    )

with col2:
    st.metric(
        label="Returning Users",
        value=f"{latest_data['returning_users']:,.0f}",
        delta=f"{retention_ratio:.1f}%",
        help="Active users who are not new (retention indicator)"
    )

with col3:
    st.metric(
        label="New Users",
        value=f"{latest_data['new_users']:,.0f}",
        delta=f"{new_user_ratio:.1f}%",
        help=f"Users who signed up in latest {aggregation.lower()} period"
    )

# --------------------------
# New Chart: Active Users by Device Usage Pattern (Stacked Bar Chart)
# --------------------------
# Extract device information from usage_details field
def extract_device(usage_details):
    if pd.isna(usage_details) or usage_details == '':
        return 'Mobile'  # Default to Mobile if usage_details doesn't exist
    try:
        # Handle if usage_details is a string representation of JSON
        if isinstance(usage_details, str):
            import json
            usage_details = json.loads(usage_details)
        # Handle if usage_details is already a dict
        if isinstance(usage_details, dict):
            return usage_details.get('device', 'Mobile')
        else:
            return 'Mobile'
    except (json.JSONDecodeError, TypeError, AttributeError):
        return 'Mobile'  # Default to Mobile if parsing fails

# Use the exact same base calculation as the line chart, but add device grouping
# First, ensure usage_details column exists
events_df_temp = events_df.copy()
if 'usage_details' not in events_df_temp.columns:
    events_df_temp['usage_details'] = None

# Add device column
events_df_temp['device'] = events_df_temp['usage_details'].apply(extract_device)

# Use the same datetime index approach as the line chart
events_indexed = events_df_temp.set_index('event_occurred_at')

# Create meaningful categories that show users who used multiple devices vs single devices
def categorize_users_by_device_usage(df, frequency):
    # Create the appropriate time period grouper based on frequency
    if frequency == 'D':
        period_grouper = df.index.date
        period_name = 'date'
    elif frequency == 'W':
        period_grouper = df.index.to_period('W')
        period_name = 'week'
    elif frequency == 'M':
        period_grouper = df.index.to_period('M')
        period_name = 'month'
    else:
        period_grouper = df.index.date
        period_name = 'date'
    
    # For each user-period combination, get all devices they used
    period_user_devices = df.reset_index().groupby([period_grouper, 'user_id'])['device'].apply(set).reset_index()
    period_user_devices.columns = [period_name, 'user_id', 'device_set']
    
    # Categorize based on device usage patterns
    def get_device_category(device_set):
        devices = list(device_set)
        if len(devices) == 1:
            return devices[0] + " Only"
        else:
            return "Multiple Devices"
    
    period_user_devices['device_category'] = period_user_devices['device_set'].apply(get_device_category)
    
    # Convert period back to datetime for proper plotting
    if frequency in ['W', 'M']:
        period_user_devices[period_name] = period_user_devices[period_name].dt.start_time
    else:
        period_user_devices[period_name] = pd.to_datetime(period_user_devices[period_name])
    
    return period_user_devices

# Get device categories for each user per time period
user_device_categories = categorize_users_by_device_usage(events_indexed, freq)
time_column = 'date' if freq == 'D' else ('week' if freq == 'W' else 'month')

# Now group by time period and device category to get the correct counts
device_active_users = user_device_categories.set_index(time_column).groupby('device_category').resample(freq)['user_id'].nunique().unstack(level=0, fill_value=0)

# Create stacked bar chart
fig_device = go.Figure()

# Define colors for different device usage categories
device_colors = {
    'Mobile Only': '#1f77b4',      # Blue
    'Desktop Only': '#2ca02c',     # Green  
    'Multiple Devices': '#ff7f0e', # Orange
    'Tablet Only': '#d62728',      # Red
    'Web Only': '#9467bd',         # Purple
    'iOS Only': '#8c564b',         # Brown
    'Android Only': '#e377c2'     # Pink
}

# Add traces for each device category
for device_category in device_active_users.columns:
    fig_device.add_trace(go.Bar(
        x=device_active_users.index,
        y=device_active_users[device_category],
        name=device_category,
        marker_color=device_colors.get(device_category, '#17becf')  # Use default color if category not in predefined colors
    ))

# Set chart title based on aggregation level
chart_titles = {
    'Daily': 'Daily Active Users by Device Usage Pattern',
    'Weekly': 'Weekly Active Users by Device Usage Pattern', 
    'Monthly': 'Monthly Active Users by Device Usage Pattern'
}

fig_device.update_layout(
    title=chart_titles.get(aggregation, 'Active Users by Device Usage Pattern'),
    xaxis_title='Date',
    yaxis_title='Active Users',
    barmode='stack',
    width=1400,
    height=550,
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        font=dict(size=10)
    )
)
st.plotly_chart(fig_device)

# --------------------------
# REDESIGNED: Early Warning Indicators & User Quality
# --------------------------

st.header("⚠️ Early Warning Indicators & Enhanced User Behavior Analysis")

# Calculate comprehensive churn and enhanced behavior metrics
def calculate_user_behavior_metrics(users_df, events_df):
    """Calculate user behavior patterns and early warning indicators"""
    
    # Convert 'deleted_date' to datetime
    users_df['deleted_date'] = pd.to_datetime(users_df['deleted_date'], unit='ms', utc=True, errors='coerce').dt.tz_convert(TARGET_TZ)
    
    # Calculate days from signup to deletion
    users_df['days_to_delete'] = (users_df['deleted_date'] - users_df['created_at']).dt.days

    # Categorize users by early churn risk
    def get_churn_category(row):
        if pd.isna(row['deleted_date']):
            return 'Active'
        elif row['days_to_delete'] <= 1:
            return 'Immediate Churn (≤1 day)'
        elif row['days_to_delete'] <= 7:
            return 'Early Churn (2-7 days)'
        elif row['days_to_delete'] <= 30:
            return 'Short-term Churn (8-30 days)'
        else:
            return 'Long-term Churn (>30 days)'
    
    users_df['churn_category'] = users_df.apply(get_churn_category, axis=1)
    
    # Calculate user activation (has events)
    users_with_events = set(events_df['user_id'].unique())
    users_df['is_activated'] = users_df['id'].isin(users_with_events)
    
    # Calculate sophisticated usage intensity for each user
    user_usage_stats = {}
    now_dt = datetime.now(TARGET_TZ)
    
    for user_id in users_df['id'].unique():
        user_events = events_df[events_df['user_id'] == user_id]
        if not user_events.empty:
            total_events = len(user_events)
            days_active = user_events['event_occurred_at'].dt.date.nunique()
            
            # Last 30 days analysis
            thirty_days_ago = now_dt - pd.Timedelta(days=30)
            recent_events = user_events[user_events['event_occurred_at'] >= thirty_days_ago]
            recent_active_days = recent_events['event_occurred_at'].dt.date.nunique() if not recent_events.empty else 0
            
            # Last 3 months analysis (monthly active days)
            three_months_ago = now_dt - pd.Timedelta(days=90)
            last_3_months_events = user_events[user_events['event_occurred_at'] >= three_months_ago]
            
            monthly_active_days = []
            for month_offset in range(3):  # Last 3 months
                month_start = now_dt - pd.Timedelta(days=30*(month_offset+1))
                month_end = now_dt - pd.Timedelta(days=30*month_offset)
                month_events = last_3_months_events[
                    (last_3_months_events['event_occurred_at'] >= month_start) &
                    (last_3_months_events['event_occurred_at'] < month_end)
                ]
                month_days = month_events['event_occurred_at'].dt.date.nunique() if not month_events.empty else 0
                monthly_active_days.append(month_days)
            
            # Check if user has 4+ days of activity for each of the last 3 months
            consistent_monthly_usage = all(days >= 4 for days in monthly_active_days)
            
            user_usage_stats[user_id] = {
                'total_events': total_events,
                'days_active': days_active,
                'recent_active_days': recent_active_days,
                'monthly_active_days': monthly_active_days,
                'consistent_monthly_usage': consistent_monthly_usage,
                'events_per_active_day': total_events / days_active if days_active > 0 else 0
            }
        else:
            user_usage_stats[user_id] = {
                'total_events': 0,
                'days_active': 0,
                'recent_active_days': 0,
                'monthly_active_days': [0, 0, 0],
                'consistent_monthly_usage': False,
                'events_per_active_day': 0
            }
    
    # Add usage stats to users dataframe
    users_df['total_events'] = users_df['id'].map(lambda x: user_usage_stats[x]['total_events'])
    users_df['days_active'] = users_df['id'].map(lambda x: user_usage_stats[x]['days_active'])
    users_df['recent_active_days'] = users_df['id'].map(lambda x: user_usage_stats[x]['recent_active_days'])
    users_df['monthly_active_days'] = users_df['id'].map(lambda x: user_usage_stats[x]['monthly_active_days'])
    users_df['consistent_monthly_usage'] = users_df['id'].map(lambda x: user_usage_stats[x]['consistent_monthly_usage'])
    users_df['events_per_active_day'] = users_df['id'].map(lambda x: user_usage_stats[x]['events_per_active_day'])

    # Behavior score based on setup completeness and activation
    def calculate_behavior_score(row):
        score = 0
        # Push token (+25 points)
        try:
            if pd.notna(row['expo_push_token']) and row['expo_push_token'] != '':
                score += 25
        except:
            pass
        
        # OAuth setup (+25 points)
        try:
            oauth_val = row['oauth_options']
            if (pd.notna(oauth_val) and 
                oauth_val is not None and 
                str(oauth_val) != '[]' and 
                str(oauth_val) != 'nan'):
                score += 25
        except:
            pass
        
        # Has events (+50 points - most important)
        try:
            if row['is_activated']:
                score += 50
        except:
            pass
        
        return score
    
    users_df['behavior_score'] = users_df.apply(calculate_behavior_score, axis=1)
    
    # Enhanced behavior categorization with granular usage intensity
    def categorize_user_behavior_granular(row):
        base_score = row['behavior_score']
        consistent_monthly = row['consistent_monthly_usage']
        recent_active_days = row['recent_active_days']
        monthly_days = row['monthly_active_days']
        
        # Define granular usage intensity levels
        # High usage: 4+ days/month for 3 months OR 6+ days in last 30 days
        high_usage = consistent_monthly or recent_active_days >= 6
        
        # Medium usage: Some consistent usage but not high
        # 2+ days/month for at least 2 months OR 3-5 days in last 30 days
        medium_usage = (
            (sum(1 for days in monthly_days if days >= 2) >= 2) or 
            (3 <= recent_active_days <= 5)
        ) and not high_usage
        
        # Low usage: Everything else that has some activation
        low_usage = not high_usage and not medium_usage
        
        if base_score <= 25:
            return 'Dormant Users'
        elif base_score <= 50:
            return 'Trial Users'
        elif base_score <= 75:  # Engaged Users (51-75)
            if high_usage:
                return 'Engaged High Users'
            elif medium_usage:
                return 'Engaged Medium Users'
            else:
                return 'Engaged Low Users'
        else:  # Power Users (76-100)
            if high_usage:
                return 'Power High Users'
            elif medium_usage:
                return 'Power Medium Users'
            else:
                return 'Power Low Users'
    
    users_df['behavior_category_enhanced'] = users_df.apply(categorize_user_behavior_granular, axis=1)
    
    return users_df

# Calculate behavior metrics
users_behavior_df = calculate_user_behavior_metrics(users_df.copy(), events_df)

# 1. Churn Analysis by Time Period
churn_over_time = users_behavior_df[users_behavior_df['churn_category'] != 'Active'].set_index('created_at').groupby('churn_category').resample(freq).size().unstack(level=0, fill_value=0)

if not churn_over_time.empty:
    fig_churn = go.Figure()
    
    churn_colors = {
        'Immediate Churn (≤1 day)': '#d62728',     # Red - critical
        'Early Churn (2-7 days)': '#ff7f0e',       # Orange - warning
        'Short-term Churn (8-30 days)': '#ffbb78', # Light orange
        'Long-term Churn (>30 days)': '#2ca02c'    # Green - natural churn
    }
    
    for category in churn_over_time.columns:
        if category in churn_colors:
            fig_churn.add_trace(go.Scatter(
                x=churn_over_time.index,
                y=churn_over_time[category],
                mode='lines+markers',
        name=category,
                line=dict(color=churn_colors[category], width=2),
                stackgroup='one',
                hovertemplate=f'<b>{category}</b><br>Date: %{{x}}<br>Churned Users: %{{y}}<extra></extra>'
    ))

    fig_churn.update_layout(
        title=f'{aggregation} User Churn Analysis by Time to Churn',
    xaxis_title='Date',
        yaxis_title='Churned Users',
        width=1400,
        height=400,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        ),
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_churn)

# 2. Enhanced User Behavior Distribution
behavior_distribution = users_behavior_df['behavior_category_enhanced'].value_counts()

fig_behavior = go.Figure()

behavior_colors = {
    'Dormant Users': '#d62728',           # Red - concerning
    'Trial Users': '#ff7f0e',             # Orange - needs attention  
    'Engaged Low Users': '#ffcc99',       # Light orange - basic usage
    'Engaged Medium Users': '#ffa500',    # Orange - decent usage
    'Engaged High Users': '#2ca02c',      # Green - good usage
    'Power Low Users': '#b0c4de',         # Light steel blue - setup but low usage
    'Power Medium Users': '#4169e1',      # Royal blue - good setup + usage
    'Power High Users': '#1f77b4'         # Dark blue - excellent
}

# Ensure consistent ordering for the chart (progression from worst to best)
category_order = ['Dormant Users', 'Trial Users', 'Engaged Low Users', 'Engaged Medium Users', 'Engaged High Users', 'Power Low Users', 'Power Medium Users', 'Power High Users']
ordered_distribution = behavior_distribution.reindex(category_order, fill_value=0)

# Create detailed hover definitions for each category
hover_definitions = {
    'Dormant Users': 'Minimal setup, no meaningful usage<br>• Score: 0-25 points<br>• Little to no app engagement<br>• May have basic signup only',
    'Trial Users': 'Some setup or basic usage, still exploring<br>• Score: 26-50 points<br>• Either some activation OR some setup<br>• Testing the platform',
    'Engaged Low Users': 'Good setup but LOW usage intensity<br>• Score: 51-75 points<br>• Has activation + some setup<br>• Usage: <2 days/month consistently<br>• AND <3 days in last 30 days',
    'Engaged Medium Users': 'Good setup + MEDIUM usage intensity<br>• Score: 51-75 points<br>• Has activation + some setup<br>• Usage: 2+ days/month for 2+ months<br>• OR 3-5 days in last 30 days',
    'Engaged High Users': 'Good setup + HIGH usage intensity<br>• Score: 51-75 points<br>• Has activation + some setup<br>• Usage: 4+ days/month for 3 months<br>• OR 6+ days in last 30 days',
    'Power Low Users': 'Complete setup but LOW usage (optimization opportunity!)<br>• Score: 76-100 points<br>• Has activation + push token + OAuth<br>• Usage: <2 days/month consistently<br>• AND <3 days in last 30 days',
    'Power Medium Users': 'Complete setup + MEDIUM usage<br>• Score: 76-100 points<br>• Has activation + push token + OAuth<br>• Usage: 2+ days/month for 2+ months<br>• OR 3-5 days in last 30 days',
    'Power High Users': 'Complete setup + HIGH usage - TRUE CHAMPIONS!<br>• Score: 76-100 points<br>• Has activation + push token + OAuth<br>• Usage: 4+ days/month for 3 months<br>• OR 6+ days in last 30 days<br>• Most valuable user segment'
}

fig_behavior.add_trace(go.Bar(
    x=ordered_distribution.index,
    y=ordered_distribution.values,
    name='User Count',
    marker_color=[behavior_colors.get(cat, '#7f7f7f') for cat in ordered_distribution.index],
    text=[f'{count}<br>({count/len(users_behavior_df)*100:.1f}%)' for count in ordered_distribution.values],
    textposition='auto',
    hovertemplate='<b>%{x}</b><br>' +
                  'Count: %{y:,} users (%{customdata:.1f}%)<br><br>' +
                  '<i>Definition:</i><br>%{hovertext}<extra></extra>',
    customdata=[count/len(users_behavior_df)*100 for count in ordered_distribution.values],
    hovertext=[hover_definitions.get(cat, 'No definition available') for cat in ordered_distribution.index]
))

fig_behavior.update_layout(
    title='Granular User Behavior Distribution (Setup + Usage Consistency)',
    xaxis_title='User Behavior Category',
    yaxis_title='Number of Users',
    width=1400,
    height=500,
    showlegend=False,
    xaxis=dict(tickangle=45)
)

st.plotly_chart(fig_behavior)

# Enhanced behavior category explanations
st.write("**Granular User Behavior Categories:**")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.write("🔴 **Dormant Users**")
    st.write("Minimal setup, no meaningful usage")
    st.write("")
    st.write("🟠 **Trial Users**") 
    st.write("Some setup or basic usage, still exploring")

with col2:
    st.write("🟡 **Engaged Low Users**")
    st.write("Good setup but minimal usage")
    st.write("")
    st.write("🟠 **Engaged Medium Users**")
    st.write("Good setup + moderate usage")

with col3:
    st.write("🟢 **Engaged High Users**")
    st.write("Good setup + consistent high usage")
    st.write("")
    st.write("💙 **Power Low Users**")
    st.write("Complete setup but surprisingly low usage")

with col4:
    st.write("🔵 **Power Medium Users**")
    st.write("Complete setup + moderate usage")
    st.write("")
    st.write("🔵 **Power High Users**")
    st.write("Complete setup + high usage - TRUE CHAMPIONS!")

# Usage intensity criteria
st.write("**Usage Criteria:**")
st.write("• **High**: 4+ days/month for 3 months OR 6+ days in last 30 days")
st.write("• **Medium**: 2+ days/month for 2+ months OR 3-5 days in last 30 days")
st.write("• **Low**: Below medium criteria")

# Enhanced behavior insights
st.write("**Key Behavior Insights:**")
col1, col2, col3, col4 = st.columns(4)

# Calculate enhanced metrics
power_high_count = (users_behavior_df['behavior_category_enhanced'] == 'Power High Users').sum()
engaged_high_count = (users_behavior_df['behavior_category_enhanced'] == 'Engaged High Users').sum()
total_high_users = power_high_count + engaged_high_count

# All users with medium or high usage
high_medium_users = users_behavior_df['behavior_category_enhanced'].str.contains('High|Medium', na=False).sum()

# Users with good setup but low usage (optimization opportunity)
low_usage_with_setup = users_behavior_df['behavior_category_enhanced'].isin(['Engaged Low Users', 'Power Low Users']).sum()

with col1:
    st.metric(
        label="True Champions",
        value=f"{power_high_count:,}",
        delta=f"{power_high_count/len(users_behavior_df)*100:.1f}%",
        help="Power High Users - complete setup + high usage consistency"
    )

with col2:
    st.metric(
        label="High Usage Total",
        value=f"{total_high_users:,}",
        delta=f"{total_high_users/len(users_behavior_df)*100:.1f}%",
        help="Engaged High + Power High Users"
    )

with col3:
    st.metric(
        label="Active Users (Med/High)",
        value=f"{high_medium_users:,}",
        delta=f"{high_medium_users/len(users_behavior_df)*100:.1f}%",
        help="Users with medium or high usage intensity"
    )

with col4:
    st.metric(
        label="Setup But Low Usage",
        value=f"{low_usage_with_setup:,}",
        delta=f"{low_usage_with_setup/len(users_behavior_df)*100:.1f}%",
        help="Users with good setup but low usage - optimization opportunity"
    )

# 4. Traditional Risk Metrics Summary
st.write("**Risk & Activation Metrics:**")
col1, col2, col3, col4 = st.columns(4)

# Calculate key metrics
total_churned = len(users_behavior_df[users_behavior_df['churn_category'] != 'Active'])
immediate_churn_rate = len(users_behavior_df[users_behavior_df['churn_category'] == 'Immediate Churn (≤1 day)']) / len(users_behavior_df) * 100
activation_rate = users_behavior_df['is_activated'].sum() / len(users_behavior_df) * 100
avg_behavior_score = users_behavior_df['behavior_score'].mean()

with col1:
    st.metric(
        label="Total Churned Users",
        value=f"{total_churned:,}",
        help="Users who have been deleted from the system"
    )

with col2:
    st.metric(
        label="Immediate Churn Rate",
        value=f"{immediate_churn_rate:.1f}%",
        help="% of users deleted within 1 day of signup (critical metric)"
    )

with col3:
    st.metric(
        label="User Activation Rate",
        value=f"{activation_rate:.1f}%",
        help="% of users who have generated at least one event"
    )

with col4:
    st.metric(
        label="Avg Behavior Score",
        value=f"{avg_behavior_score:.0f}/100",
        help="Average user behavior score: 50pts for activation + 25pts each for push token & OAuth setup"
    )

st.markdown("---")
st.markdown("## 📈 **Advanced Analytics & Deep Insights**")
st.markdown("*The sections below provide detailed retention, engagement, and user behavior analysis.*")

# --------------------------
# New Chart: Retention Rates (Day 1, Day 7, Day 30)
# --------------------------

def calculate_retention_rates(users_df, events_df, target_tz):
    """Calculate retention rates based on first event date (activation date)"""
    now_et = datetime.now(target_tz)
    
    # Find first event date for each user (activation date)
    user_first_events = events_df.groupby('user_id')['event_occurred_at'].min().reset_index()
    user_first_events.columns = ['user_id', 'activation_date']
    
    # Merge with users data
    users_with_activation = users_df.merge(user_first_events, left_on='id', right_on='user_id', how='inner')
    
    # Only consider users who have at least one event
    if users_with_activation.empty:
        return pd.DataFrame()
    
    retention_data = []
    
    # Daily retention periods
    daily_periods = [1, 7, 30, 60, 90]
    for period_days in daily_periods:
        # Only consider users activated at least 'period_days' ago (denominator filter)
        # This ensures we only count users who have had enough time to potentially return
        cutoff_date = now_et - pd.Timedelta(days=period_days)
        eligible_users = users_with_activation[users_with_activation['activation_date'] <= cutoff_date].copy()
        
        if eligible_users.empty:
            retention_data.append({
                'period': f'Day {period_days}',
                'retention_rate': 0,
                'total_users': 0,
                'returned_users': 0,
                'type': 'daily'
            })
            continue
        
        # Calculate the target return date range for each user (from activation date)
        eligible_users['return_start'] = eligible_users['activation_date'] + pd.Timedelta(days=period_days)
        eligible_users['return_end'] = eligible_users['activation_date'] + pd.Timedelta(days=period_days + 1)
        
        # Count users who had activity within the return window
        returned_users = 0
        for _, user in eligible_users.iterrows():
            user_events = events_df[
                (events_df['user_id'] == user['user_id']) &
                (events_df['event_occurred_at'] >= user['return_start']) &
                (events_df['event_occurred_at'] < user['return_end'])
            ]
            if not user_events.empty:
                returned_users += 1
        
        retention_rate = (returned_users / len(eligible_users)) * 100 if len(eligible_users) > 0 else 0
        
        retention_data.append({
            'period': f'Day {period_days}',
            'retention_rate': retention_rate,
            'total_users': len(eligible_users),
            'returned_users': returned_users,
            'type': 'daily'
        })
    
    # Weekly retention periods
    weekly_periods = [1, 2, 4, 8, 12]  # weeks
    for period_weeks in weekly_periods:
        period_days = period_weeks * 7
        # Only consider users activated at least 'period_days' ago (denominator filter)
        # This ensures we only count users who have had enough time to potentially return
        cutoff_date = now_et - pd.Timedelta(days=period_days)
        eligible_users = users_with_activation[users_with_activation['activation_date'] <= cutoff_date].copy()
        
        if eligible_users.empty:
            retention_data.append({
                'period': f'Week {period_weeks}',
                'retention_rate': 0,
                'total_users': 0,
                'returned_users': 0,
                'type': 'weekly'
            })
            continue
        
        # Calculate the target return week range for each user
        eligible_users['return_week_start'] = eligible_users['activation_date'] + pd.Timedelta(days=period_days)
        eligible_users['return_week_end'] = eligible_users['activation_date'] + pd.Timedelta(days=period_days + 7)
        
        # Count users who had activity within the return week
        returned_users = 0
        for _, user in eligible_users.iterrows():
            user_events = events_df[
                (events_df['user_id'] == user['user_id']) &
                (events_df['event_occurred_at'] >= user['return_week_start']) &
                (events_df['event_occurred_at'] < user['return_week_end'])
            ]
            if not user_events.empty:
                returned_users += 1
        
        retention_rate = (returned_users / len(eligible_users)) * 100 if len(eligible_users) > 0 else 0
        
        retention_data.append({
            'period': f'Week {period_weeks}',
            'retention_rate': retention_rate,
            'total_users': len(eligible_users),
            'returned_users': returned_users,
            'type': 'weekly'
        })
    
    # Monthly retention periods
    monthly_periods = [1, 2, 3, 6, 12]  # months
    for period_months in monthly_periods:
        period_days = period_months * 30  # Approximate 30 days per month
        # Only consider users activated at least 'period_days' ago (denominator filter)
        # This ensures we only count users who have had enough time to potentially return
        cutoff_date = now_et - pd.Timedelta(days=period_days)
        eligible_users = users_with_activation[users_with_activation['activation_date'] <= cutoff_date].copy()
        
        if eligible_users.empty:
            retention_data.append({
                'period': f'Month {period_months}',
                'retention_rate': 0,
                'total_users': 0,
                'returned_users': 0,
                'type': 'monthly'
            })
            continue
        
        # Calculate the target return month range for each user (30-day window)
        eligible_users['return_month_start'] = eligible_users['activation_date'] + pd.Timedelta(days=period_days)
        eligible_users['return_month_end'] = eligible_users['activation_date'] + pd.Timedelta(days=period_days + 30)
        
        # Count users who had activity within the return month
        returned_users = 0
        for _, user in eligible_users.iterrows():
            user_events = events_df[
                (events_df['user_id'] == user['user_id']) &
                (events_df['event_occurred_at'] >= user['return_month_start']) &
                (events_df['event_occurred_at'] < user['return_month_end'])
            ]
            if not user_events.empty:
                returned_users += 1
        
        retention_rate = (returned_users / len(eligible_users)) * 100 if len(eligible_users) > 0 else 0
        
        retention_data.append({
            'period': f'Month {period_months}',
            'retention_rate': retention_rate,
            'total_users': len(eligible_users),
            'returned_users': returned_users,
            'type': 'monthly'
        })
    
    return pd.DataFrame(retention_data)

# Calculate retention rates
retention_df = calculate_retention_rates(users_df_initial, events_df_initial, TARGET_TZ)

if not retention_df.empty:
    # Separate daily, weekly, and monthly retention
    daily_retention = retention_df[retention_df['type'] == 'daily']
    weekly_retention = retention_df[retention_df['type'] == 'weekly']
    monthly_retention = retention_df[retention_df['type'] == 'monthly']
    
    # Create daily retention chart
    if not daily_retention.empty:
        fig_daily_retention = go.Figure()
        
        fig_daily_retention.add_trace(go.Bar(
            x=daily_retention['period'],
            y=daily_retention['retention_rate'],
            name='Daily Retention Rate',
            marker_color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd'],
            text=[f'{rate:.1f}%<br>({returned}/{total})' 
                  for rate, returned, total in zip(daily_retention['retention_rate'], 
                                                  daily_retention['returned_users'], 
                                                  daily_retention['total_users'])],
            textposition='auto'
        ))
        
        fig_daily_retention.update_layout(
            title='Daily Retention Rates (Based on First Event Date)',
            xaxis_title='Days After First Event',
            yaxis_title='Retention Rate (%)',
            width=1400,
            height=400,
            yaxis=dict(range=[0, max(100, daily_retention['retention_rate'].max() * 1.1) if not daily_retention.empty else 100]),
            showlegend=False
        )
        
        st.plotly_chart(fig_daily_retention)
    
    # Create weekly retention chart
    if not weekly_retention.empty:
        fig_weekly_retention = go.Figure()
        
        fig_weekly_retention.add_trace(go.Bar(
            x=weekly_retention['period'],
            y=weekly_retention['retention_rate'],
            name='Weekly Retention Rate',
            marker_color=['#9467bd', '#d62728', '#ff7f0e', '#8c564b', '#e377c2'],
            text=[f'{rate:.1f}%<br>({returned}/{total})' 
                  for rate, returned, total in zip(weekly_retention['retention_rate'], 
                                                  weekly_retention['returned_users'], 
                                                  weekly_retention['total_users'])],
            textposition='auto'
        ))
        
        fig_weekly_retention.update_layout(
            title='Weekly Retention Rates (Based on First Event Date)',
            xaxis_title='Weeks After First Event',
            yaxis_title='Retention Rate (%)',
            width=1400,
            height=400,
            yaxis=dict(range=[0, max(100, weekly_retention['retention_rate'].max() * 1.1) if not weekly_retention.empty else 100]),
            showlegend=False
        )
        
        st.plotly_chart(fig_weekly_retention)
    
    # Create monthly retention chart
    if not monthly_retention.empty:
        fig_monthly_retention = go.Figure()
        
        fig_monthly_retention.add_trace(go.Bar(
            x=monthly_retention['period'],
            y=monthly_retention['retention_rate'],
            name='Monthly Retention Rate',
            marker_color=['#17becf', '#bcbd22', '#ff7f0e', '#2ca02c', '#1f77b4'],
            text=[f'{rate:.1f}%<br>({returned}/{total})' 
                  for rate, returned, total in zip(monthly_retention['retention_rate'], 
                                                  monthly_retention['returned_users'], 
                                                  monthly_retention['total_users'])],
            textposition='auto'
        ))
        
        fig_monthly_retention.update_layout(
            title='Monthly Retention Rates (Based on First Event Date)',
            xaxis_title='Months After First Event',
            yaxis_title='Retention Rate (%)',
            width=1400,
            height=400,
            yaxis=dict(range=[0, max(100, monthly_retention['retention_rate'].max() * 1.1) if not monthly_retention.empty else 100]),
            showlegend=False
        )
        
        st.plotly_chart(fig_monthly_retention)
    
    # Display retention summary
    st.subheader("Retention Summary")
    
    # Daily retention summary
    if not daily_retention.empty:
        st.write("**Daily Retention (from first event)**")
        cols = st.columns(len(daily_retention))
        for i, (_, row) in enumerate(daily_retention.iterrows()):
            with cols[i]:
                st.metric(
                    label=f"{row['period']} Retention",
                    value=f"{row['retention_rate']:.1f}%",
                    delta=f"{row['returned_users']}/{row['total_users']} users"
                )
    
    # Weekly retention summary
    if not weekly_retention.empty:
        st.write("**Weekly Retention (from first event)**")
        cols = st.columns(len(weekly_retention))
        for i, (_, row) in enumerate(weekly_retention.iterrows()):
            with cols[i]:
                st.metric(
                    label=f"{row['period']} Retention",
                    value=f"{row['retention_rate']:.1f}%",
                    delta=f"{row['returned_users']}/{row['total_users']} users"
                )
    
    # Monthly retention summary
    if not monthly_retention.empty:
        st.write("**Monthly Retention (from first event)**")
        cols = st.columns(len(monthly_retention))
        for i, (_, row) in enumerate(monthly_retention.iterrows()):
            with cols[i]:
                st.metric(
                    label=f"{row['period']} Retention",
                    value=f"{row['retention_rate']:.1f}%",
                    delta=f"{row['returned_users']}/{row['total_users']} users"
                )
    
    # Add explanation
    st.info("📊 **Retention Calculation Method**: Retention is calculated from each user's first event date (activation date) rather than signup date. This provides more accurate retention metrics by only considering users who were actually being tracked from their activation point.")
else:
    st.warning("No retention data available. This requires users with multiple events over time.")

# --------------------------
# New Chart: Monthly Active Users (MAU) and Stickiness
# --------------------------

def calculate_mau_and_stickiness(events_df, target_tz):
    """Calculate MAU and DAU/MAU stickiness ratio"""
    now_et = datetime.now(target_tz)
    
    # Calculate MAU for the last 12 months
    mau_data = []
    
    for months_back in range(12):
        month_end = now_et.replace(day=1) - pd.Timedelta(days=1) if months_back == 0 else (now_et.replace(day=1) - pd.DateOffset(months=months_back)).replace(day=1) + pd.DateOffset(months=1) - pd.Timedelta(days=1)
        month_start = month_end.replace(day=1)
        
        # Get events for this month
        month_events = events_df[
            (events_df['event_occurred_at'] >= month_start) &
            (events_df['event_occurred_at'] <= month_end)
        ]
        
        mau = month_events['user_id'].nunique() if not month_events.empty else 0
        
        # Calculate average DAU for this month
        daily_active = month_events.set_index('event_occurred_at').resample('D')['user_id'].nunique()
        avg_dau = daily_active.mean() if not daily_active.empty else 0
        
        # Calculate stickiness (DAU/MAU ratio)
        stickiness = (avg_dau / mau) * 100 if mau > 0 else 0
        
        mau_data.append({
            'month': month_start,
            'mau': mau,
            'avg_dau': avg_dau,
            'stickiness': stickiness
        })
    
    return pd.DataFrame(mau_data).sort_values('month')

# Calculate MAU and stickiness
mau_df = calculate_mau_and_stickiness(events_df_initial, TARGET_TZ)

# Create MAU and stickiness chart
fig_mau = go.Figure()

# Add MAU trace
fig_mau.add_trace(go.Scatter(
    x=mau_df['month'],
    y=mau_df['mau'],
    mode='lines+markers',
    name='Monthly Active Users (MAU)',
    line=dict(color='#1f77b4'),
    yaxis='y'
))

# Add stickiness trace on secondary y-axis
fig_mau.add_trace(go.Scatter(
    x=mau_df['month'],
    y=mau_df['stickiness'],
    mode='lines+markers',
    name='Stickiness (DAU/MAU %)',
    line=dict(color='#ff7f0e'),
    yaxis='y2'
))

fig_mau.update_layout(
    title='Monthly Active Users (MAU) and User Stickiness',
    xaxis_title='Month',
    width=1400,
    height=550,
    yaxis=dict(
        title='Monthly Active Users',
        side='left'
    ),
    yaxis2=dict(
        title='Stickiness (%)',
        side='right',
        overlaying='y',
        range=[0, 100]
    ),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        font=dict(size=10)
    )
)

st.plotly_chart(fig_mau)

# Display MAU summary
current_mau = mau_df.iloc[-1] if not mau_df.empty else None
if current_mau is not None:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Current MAU", f"{current_mau['mau']:,.0f}")
    with col2:
        st.metric("Avg Daily Active Users", f"{current_mau['avg_dau']:.0f}")
    with col3:
        st.metric("User Stickiness", f"{current_mau['stickiness']:.1f}%")

# --------------------------
# New Chart: Session Analysis
# --------------------------

def calculate_session_metrics(events_df, target_tz):
    """Calculate session duration and frequency metrics"""
    # Sort events by user and timestamp
    events_sorted = events_df.sort_values(['user_id', 'event_occurred_at'])
    
    # Define session timeout (30 minutes of inactivity ends a session)
    session_timeout = pd.Timedelta(minutes=30)
    
    session_data = []
    
    for user_id in events_sorted['user_id'].unique():
        user_events = events_sorted[events_sorted['user_id'] == user_id].copy()
        
        if len(user_events) < 2:
            continue
        
        # Calculate time differences between consecutive events
        user_events['time_diff'] = user_events['event_occurred_at'].diff()
        
        # Start a new session when time gap > session_timeout
        user_events['new_session'] = user_events['time_diff'] > session_timeout
        user_events['session_id'] = user_events['new_session'].cumsum()
        
        # Calculate session metrics
        for session_id, session_events in user_events.groupby('session_id'):
            session_start = session_events['event_occurred_at'].min()
            session_end = session_events['event_occurred_at'].max()
            session_duration = (session_end - session_start).total_seconds() / 60  # in minutes
            event_count = len(session_events)
            
            session_data.append({
                'user_id': user_id,
                'session_date': session_start.date(),
                'session_duration_minutes': max(session_duration, 1),  # Minimum 1 minute
                'events_in_session': event_count
            })
    
    return pd.DataFrame(session_data)

# Calculate session metrics
session_df = calculate_session_metrics(events_df_initial, TARGET_TZ)

if not session_df.empty:
    # Filter sessions within date range
    session_df['session_date'] = pd.to_datetime(session_df['session_date'])
    # Convert start_date and end_date to datetime for comparison
    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date)
    session_df_filtered = session_df[
        (session_df['session_date'] >= start_datetime) & 
        (session_df['session_date'] <= end_datetime)
    ]
    
    if not session_df_filtered.empty:
        # Aggregate session metrics by date
        daily_session_metrics = session_df_filtered.groupby('session_date').agg({
            'session_duration_minutes': ['mean', 'median'],
            'events_in_session': 'mean',
            'user_id': 'nunique'  # Unique users with sessions
        }).round(2)
        
        daily_session_metrics.columns = ['avg_session_duration', 'median_session_duration', 
                                       'avg_events_per_session', 'users_with_sessions']
        daily_session_metrics = daily_session_metrics.reset_index()
        
        # Resample based on aggregation setting
        if aggregation != 'Daily':
            daily_session_metrics = daily_session_metrics.set_index('session_date')
            freq_map = {'Weekly': 'W', 'Monthly': 'M'}
            daily_session_metrics = daily_session_metrics.resample(freq_map[aggregation]).agg({
                'avg_session_duration': 'mean',
                'median_session_duration': 'mean',
                'avg_events_per_session': 'mean',
                'users_with_sessions': 'sum'
            }).round(2).reset_index()
        
        # Create session metrics chart
        fig_sessions = go.Figure()
        
        # Average session duration
        fig_sessions.add_trace(go.Scatter(
            x=daily_session_metrics['session_date'],
            y=daily_session_metrics['avg_session_duration'],
            mode='lines+markers',
            name='Avg Session Duration (min)',
            line=dict(color='#1f77b4'),
            yaxis='y'
        ))
        
        # Events per session on secondary axis
        fig_sessions.add_trace(go.Scatter(
            x=daily_session_metrics['session_date'],
            y=daily_session_metrics['avg_events_per_session'],
            mode='lines+markers',
            name='Avg Events per Session',
            line=dict(color='#ff7f0e'),
            yaxis='y2'
        ))
        
        fig_sessions.update_layout(
            title=f'{aggregation} Session Analysis',
            xaxis_title='Date',
            width=1400,
            height=550,
            yaxis=dict(
                title='Session Duration (minutes)',
                side='left'
            ),
            yaxis2=dict(
                title='Events per Session',
                side='right',
                overlaying='y'
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="center",
                x=0.5,
                font=dict(size=10)
            )
        )
        
        st.plotly_chart(fig_sessions)
        
        # Session summary metrics
        st.subheader("Session Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        latest_metrics = daily_session_metrics.iloc[-1] if not daily_session_metrics.empty else None
        overall_avg_duration = session_df_filtered['session_duration_minutes'].mean()
        overall_sessions_per_user = len(session_df_filtered) / session_df_filtered['user_id'].nunique()
        
        with col1:
            st.metric("Avg Session Duration", f"{overall_avg_duration:.1f} min")
        with col2:
            st.metric("Median Session Duration", f"{session_df_filtered['session_duration_minutes'].median():.1f} min")
        with col3:
            st.metric("Avg Sessions per User", f"{overall_sessions_per_user:.1f}")
        with col4:
            st.metric("Total Sessions", f"{len(session_df_filtered):,}")

# # --------------------------
# # New Chart: User Segmentation
# # --------------------------

# def segment_users_by_activity(users_df, events_df, session_df):
#     """Segment users based on activity patterns"""
#     user_segments = []
    
#     for user_id in users_df['id'].unique():
#         user_events = events_df[events_df['user_id'] == user_id]
#         user_sessions = session_df[session_df['user_id'] == user_id] if not session_df.empty else pd.DataFrame()
        
#         # Calculate user metrics
#         total_events = len(user_events)
#         days_active = user_events['event_occurred_at'].dt.date.nunique() if not user_events.empty else 0
#         total_sessions = len(user_sessions)
#         avg_session_duration = user_sessions['session_duration_minutes'].mean() if not user_sessions.empty else 0
        
#         # Get user creation date
#         user_created = users_df[users_df['id'] == user_id]['created_at'].iloc[0]
#         days_since_signup = (datetime.now(TARGET_TZ) - user_created).days
        
#         # Segmentation logic
#         if total_events == 0:
#             segment = 'Inactive'
#         elif days_active >= 7 and total_events >= 50:
#             segment = 'Power User'
#         elif days_active >= 3 and total_events >= 15:
#             segment = 'Regular User'
#         elif days_active >= 1 and total_events >= 5:
#             segment = 'Casual User'
#         elif days_since_signup <= 7:
#             segment = 'New User'
#         else:
#             segment = 'At Risk'
        
#         user_segments.append({
#             'user_id': user_id,
#             'segment': segment,
#             'total_events': total_events,
#             'days_active': days_active,
#             'total_sessions': total_sessions,
#             'avg_session_duration': avg_session_duration,
#             'days_since_signup': days_since_signup
#         })
    
#     return pd.DataFrame(user_segments)

# # Calculate user segments
# user_segments_df = segment_users_by_activity(users_df_initial, events_df_initial, session_df)

# # Create user segmentation chart
# segment_counts = user_segments_df['segment'].value_counts()

# fig_segments = go.Figure()

# # Define colors for segments
# segment_colors = {
#     'Power User': '#2ca02c',      # Green
#     'Regular User': '#1f77b4',    # Blue
#     'Casual User': '#ff7f0e',     # Orange
#     'New User': '#9467bd',        # Purple
#     'At Risk': '#d62728',         # Red
#     'Inactive': '#7f7f7f'         # Gray
# }

# fig_segments.add_trace(go.Bar(
#     x=segment_counts.values,
#     y=segment_counts.index,
#     orientation='h',
#     name='User Count',
#     marker_color=[segment_colors.get(segment, '#17becf') for segment in segment_counts.index],
#     text=[f'{count} ({count/len(user_segments_df)*100:.1f}%)' for count in segment_counts.values],
#     textposition='auto'
# ))

# fig_segments.update_layout(
#     title='User Segmentation by Activity Level',
#     xaxis_title='Number of Users',
#     yaxis_title='User Segment',
#     width=1400,
#     height=400,
#     showlegend=False
# )

# st.plotly_chart(fig_segments)

# # User segmentation summary
# st.subheader("User Segment Breakdown")
# cols = st.columns(len(segment_counts))

# for i, (segment, count) in enumerate(segment_counts.items()):
#     with cols[i]:
#         percentage = (count / len(user_segments_df)) * 100
#         st.metric(segment, f"{count:,}", f"{percentage:.1f}%")

# --------------------------
# New Chart: Cohort Analysis
# --------------------------

def create_cohort_analysis(users_df, events_df, target_tz):
    """Create cohort retention analysis based on activation date with monthly vintages starting March 2024"""
    # Find first event date for each user (activation date)
    user_first_events = events_df.groupby('user_id')['event_occurred_at'].min().reset_index()
    user_first_events.columns = ['user_id', 'activation_date']
    user_first_events['activation_month'] = user_first_events['activation_date'].dt.to_period('M')
    
    # Only consider users activated from March 2024 onwards
    march_2024 = pd.Period('2024-03', freq='M')
    user_first_events = user_first_events[user_first_events['activation_month'] >= march_2024]
    
    if user_first_events.empty:
        return pd.DataFrame(), pd.Series()
    
    # Get all unique months from events
    events_df_cohort = events_df.copy()
    events_df_cohort['event_month'] = events_df_cohort['event_occurred_at'].dt.to_period('M')
    
    # Create user-month activity matrix
    user_activity = events_df_cohort.groupby(['user_id', 'event_month']).size().reset_index(name='events')
    user_activity = user_activity.pivot_table(index='user_id', columns='event_month', values='events', fill_value=0)
    
    # Merge with user activation data
    cohort_data = user_first_events[['user_id', 'activation_month']].set_index('user_id')
    cohort_data = cohort_data.join(user_activity, how='left')
    
    # Calculate periods (months since activation)
    cohort_sizes = cohort_data.groupby('activation_month').size()
    cohort_table = pd.DataFrame()
    
    # Generate month vintages starting from March 2024
    current_month = pd.Period(datetime.now(target_tz), freq='M')
    month_range = pd.period_range(march_2024, current_month, freq='M')
    
    for activation_month in month_range:
        if activation_month in cohort_sizes.index:
            cohort_users = cohort_data[cohort_data['activation_month'] == activation_month]
            
            # Format the month label as "YYYY-MM" for better readability
            month_label = activation_month.strftime('%Y-%m')
            
            for i, month in enumerate(user_activity.columns):
                if month >= activation_month:
                    period = (month - activation_month).n
                    if 1 <= period <= 12:  # Start from Month 1, skip Month 0 (always 100%)
                        active_users = (cohort_users[month] > 0).sum()
                        retention_rate = active_users / len(cohort_users) * 100
                        
                        cohort_table.loc[month_label, f'Month {period}'] = retention_rate
    
    return cohort_table, cohort_sizes

# Only create cohort analysis if we have sufficient data
if len(users_df_initial) > 10 and len(events_df_initial) > 100:
    cohort_table, cohort_sizes = create_cohort_analysis(users_df_initial, events_df_initial, TARGET_TZ)
    
    if not cohort_table.empty and len(cohort_table.columns) > 1:
        # Create heatmap for cohort analysis
        import plotly.figure_factory as ff
        
        # Prepare data for heatmap
        cohort_display = cohort_table.copy()
        
        # Sort the index to ensure proper chronological order (most recent at top)
        cohort_display = cohort_display.sort_index(ascending=False)
        
        # Create annotations for the heatmap
        annotations = []
        for i, signup_month in enumerate(cohort_display.index):
            for j, period in enumerate(cohort_display.columns):
                value = cohort_display.iloc[i, j]
                if not pd.isna(value):
                    annotations.append(
                        dict(
                            x=j, y=i,
                            text=f'{value:.1f}%',
                            showarrow=False,
                            font=dict(color='white' if value < 50 else 'black')
                        )
                    )
        
        fig_cohort = go.Figure()
        
        # Calculate actual min and max values for better color scaling
        actual_values = cohort_display.values[~pd.isna(cohort_display.values)]
        if len(actual_values) > 0:
            min_val = float(actual_values.min())
            max_val = float(actual_values.max())
            # Add small buffer for better visual distribution
            color_range = max_val - min_val
            zmin = max(0, min_val - color_range * 0.1)
            zmax = min(100, max_val + color_range * 0.1)
        else:
            zmin, zmax = 0, 100
        
        fig_cohort.add_trace(go.Heatmap(
            z=cohort_display.values,
            x=list(cohort_display.columns),
            y=list(cohort_display.index),
            colorscale='RdYlGn',
            showscale=True,
            hoverongaps=False,
            zmin=zmin,
            zmax=zmax,
            colorbar=dict(title="Retention Rate (%)"),
            hovertemplate='<b>Cohort: %{y}</b><br>Period: %{x}<br>Retention: %{z:.1f}%<extra></extra>'
        ))
        
        # Add annotations
        for annotation in annotations:
            fig_cohort.add_annotation(annotation)
        
        # Adjust height based on number of cohorts (minimum 400, maximum 800)
        chart_height = min(800, max(400, len(cohort_display.index) * 50 + 200))
        
        fig_cohort.update_layout(
            title='Cohort Analysis - Monthly Retention Rates (%) [March 2024+ Vintages, Month 0 Excluded]',
            xaxis_title='Period (Months since first event)',
            yaxis_title='Activation Cohort (Monthly Vintage)',
            width=1400,
            height=chart_height,
            yaxis=dict(
                type='category',
                categoryorder='array',
                categoryarray=list(cohort_display.index),
                tickfont=dict(size=12)
            ),
            xaxis=dict(
                tickfont=dict(size=12)
            )
        )
        
        st.plotly_chart(fig_cohort)
        
        # Cohort summary insights
        st.subheader("Cohort Insights")
        col1, col2, col3 = st.columns(3)
        
        # Calculate average retention rates (starting from Month 1 since Month 0 is removed)
        month_1_retention = cohort_table['Month 1'].mean() if 'Month 1' in cohort_table.columns else 0
        month_3_retention = cohort_table['Month 3'].mean() if 'Month 3' in cohort_table.columns else 0
        month_6_retention = cohort_table['Month 6'].mean() if 'Month 6' in cohort_table.columns else 0
        
        with col1:
            st.metric("Avg Month 1 Retention", f"{month_1_retention:.1f}%")
        with col2:
            st.metric("Avg Month 3 Retention", f"{month_3_retention:.1f}%")
        with col3:
            st.metric("Avg Month 6 Retention", f"{month_6_retention:.1f}%")
    else:
        st.info("Insufficient data for cohort analysis. Need more historical user and event data.")
else:
    st.info("Cohort analysis requires more user and event data to generate meaningful insights.")
