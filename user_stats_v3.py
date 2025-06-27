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
    # Query the endpoints
    events_response = requests.get(events_url, headers=HEADERS)
    users_response = requests.get(users_url, headers=HEADERS)

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

# Aggregate total new users
total_new_users = users_df.set_index('created_at').resample(freq).size()

# Filter users with a populated expo_push_token
users_with_token = users_df[users_df['expo_push_token'].notna() & (users_df['expo_push_token'] != '')]
new_users_with_token = users_with_token.set_index('created_at').resample(freq).size()

# Filter users with non-empty oauth_options
users_with_oauth_options = users_df[users_df['oauth_options'].notna() & (users_df['oauth_options'].astype(str) != '[]')]
new_users_with_oauth_options = users_with_oauth_options.set_index('created_at').resample(freq).size()

# Filter users with both expo_push_token and oauth_options
users_with_both = users_df[
    (users_df['expo_push_token'].notna() & (users_df['expo_push_token'] != '')) &
    (users_df['oauth_options'].notna() & (users_df['oauth_options'].astype(str) != '[]'))
]
new_users_with_both = users_with_both.set_index('created_at').resample(freq).size()

# Aggregate active users
daily_active_users = events_df.set_index('event_occurred_at').resample(freq)['user_id'].nunique().reset_index(name='active_users')

# Create a line chart for total new users, new users with a token, and new users with oauth options
fig1 = go.Figure()
fig1.add_trace(go.Scatter(
    x=total_new_users.index, 
    y=total_new_users, 
    mode='lines', 
    name='Total New Users',
    line=dict(color='darkcyan')  # Set color for Total New Users line
))
fig1.add_trace(go.Scatter(
    x=new_users_with_token.index, 
    y=new_users_with_token, 
    mode='lines', 
    name='New Users with Expo Push Token',
    line=dict(color='darkred')  # Set color for New Users with Expo Push Token line
))
fig1.add_trace(go.Scatter(
    x=new_users_with_oauth_options.index, 
    y=new_users_with_oauth_options, 
    mode='lines', 
    name='New Users with OAuth Options',
    line=dict(color='#2ca02c')  # Set color for New Users with OAuth Options line
))
fig1.add_trace(go.Scatter(
    x=new_users_with_both.index, 
    y=new_users_with_both, 
    mode='lines', 
    name='New Users with Both Token and OAuth Options',
    line=dict(color='goldenrod')  # Set color for New Users with Both Token and OAuth Options line
))

fig1.update_layout(
    title='New Users',
    xaxis_title='Date',
    yaxis_title='Count',
    width=1400,  # Increased chart width
    height=550,
    legend=dict(
        orientation="h",  # Horizontal orientation for the legend
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        font=dict(size=10)
    )
)
st.plotly_chart(fig1)

# --- Referral Source Percentage Chart ---
# Filter out users with no referral source
users_df_referral = users_df[users_df['referral_source'].notna() & (users_df['referral_source'] != '')].copy()

if not users_df_referral.empty:
    # Group by referral source and resample
    referral_counts = users_df_referral.set_index('created_at').groupby('referral_source').resample(freq).size().unstack(level=0, fill_value=0)

    # Calculate total users *with* referral sources for each period
    total_referral_users = users_df_referral.set_index('created_at').resample(freq).size()

    # Align referral counts with the total referral users
    # Use .align to handle potential empty periods correctly
    total_referral_users_aligned, referral_counts_aligned = total_referral_users.align(referral_counts, join='left', axis=0, fill_value=0)

    # Calculate percentages based on the total number of users *with* referral sources
    # Avoid division by zero if a period has zero referral users
    referral_percentages = referral_counts_aligned.divide(total_referral_users_aligned, axis=0).fillna(0) * 100

    fig_referral = go.Figure()
    # Use Plotly's default color sequence or define a custom one if needed

    for i, source in enumerate(referral_percentages.columns):
        fig_referral.add_trace(go.Scatter(
            x=referral_percentages.index,
            y=referral_percentages[source],
            mode='lines',
            name=str(source), # Ensure source name is string
            stackgroup='one' # Enable stacking for line charts
        ))

    fig_referral.update_layout(
        title='Percentage of New Users by Referral Source',
        xaxis_title='Date',
        yaxis_title='Percentage (%)',
        width=1400,
        height=550,
        yaxis=dict(range=[0, 100]), # Ensure y-axis goes up to 100%
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        )
    )
    st.plotly_chart(fig_referral)
else:
    # Display a message if no referral data is available for the selected period
    st.info(f"No users with referral sources found between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}.")

# Create a figure for daily active users
fig2 = go.Figure(data=[
    go.Scatter(
        x=daily_active_users['event_occurred_at'],
        y=daily_active_users['active_users'],
        mode='lines',
        name='Active Users',
        line=dict(color='darkcyan')  # Set color for Active Users line
    )
])

# Default title
chart_title = 'Active Users'

# Conditionally add returning users trace and update title if aggregation is Daily
if aggregation == 'Daily':
    # Set index for alignment
    active_users_series = daily_active_users.set_index('event_occurred_at')['active_users']

    # Align active and new user series to handle any date mismatches
    aligned_active, aligned_new = active_users_series.align(total_new_users, join='outer', fill_value=0)

    # Calculate returning users (Active - New), ensuring it's not negative
    returning_users = (aligned_active - aligned_new).clip(lower=0)

    # Add the new trace to the figure
    fig2.add_trace(go.Scatter(
        x=returning_users.index,
        y=returning_users,
        mode='lines',
        name='Returning Users',
        line=dict(color='orange') # Style to differentiate
    ))

    # Update title to reflect the new data
    chart_title = 'Daily Active vs. Returning Users'


fig2.update_layout(
    title=chart_title,
    xaxis_title='Date',
    yaxis_title='Count',
    width=1400,  # Increased chart width
    height=550
)
st.plotly_chart(fig2)

# --------------------------
# New Chart: Percentage of Users Deleted Within 7 Days by Category
# --------------------------
# Convert 'deleted_date' to datetime, interpret as UTC, convert to ET, handling empty values
days_range = 1 # Keep 7 days for consistency
users_df['deleted_date'] = pd.to_datetime(users_df['deleted_date'], unit='ms', utc=True, errors='coerce').dt.tz_convert(TARGET_TZ)

# Calculate the difference in days between 'deleted_date' and 'created_at'
users_df['days_to_delete'] = (users_df['deleted_date'] - users_df['created_at']).dt.days

# Filter data to only include users created at least 'days_range' days ago
# Use timezone-aware comparison
cutoff_dt = now_et - pd.Timedelta(days=days_range)
users_df_filtered = users_df[users_df['created_at'] <= cutoff_dt].copy() # Use .copy() to avoid SettingWithCopyWarning

# Determine if the user was deleted within 'days_range' days
users_df_filtered['deleted_within_x_days'] = users_df_filtered['days_to_delete'].le(days_range) & users_df_filtered['deleted_date'].notna()

# --- Categorize Users ---
# Handle potential missing 'apple_oauth' column or non-dict values gracefully
if 'apple_oauth' not in users_df_filtered.columns:
    users_df_filtered['apple_oauth'] = None # Add column if missing

# Ensure 'apple_oauth' contains dictionaries or NaN/None before checking 'id'
def check_apple_id(x):
    # Explicitly return True if 'id' exists and is truthy, False otherwise
    return bool(isinstance(x, dict) and x.get('id'))

users_df_filtered['has_apple_id'] = users_df_filtered['apple_oauth'].apply(check_apple_id)
users_df_filtered['has_oauth_options'] = users_df_filtered['oauth_options'].notna() & (users_df_filtered['oauth_options'].astype(str) != '[]')

# Define categories
is_apple_no_email = users_df_filtered['has_apple_id'] & ~users_df_filtered['has_oauth_options']
is_apple_with_email = users_df_filtered['has_apple_id'] & users_df_filtered['has_oauth_options']
is_other = ~users_df_filtered['has_apple_id']

# Assign categories
users_df_filtered.loc[is_apple_no_email, 'category'] = 'Apple OAuth (No Email Link)'
users_df_filtered.loc[is_apple_with_email, 'category'] = 'Apple OAuth (With Email Link)'
users_df_filtered.loc[is_other, 'category'] = 'Other Users'
users_df_filtered['category'] = users_df_filtered['category'].fillna('Other Users') # Catch any NaNs just in case

# --- Aggregate and Calculate Percentages ---
fig3 = go.Figure()
colors = {'Apple OAuth (No Email Link)': 'red', 'Apple OAuth (With Email Link)': 'orange', 'Other Users': 'purple'}

for category in users_df_filtered['category'].unique():
    category_df = users_df_filtered[users_df_filtered['category'] == category]

    # Aggregate the data based on the selected frequency
    deleted_within_x_days = category_df.set_index('created_at').resample(freq)['deleted_within_x_days'].sum()
    total_users = category_df.set_index('created_at').resample(freq).size()

    # Calculate the percentage, handle division by zero
    percentage_deleted = (deleted_within_x_days / total_users * 100).fillna(0)

    fig3.add_trace(go.Scatter(
        x=percentage_deleted.index,
        y=percentage_deleted,
        mode='lines',
        name=category,
        line=dict(color=colors.get(category, 'grey')) # Use grey as default if category not in colors
    ))

# --- Update Chart Layout ---
fig3.update_layout(
    title=f'Percentage of Users Deleted Within {days_range} Days by Authentication Type',
    xaxis_title='Date',
    yaxis_title='Percentage (%)',
    width=1400,  # Align width with other charts
    height=550,
    yaxis=dict(range=[0, 100]),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        font=dict(size=10)
    )
)

st.plotly_chart(fig3)
