import requests
import pandas as pd
from datetime import datetime
import pytz

# Set API key and headers
API_KEY = '29aa71bb-ce89-44df-8978-82c08473f05d'
HEADERS = {'Authorization': f'Bearer {API_KEY}'}

# Define the target timezone
TARGET_TZ = pytz.timezone('US/Eastern')

# API endpoints
events_url = 'https://xlw5-kd1n-crdj.n7c.xano.io/api:-VPGC53-/app_events'
users_url = 'https://xlw5-kd1n-crdj.n7c.xano.io/api:-VPGC53-/users'

def load_data():
    """Load data from API endpoints with retry logic"""
    max_retries = 3
    retry_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            print(f"Loading data (attempt {attempt + 1}/{max_retries})...")
            events_response = requests.get(events_url, headers=HEADERS, timeout=30)
            users_response = requests.get(users_url, headers=HEADERS, timeout=30)
            
            # Check for successful responses
            if events_response.status_code == 200 and users_response.status_code == 200:
                events_data = events_response.json()
                users_data = users_response.json()

                events_df = pd.DataFrame(events_data)
                users_df = pd.DataFrame(users_data)

                # Convert timestamp fields to datetime, interpret as UTC, then convert to ET
                users_df['created_at'] = pd.to_datetime(users_df['created_at'], unit='ms', utc=True).dt.tz_convert(TARGET_TZ)
                events_df['event_occurred_at'] = pd.to_datetime(events_df['event_occurred_at'], unit='ms', utc=True).dt.tz_convert(TARGET_TZ)
                
                print(f"Successfully loaded {len(users_df)} users and {len(events_df)} events")
                return users_df, events_df
            else:
                print(f"Failed to retrieve data. Status codes: {events_response.status_code}, {users_response.status_code}")
                return pd.DataFrame(), pd.DataFrame()
                
        except (requests.exceptions.ChunkedEncodingError, 
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException) as e:
            
            if attempt < max_retries - 1:  # Not the last attempt
                print(f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
                import time
                time.sleep(retry_delay)
                continue
            else:  # Last attempt failed
                print(f"Failed to retrieve data after {max_retries} attempts. Error: {str(e)}")
                return pd.DataFrame(), pd.DataFrame()

def find_month_6_plus_active_users(users_df, events_df, target_tz):
    """
    Find users who were active in their 6th month OR LATER after activation.
    This includes users who retained for at least 6 months (6+, 7+, 8+, etc.).
    Based on the retention calculation logic from the main dashboard.
    """
    now_et = datetime.now(target_tz)
    
    # Find first event date for each user (activation date)
    user_first_events = events_df.groupby('user_id')['event_occurred_at'].min().reset_index()
    user_first_events.columns = ['user_id', 'activation_date']
    
    # Merge with users data
    users_with_activation = users_df.merge(user_first_events, left_on='id', right_on='user_id', how='inner')
    
    if users_with_activation.empty:
        print("No users with activation events found.")
        return pd.DataFrame()
    
    # Month 6+ retention calculation (following the same logic as the dashboard)
    period_days = 6 * 30  # 180 days (6 months minimum)
    
    # Only consider users activated at least 180 days ago (denominator filter)
    # This ensures we only count users who have had enough time to potentially return in month 6+
    cutoff_date = now_et - pd.Timedelta(days=period_days)
    eligible_users = users_with_activation[users_with_activation['activation_date'] <= cutoff_date].copy()
    
    if eligible_users.empty:
        print(f"No users found who were activated at least {period_days} days ago.")
        print(f"Cutoff date: {cutoff_date}")
        print(f"Earliest activation date: {users_with_activation['activation_date'].min()}")
        return pd.DataFrame()
    
    print(f"Found {len(eligible_users)} users eligible for Month 6+ retention analysis")
    
    # Calculate the target return period for each user (from activation date)
    # Month 6+ starts 180 days after activation and continues indefinitely
    eligible_users['return_period_start'] = eligible_users['activation_date'] + pd.Timedelta(days=period_days)
    
    # Find users who had activity from their 6th month onwards
    month_6_plus_active_users = []
    
    print("Checking for Month 6+ activity...")
    for _, user in eligible_users.iterrows():
        user_events_month_6_plus = events_df[
            (events_df['user_id'] == user['user_id']) &
            (events_df['event_occurred_at'] >= user['return_period_start'])
        ]
        
        if not user_events_month_6_plus.empty:
            # Calculate which months they were active in
            user_events_month_6_plus = user_events_month_6_plus.copy()
            user_events_month_6_plus['days_since_activation'] = (
                user_events_month_6_plus['event_occurred_at'] - user['activation_date']
            ).dt.days
            user_events_month_6_plus['month_since_activation'] = (
                user_events_month_6_plus['days_since_activation'] // 30
            ) + 1  # +1 because month 1 starts at day 0
            
            # Get the months they were active in (6+)
            active_months = sorted(user_events_month_6_plus['month_since_activation'].unique())
            active_months_6_plus = [m for m in active_months if m >= 6]
            
            if active_months_6_plus:  # Double-check they have 6+ month activity
                # Add additional info about their Month 6+ activity
                user_record = user.copy()
                user_record['total_events_month_6_plus'] = len(user_events_month_6_plus)
                user_record['first_event_month_6_plus'] = user_events_month_6_plus['event_occurred_at'].min()
                user_record['last_event_month_6_plus'] = user_events_month_6_plus['event_occurred_at'].max()
                user_record['active_days_month_6_plus'] = user_events_month_6_plus['event_occurred_at'].dt.date.nunique()
                user_record['active_months_6_plus'] = active_months_6_plus
                user_record['earliest_retention_month'] = min(active_months_6_plus)
                user_record['latest_retention_month'] = max(active_months_6_plus)
                user_record['total_retention_months'] = len(active_months_6_plus)
                
                # Calculate the span of their long-term retention
                retention_span_days = (
                    user_record['last_event_month_6_plus'] - user_record['first_event_month_6_plus']
                ).days
                user_record['retention_span_days'] = retention_span_days
                
                month_6_plus_active_users.append(user_record)
    
    if not month_6_plus_active_users:
        print("No users were active in their 6th month or later.")
        return pd.DataFrame()
    
    result_df = pd.DataFrame(month_6_plus_active_users)
    print(f"Found {len(result_df)} users who were active in their 6th month or later")
    
    return result_df

def main():
    """Main function to identify and output Month 6+ active users"""
    print("=== Month 6+ Long-Term Retained Users Analysis ===")
    print(f"Analysis run at: {datetime.now(TARGET_TZ)}")
    print()
    
    # Load data
    users_df, events_df = load_data()
    
    if users_df.empty or events_df.empty:
        print("Failed to load data. Exiting.")
        return
    
    # Find Month 6+ active users
    month_6_users = find_month_6_plus_active_users(users_df, events_df, TARGET_TZ)
    
    if month_6_users.empty:
        print("No Month 6+ active users found.")
        return
    
    # Display results
    print("\n=== MONTH 6+ LONG-TERM RETAINED USERS ===")
    print(f"Total users active in their 6th month or later: {len(month_6_users)}")
    print()
    
    # Sort by earliest retention month, then by total retention months (most impressive first)
    month_6_users_sorted = month_6_users.sort_values(['earliest_retention_month', 'total_retention_months'], ascending=[True, False])
    
    # Display key information for each user
    print("User Details:")
    print("-" * 140)
    
    for _, user in month_6_users_sorted.iterrows():
        print(f"User ID: {user['id']}")
        print(f"  Created: {user['created_at'].strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"  Activated: {user['activation_date'].strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"  Long-term Activity: Months {user['earliest_retention_month']}-{user['latest_retention_month']} ({user['total_retention_months']} months)")
        print(f"  Active Months: {user['active_months_6_plus']}")
        print(f"  Total Events (Month 6+): {user['total_events_month_6_plus']} events across {user['active_days_month_6_plus']} days")
        print(f"  Activity Span: {user['first_event_month_6_plus'].strftime('%Y-%m-%d')} to {user['last_event_month_6_plus'].strftime('%Y-%m-%d')} ({user['retention_span_days']} days)")
        
        # Show setup information if available
        try:
            has_token = pd.notna(user['expo_push_token']) and user['expo_push_token'] != ''
            has_oauth = pd.notna(user['oauth_options']) and str(user['oauth_options']) != '[]'
            setup_status = []
            if has_token:
                setup_status.append("Push Token")
            if has_oauth:
                setup_status.append("OAuth")
            print(f"  Setup: {', '.join(setup_status) if setup_status else 'Basic'}")
        except:
            print(f"  Setup: Unknown")
        
        # Show referral source if available
        try:
            if pd.notna(user['referral_source']) and user['referral_source'] != '':
                print(f"  Referral: {user['referral_source']}")
        except:
            pass
        
        print()
    
    # Save to CSV
    output_filename = f"month_6_plus_retained_users_{datetime.now(TARGET_TZ).strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Select relevant columns for CSV output
    output_columns = [
        'id', 'created_at', 'activation_date', 
        'earliest_retention_month', 'latest_retention_month', 'total_retention_months',
        'total_events_month_6_plus', 'active_days_month_6_plus', 'retention_span_days',
        'first_event_month_6_plus', 'last_event_month_6_plus', 'active_months_6_plus'
    ]
    
    # Add optional columns if they exist
    for col in ['expo_push_token', 'oauth_options', 'referral_source']:
        if col in month_6_users.columns:
            output_columns.append(col)
    
    # Create output dataframe with selected columns
    output_df = month_6_users[output_columns].copy()
    
    # Convert datetime columns to string for better CSV readability
    datetime_cols = ['created_at', 'activation_date', 'first_event_month_6_plus', 'last_event_month_6_plus']
    for col in datetime_cols:
        if col in output_df.columns:
            output_df[col] = output_df[col].dt.strftime('%Y-%m-%d %H:%M:%S %Z')
    
    # Convert list column to string for CSV
    if 'active_months_6_plus' in output_df.columns:
        output_df['active_months_6_plus'] = output_df['active_months_6_plus'].astype(str)
    
    output_df.to_csv(output_filename, index=False)
    print(f"Results saved to: {output_filename}")
    
    # Summary statistics
    print("\n=== SUMMARY STATISTICS ===")
    print(f"Long-term retention (6+ months): {len(month_6_users)} users were active in their 6th month or later")
    
    avg_events = month_6_users['total_events_month_6_plus'].mean()
    avg_days = month_6_users['active_days_month_6_plus'].mean()
    avg_retention_months = month_6_users['total_retention_months'].mean()
    avg_span_days = month_6_users['retention_span_days'].mean()
    
    print(f"Average events per user (Month 6+): {avg_events:.1f}")
    print(f"Average active days per user (Month 6+): {avg_days:.1f}")
    print(f"Average retention months per user: {avg_retention_months:.1f}")
    print(f"Average retention span: {avg_span_days:.0f} days")
    
    # Show activation date range
    earliest_activation = month_6_users['activation_date'].min()
    latest_activation = month_6_users['activation_date'].max()
    print(f"Activation date range: {earliest_activation.strftime('%Y-%m-%d')} to {latest_activation.strftime('%Y-%m-%d')}")
    
    # Show retention month distribution
    print(f"\nRetention month distribution:")
    earliest_months = month_6_users['earliest_retention_month'].value_counts().sort_index()
    for month, count in earliest_months.items():
        print(f"  First retained in Month {month}: {count} users")
    
    # Show most impressive long-term users
    print(f"\nTop 10 most impressive long-term users:")
    top_users = month_6_users.nlargest(10, ['total_retention_months', 'total_events_month_6_plus'])
    for _, user in top_users.iterrows():
        print(f"  User {user['id']}: {user['total_retention_months']} months, {user['total_events_month_6_plus']} events, span {user['retention_span_days']} days")

if __name__ == "__main__":
    main()