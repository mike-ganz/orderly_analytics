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

max_retries = 3
retry_delay = 2  # seconds

for attempt in range(max_retries):
    try:
        print(f"Loading data (attempt {attempt + 1}/{max_retries})...")
        events_response = requests.get(events_url, headers=HEADERS, timeout=30)
        
        # Check for successful responses
        if events_response.status_code == 200:
            events_data = events_response.json()
            events_df = pd.DataFrame(events_data)

            # Convert timestamp fields to datetime, interpret as UTC, then convert to ET
            events_df['event_occurred_at'] = pd.to_datetime(events_df['event_occurred_at'], unit='ms', utc=True).dt.tz_convert(TARGET_TZ)

            print(f"Successfully loaded {len(events_df)} events")
            break
        else:
            print(f"Failed to retrieve data. Status codes: {events_response.status_code}")

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

# ---------------- Date Filter: 2025-07-30 or later ----------------
# Ensure we filter using the tz-aware event_occurred_at in US/Eastern
cutoff_dt = TARGET_TZ.localize(datetime(2025, 7, 30))
events_df = events_df.loc[events_df['event_occurred_at'] >= cutoff_dt].copy()
print(f"Events on or after {cutoff_dt.date()}: {len(events_df)}")

# ---------------- Inventory Feature Analysis ----------------
# Identify inventory-related events by screen name (case-insensitive)
inventory_mask = events_df['screen_name'].astype(str).str.contains('inventory', case=False, na=False)
inventory_events_df = events_df.loc[inventory_mask].copy()

print(f"Total inventory-related events: {len(inventory_events_df)}")

# Users who have tried inventory = users with at least one inventory event
users_who_tried = inventory_events_df['user_id'].nunique()
print(f"Users who tried inventory: {users_who_tried}")

# Compute distinct usage days per user
inventory_events_df['event_date'] = inventory_events_df['event_occurred_at'].dt.date

unique_user_dates = (
    inventory_events_df[['user_id', 'event_date']]
    .drop_duplicates()
    .sort_values(['user_id', 'event_date'])
    .reset_index(drop=True)
)

# First and second distinct usage days per user
unique_user_dates['date_rank'] = unique_user_dates.groupby('user_id')['event_date'].rank(method='first')
first_date_by_user = (
    unique_user_dates[unique_user_dates['date_rank'] == 1]
    .set_index('user_id')['event_date']
)
second_date_by_user = (
    unique_user_dates[unique_user_dates['date_rank'] == 2]
    .set_index('user_id')['event_date']
)

returners_count = len(second_date_by_user)
return_rate = (returners_count / users_who_tried) if users_who_tried else 0.0
print(f"Users who came back on a different day: {returners_count} ({return_rate:.1%})")

# Days until second use (only for users with a second distinct day)
if returners_count:
    # Convert to datetime for timedelta arithmetic
    first_dt = pd.to_datetime(first_date_by_user.loc[second_date_by_user.index])
    second_dt = pd.to_datetime(second_date_by_user)
    days_to_return = (second_dt - first_dt).dt.days

    summary = {
        'count': int(days_to_return.shape[0]),
        'mean_days': float(days_to_return.mean()),
        'median_days': float(days_to_return.median()),
        'p25_days': float(days_to_return.quantile(0.25)),
        'p75_days': float(days_to_return.quantile(0.75)),
        'min_days': int(days_to_return.min()),
        'max_days': int(days_to_return.max()),
    }
    print("Days until users come back to Inventory (different day):")
    for k, v in summary.items():
        print(f"  {k}: {v}")
else:
    print("No users returned to Inventory on a different day yet.")

# Optional: quick peek at inventory screens breakdown
screen_counts = (
    inventory_events_df['screen_name']
    .value_counts()
    .head(10)
)
print("Top Inventory screens:")
print(screen_counts.to_string())

# ---------------- Return rates by first-day screen exposure ----------------
# Build per-user first inventory day
first_inventory_day_by_user = first_date_by_user  # alias for clarity

# Subset to events that occurred on each user's first inventory day
first_day_events_df = (
    inventory_events_df
    .merge(first_inventory_day_by_user.rename('first_day'), left_on='user_id', right_index=True)
)
first_day_events_df = first_day_events_df[
    first_day_events_df['event_date'] == first_day_events_df['first_day']
].copy()

# Normalize screen names to lower for grouping checks
first_day_events_df['screen_lower'] = first_day_events_df['screen_name'].astype(str).str.lower()

# Aggregate first-day exposure per user
first_day_exposure = (
    first_day_events_df.groupby('user_id')['screen_lower']
    .agg(lambda s: set(s))
)

def classify_first_day_screen(screen_set: set) -> str:
    if 'inventorydetails' in screen_set:
        return 'first_day_details'
    if screen_set == {'allinventory'}:
        return 'first_day_all_only'
    return 'first_day_other_mix'

first_day_group = first_day_exposure.apply(classify_first_day_screen)

# Determine who returned (second distinct day exists)
returned_user_ids = set(second_date_by_user.index)

def compute_group_metrics(group_name: str):
    users_in_group = set(first_day_group[first_day_group == group_name].index)
    total_users = len(users_in_group)
    if total_users == 0:
        return {
            'group': group_name,
            'users': 0,
            'returners': 0,
            'return_rate': 0.0,
            'median_days_to_return': None,
        }
    group_returners = users_in_group & returned_user_ids
    return_rate_local = len(group_returners) / total_users

    # Days to return (for returners in this group)
    if group_returners:
        first_dt = pd.to_datetime(first_inventory_day_by_user.loc[list(group_returners)])
        second_dt = pd.to_datetime(second_date_by_user.loc[list(group_returners)])
        days_to_return_local = (second_dt - first_dt).dt.days
        median_days = float(days_to_return_local.median())
    else:
        median_days = None

    return {
        'group': group_name,
        'users': total_users,
        'returners': len(group_returners),
        'return_rate': return_rate_local,
        'median_days_to_return': median_days,
    }

groups_to_report = ['first_day_details', 'first_day_all_only', 'first_day_other_mix']
group_summaries = [compute_group_metrics(g) for g in groups_to_report]

print("\nReturn rates by first-day screen exposure:")
for summary in group_summaries:
    print(
        f"  {summary['group']}: users={summary['users']}, "
        f"returners={summary['returners']} ({summary['return_rate']:.1%}), "
        f"median_days_to_return={summary['median_days_to_return']}"
    )

# Debug: show first-day screen-set breakdown to verify counts
screen_set_series = first_day_exposure.apply(lambda s: tuple(sorted(s)))
screen_set_counts = screen_set_series.value_counts()
print("\nFirst-day screen-set breakdown (top 10):")
print(screen_set_counts.head(10).to_string())

# Additional view: segment by FIRST screen touched on user's first inventory day
first_day_first_event = (
    first_day_events_df
    .sort_values(['user_id', 'event_occurred_at'])
    .groupby('user_id', as_index=False)
    .first()[['user_id', 'screen_name']]
)
first_screen_group = first_day_first_event.set_index('user_id')['screen_name'].str.lower()

def compute_first_screen_metrics(screen_key: str):
    users_in_group = set(first_screen_group[first_screen_group == screen_key].index)
    total_users = len(users_in_group)
    group_returners = users_in_group & returned_user_ids
    rate = (len(group_returners) / total_users) if total_users else 0.0
    median_days = None
    if group_returners:
        first_dt = pd.to_datetime(first_inventory_day_by_user.loc[list(group_returners)])
        second_dt = pd.to_datetime(second_date_by_user.loc[list(group_returners)])
        median_days = float(((second_dt - first_dt).dt.days).median())
    return screen_key, total_users, len(group_returners), rate, median_days

first_screen_keys = ['allinventory', 'inventorydetails', 'inventorylistitem']
first_screen_summaries = [compute_first_screen_metrics(k) for k in first_screen_keys]
print("\nReturn rates by FIRST screen on first day:")
for key, users_cnt, ret_cnt, rate, med_days in first_screen_summaries:
    print(f"  first_screen={key}: users={users_cnt}, returners={ret_cnt} ({rate:.1%}), median_days_to_return={med_days}")

# ---------------- Cohorts by EVER exposure (across the filtered window) ----------------
# Build per-user set of inventory screens across the window
user_ever_screens = (
    inventory_events_df.assign(screen_lower=lambda d: d['screen_name'].astype(str).str.lower())
    .groupby('user_id')['screen_lower']
    .agg(lambda s: set(s))
)

ever_details_users = set(user_ever_screens[user_ever_screens.apply(lambda s: 'inventorydetails' in s)].index)
ever_all_only_users = set(user_ever_screens[user_ever_screens.apply(lambda s: s == {'allinventory'})].index)
ever_other_mix_users = set(user_ever_screens.index) - ever_details_users - ever_all_only_users

def compute_return_metrics_for_users(user_ids: set, label: str):
    total_users = len(user_ids)
    if total_users == 0:
        print(f"  {label}: users=0")
        return
    group_returners = user_ids & returned_user_ids
    rate = len(group_returners) / total_users
    if group_returners:
        first_dt = pd.to_datetime(first_inventory_day_by_user.loc[list(group_returners)])
        second_dt = pd.to_datetime(second_date_by_user.loc[list(group_returners)])
        median_days = float(((second_dt - first_dt).dt.days).median())
    else:
        median_days = None
    print(
        f"  {label}: users={total_users}, returners={len(group_returners)} ({rate:.1%}), "
        f"median_days_to_return={median_days}"
    )

print("\nReturn rates by EVER screen exposure (within cutoff window):")
compute_return_metrics_for_users(ever_details_users, 'ever_details')
compute_return_metrics_for_users(ever_all_only_users, 'ever_all_only')
compute_return_metrics_for_users(ever_other_mix_users, 'ever_other_mix')