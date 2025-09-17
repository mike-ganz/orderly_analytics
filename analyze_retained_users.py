import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter, defaultdict
import numpy as np
from wordcloud import WordCloud
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Set API key and headers
API_KEY = '29aa71bb-ce89-44df-8978-82c08473f05d'
HEADERS = {'Authorization': f'Bearer {API_KEY}'}

URL = 'https://xlw5-kd1n-crdj.n7c.xano.io/api:KPv3lPa-/user_profiles'

print("Fetching user profile data...")
try:
    response = requests.get(URL, headers=HEADERS)
    print(f"API Response Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Successfully loaded {len(data)} user profiles")
        
        if len(data) == 0:
            print("⚠️  Warning: API returned empty data")
            exit()
        
# Data loaded successfully
    else:
        print(f"❌ API Error: {response.status_code}")
        print(f"Response: {response.text}")
        exit()
        
except requests.exceptions.RequestException as e:
    print(f"❌ Request failed: {e}")
    exit()
except Exception as e:
    print(f"❌ Error processing response: {e}")
    exit()

# =============================================================================
# DATA ANALYSIS FRAMEWORK
# =============================================================================

class UserProfileAnalyzer:
    def __init__(self, data):
        self.data = data
        self.df = self._create_dataframe()
        
    def _create_dataframe(self):
        """Convert nested JSON data to flat pandas DataFrame for analysis"""
        records = []
        
        for user in self.data:
            # Check if we have profile_data.profile structure
            if 'profile_data' in user and 'profile' in user['profile_data']:
                profile = user['profile_data']['profile']
            elif 'profile' in user:
                profile = user['profile']
            else:
                continue
            record = {}
            
            # Demographics
            demo = profile.get('demographics', {})
            record.update({
                'gender': demo.get('likely_gender'),
                'age_range': demo.get('likely_age_range'),
                'occupation': demo.get('likely_occupation'),
                'income_level': demo.get('likely_income_level'),
                'location_type': demo.get('likely_location_type'),
                'education_level': demo.get('likely_education_level'),
                'geographic_ties': demo.get('likely_geographic_ties', [])  # Keep as list for now
            })
            
            # Psychographics - we'll handle these as lists/strings
            psycho = profile.get('psychographics', {})
            record.update({
                'values': psycho.get('values', []),
                'interests': psycho.get('interests', []),
                'lifestyle': psycho.get('lifestyle', []),
                'personality_traits': psycho.get('personality_traits', [])
            })
            
            # Purchasing habits
            purchase = profile.get('purchasing_habits', {})
            record.update({
                'purchasing_intent': purchase.get('purchasing_intent'),
                'value_orientation': purchase.get('value_orientation'),
                'retailer_preferences': purchase.get('retailer_preferences', [])
            })
            
            # Summary
            record['summary_narrative'] = profile.get('summary_narrative', '')
            
            records.append(record)
            
        return pd.DataFrame(records)
    
    def demographic_analysis(self):
        """Comprehensive demographic breakdown and visualizations"""
        print("=" * 60)
        print("DEMOGRAPHIC ANALYSIS")
        print("=" * 60)
        
        # Gender distribution
        gender_dist = self.df['gender'].value_counts(dropna=False)
        print(f"\n📊 Gender Distribution:")
        for gender, count in gender_dist.items():
            pct = (count / len(self.df)) * 100
            gender_label = "Unknown/Missing" if pd.isna(gender) else gender
            print(f"  {gender_label}: {count} ({pct:.1f}%)")
        
        # Age ranges
        age_dist = self.df['age_range'].value_counts(dropna=False)
        print(f"\n📅 Age Range Distribution:")
        for age, count in age_dist.items():
            pct = (count / len(self.df)) * 100
            age_label = "Unknown/Missing" if pd.isna(age) else age
            print(f"  {age_label}: {count} ({pct:.1f}%)")
        
        # Income levels
        income_dist = self.df['income_level'].value_counts(dropna=False)
        print(f"\n💰 Income Level Distribution:")
        for income, count in income_dist.items():
            pct = (count / len(self.df)) * 100
            income_label = "Unknown/Missing" if pd.isna(income) else income
            print(f"  {income_label}: {count} ({pct:.1f}%)")
        
        # Education levels
        edu_dist = self.df['education_level'].value_counts(dropna=False)
        print(f"\n🎓 Education Level Distribution:")
        for edu, count in edu_dist.items():
            pct = (count / len(self.df)) * 100
            edu_label = "Unknown/Missing" if pd.isna(edu) else edu
            print(f"  {edu_label}: {count} ({pct:.1f}%)")
        
        # Geographic analysis
        geo_locations = []
        for geo in self.df['geographic_ties']:
            # Handle None/NaN
            if geo is None or (isinstance(geo, float) and pd.isna(geo)):
                continue
            if isinstance(geo, list):
                for loc in geo:
                    if loc and str(loc).strip().lower() not in ['unclear', 'unknown', '']:
                        geo_locations.append(str(loc).strip())
            elif isinstance(geo, str) and geo.strip().lower() not in ['unclear', 'unknown', '']:
                geo_locations.extend([loc.strip() for loc in geo.split(',') if loc.strip()])
        
        if geo_locations:
            geo_counter = Counter(geo_locations)
            print(f"\n🌍 Geographic Distribution:")
            for location, count in geo_counter.most_common():
                pct = (count / len(self.df)) * 100
                print(f"  {location}: {count} ({pct:.1f}%)")
        else:
            print(f"\n🌍 Geographic Distribution: No clear geographic data available")
    
    def psychographic_analysis(self):
        """Deep dive into values, interests, lifestyle, and personality"""
        print("\n" + "=" * 60)
        print("PSYCHOGRAPHIC ANALYSIS")
        print("=" * 60)
        
        # Values analysis
        all_values = []
        for values_list in self.df['values']:
            if isinstance(values_list, list):
                all_values.extend(values_list)
        
        values_counter = Counter(all_values)
        print(f"\n💎 Top Values (what users care about):")
        for value, count in values_counter.most_common(10):
            pct = (count / len(self.df)) * 100
            print(f"  {value}: {count} users ({pct:.1f}%)")
        
        # Interests analysis (top 3 primary interests only)
        primary_interests = []
        for interests_list in self.df['interests']:
            if isinstance(interests_list, list):
                # Only take the first 3 interests as primary interests
                primary_interests.extend(interests_list[:3])
        
        interests_counter = Counter(primary_interests)
        print(f"\n🎯 Top Primary Interests (first 3 per user):")
        for interest, count in interests_counter.most_common(10):
            pct = (count / len(self.df)) * 100
            print(f"  {interest}: {count} users ({pct:.1f}%)")
        
        # Lifestyle patterns
        all_lifestyle = []
        for lifestyle_list in self.df['lifestyle']:
            if isinstance(lifestyle_list, list):
                all_lifestyle.extend(lifestyle_list)
        
        lifestyle_counter = Counter(all_lifestyle)
        print(f"\n🏡 Top Lifestyle Patterns:")
        for lifestyle, count in lifestyle_counter.most_common(10):
            pct = (count / len(self.df)) * 100
            print(f"  {lifestyle[:80]}{'...' if len(lifestyle) > 80 else ''}: {count} users ({pct:.1f}%)")
        
        # Personality traits
        all_traits = []
        for traits_list in self.df['personality_traits']:
            if isinstance(traits_list, list):
                all_traits.extend(traits_list)
        
        traits_counter = Counter(all_traits)
        print(f"\n🧠 Top Personality Traits:")
        for trait, count in traits_counter.most_common(10):
            pct = (count / len(self.df)) * 100
            print(f"  {trait}: {count} users ({pct:.1f}%)")
    
    def purchasing_behavior_analysis(self):
        """Analyze shopping patterns and retailer preferences"""
        print("\n" + "=" * 60)
        print("PURCHASING BEHAVIOR ANALYSIS")
        print("=" * 60)
        
        # Value orientation
        value_orient = self.df['value_orientation'].value_counts()
        print(f"\n💳 Value Orientation:")
        for orientation, count in value_orient.items():
            pct = (count / len(self.df)) * 100
            print(f"  {orientation}: {count} ({pct:.1f}%)")
        
        # Retailer preference analysis
        retailer_mentions = Counter()
        retailer_details = defaultdict(list)
        
        for retailer_list in self.df['retailer_preferences']:
            if isinstance(retailer_list, list):
                for retailer_info in retailer_list:
                    if isinstance(retailer_info, str) and ':' in retailer_info:
                        retailer_name = retailer_info.split(':')[0].strip()
                        retailer_mentions[retailer_name] += 1
                        retailer_details[retailer_name].append(retailer_info.split(':', 1)[1].strip())
        
        print(f"\n🛍️ Most Popular Retailers:")
        for retailer, count in retailer_mentions.most_common(10):
            pct = (count / len(self.df)) * 100
            print(f"  {retailer}: {count} users ({pct:.1f}%)")
        
        # Purchase intent
        intent_dist = self.df['purchasing_intent'].value_counts()
        print(f"\n🎯 Purchase Intent:")
        for intent, count in intent_dist.items():
            pct = (count / len(self.df)) * 100
            print(f"  {intent}: {count} ({pct:.1f}%)")
    
    def cross_dimensional_insights(self):
        """Find correlations and patterns across different data dimensions"""
        print("\n" + "=" * 60)
        print("CROSS-DIMENSIONAL INSIGHTS")
        print("=" * 60)
        
        # Age vs Income correlation
        print(f"\n📈 Age Range vs Income Level Patterns:")
        age_income = pd.crosstab(self.df['age_range'], self.df['income_level'], margins=True)
        print(age_income)
        
        # Value-Interest Alignment Analysis
        self._analyze_value_interest_alignment()
        
        # Retailer Competitive Analysis
        self._analyze_retailer_competition()
        
        # Spending Power vs Behavior Analysis
        self._analyze_spending_power_behavior()
        
    def _analyze_value_interest_alignment(self):
        """Analyze how well users' values align with their interests - key for authentic messaging"""
        print(f"\n💎 Value-Interest Alignment Analysis:")
        
        # Key alignments to check
        alignments = {
            "Health & Wellness": ["Fitness & Wellness", "Personal Grooming & Self-Care"],
            "Quality & Premium": ["Fashion & Style", "Home & Interior Design"],
            "Efficiency & Convenience": ["Technology & Smart Home", "Personal Grooming & Self-Care"],
            "Sustainability": ["Home & Interior Design", "Food & Cooking"]
        }
        
        for value_theme, related_interests in alignments.items():
            aligned_users = 0
            total_value_users = 0
            
            for _, row in self.df.iterrows():
                user_values = row['values'] if isinstance(row['values'], list) else []
                user_interests = row['interests'][:3] if isinstance(row['interests'], list) else []  # Primary interests only
                
                # Check if user has this value theme
                has_value = any(value_theme.lower() in str(val).lower() for val in user_values)
                if has_value:
                    total_value_users += 1
                    
                    # Check if they have related interests
                    has_related_interest = any(interest in user_interests for interest in related_interests)
                    if has_related_interest:
                        aligned_users += 1
            
            if total_value_users > 0:
                alignment_rate = (aligned_users / total_value_users) * 100
                print(f"  {value_theme}: {alignment_rate:.1f}% alignment ({aligned_users}/{total_value_users} users)")
    
    def _analyze_retailer_competition(self):
        """Identify which retailers compete for the same users - crucial for competitive strategy"""
        print(f"\n🏪 Retailer Competitive Analysis:")
        
        # Build retailer co-occurrence matrix
        retailer_pairs = defaultdict(int)
        all_retailers = set()
        
        for retailer_list in self.df['retailer_preferences']:
            if isinstance(retailer_list, list):
                user_retailers = []
                for retailer_info in retailer_list:
                    if isinstance(retailer_info, str) and ':' in retailer_info:
                        retailer_name = retailer_info.split(':')[0].strip()
                        user_retailers.append(retailer_name)
                        all_retailers.add(retailer_name)
                
                # Count co-occurrences
                for i, retailer1 in enumerate(user_retailers):
                    for retailer2 in user_retailers[i+1:]:
                        pair = tuple(sorted([retailer1, retailer2]))
                        retailer_pairs[pair] += 1
        
        # Show top competing pairs
        print(f"  Top Retailer Overlaps (shared users):")
        for pair, count in sorted(retailer_pairs.items(), key=lambda x: x[1], reverse=True)[:5]:
            pct = (count / len(self.df)) * 100
            print(f"    {pair[0]} + {pair[1]}: {count} users ({pct:.1f}%)")
    
    def _analyze_spending_power_behavior(self):
        """Analyze if income levels match retailer choices - identifies aspirational vs practical shoppers"""
        print(f"\n💳 Spending Power vs Retailer Behavior:")
        
        # Define retailer tiers
        premium_retailers = ['Nordstrom', 'Sephora', 'Whole Foods', 'Williams Sonoma']
        value_retailers = ['Walmart', 'Target', 'Costco', 'Dollar Tree']
        
        for income_level in ['Middle Income', 'Upper Middle Class', 'Affluent']:
            income_users = self.df[self.df['income_level'] == income_level]
            if len(income_users) == 0:
                continue
                
            premium_users = 0
            value_users = 0
            
            for _, row in income_users.iterrows():
                user_retailers = []
                if isinstance(row['retailer_preferences'], list):
                    for retailer_info in row['retailer_preferences']:
                        if isinstance(retailer_info, str) and ':' in retailer_info:
                            retailer_name = retailer_info.split(':')[0].strip()
                            user_retailers.append(retailer_name)
                
                if any(retailer in premium_retailers for retailer in user_retailers):
                    premium_users += 1
                if any(retailer in value_retailers for retailer in user_retailers):
                    value_users += 1
            
            premium_pct = (premium_users / len(income_users)) * 100
            value_pct = (value_users / len(income_users)) * 100
            
            print(f"  {income_level}: {premium_pct:.1f}% shop premium, {value_pct:.1f}% shop value retailers")
        
    def behavioral_segmentation_analysis(self):
        """Advanced behavioral segmentation beyond basic demographics"""
        print("\n" + "=" * 60)
        print("BEHAVIORAL SEGMENTATION ANALYSIS")
        print("=" * 60)
        
        # Value-driven behavioral segments
        self._analyze_value_driven_segments()
        
        # Shopping behavior clusters
        self._analyze_shopping_behavior_clusters()
        
        # Customer lifetime value indicators
        self._analyze_clv_indicators()
        
    def _analyze_value_driven_segments(self):
        """Segment users by their core value propositions"""
        print(f"\n🎯 Value-Driven Behavioral Segments:")
        
        segments = {
            "Premium Seekers": ["Quality & Premium", "Quality & Durability"],
            "Convenience Optimizers": ["Efficiency & Convenience", "Convenience & Efficiency"],
            "Value Hunters": ["Value-Seeking", "quality for price"],
            "Health Enthusiasts": ["Health & Wellness", "Ingredient Transparency"],
            "Sustainability Champions": ["Sustainability", "Eco-Conscious"]
        }
        
        for segment_name, value_keywords in segments.items():
            segment_users = 0
            segment_profiles = []
            
            for idx, row in self.df.iterrows():
                user_values = row['values'] if isinstance(row['values'], list) else []
                
                # Check if user matches this segment
                matches_segment = any(
                    any(keyword.lower() in str(val).lower() for keyword in value_keywords)
                    for val in user_values
                )
                
                if matches_segment:
                    segment_users += 1
                    segment_profiles.append({
                        'income': row['income_level'],
                        'age': row['age_range'],
                        'interests': row['interests'][:2] if isinstance(row['interests'], list) else []
                    })
            
            if segment_users > 0:
                pct = (segment_users / len(self.df)) * 100
                print(f"\n  {segment_name}: {segment_users} users ({pct:.1f}%)")
                
                # Show segment characteristics
                if segment_profiles:
                    incomes = [p['income'] for p in segment_profiles if p['income']]
                    ages = [p['age'] for p in segment_profiles if p['age']]
                    top_income = Counter(incomes).most_common(1)[0][0] if incomes else "Unknown"
                    top_age = Counter(ages).most_common(1)[0][0] if ages else "Unknown"
                    
                    print(f"    → Primary profile: {top_age}, {top_income}")
                    
                    # Top interests for this segment
                    all_interests = []
                    for profile in segment_profiles:
                        all_interests.extend(profile['interests'])
                    
                    if all_interests:
                        top_interests = Counter(all_interests).most_common(2)
                        interest_names = [interest[0] for interest in top_interests]
                        print(f"    → Key interests: {', '.join(interest_names)}")
    
    def _analyze_shopping_behavior_clusters(self):
        """Identify distinct shopping behavior patterns"""
        print(f"\n🛒 Shopping Behavior Clusters:")
        
        # Define behavior patterns based on retailer combinations
        behavior_patterns = {
            "Omnichannel Shoppers": ["Amazon", "Target", "Walmart"],  # Shop everywhere
            "Premium Brand Loyalists": ["Nordstrom", "Sephora", "Williams Sonoma"],
            "Tech-Forward Shoppers": ["Best Buy", "Amazon", "Apple"],
            "Health & Beauty Focused": ["Sephora", "Ulta", "Target"],
            "Bulk Value Shoppers": ["Costco", "Walmart", "Amazon"]
        }
        
        for pattern_name, key_retailers in behavior_patterns.items():
            pattern_users = 0
            
            for _, row in self.df.iterrows():
                user_retailers = []
                if isinstance(row['retailer_preferences'], list):
                    for retailer_info in row['retailer_preferences']:
                        if isinstance(retailer_info, str) and ':' in retailer_info:
                            retailer_name = retailer_info.split(':')[0].strip()
                            user_retailers.append(retailer_name)
                
                # Check if user matches this pattern (shops at 2+ of the key retailers)
                matches = sum(1 for retailer in key_retailers if retailer in user_retailers)
                if matches >= 2:
                    pattern_users += 1
            
            if pattern_users > 0:
                pct = (pattern_users / len(self.df)) * 100
                print(f"  {pattern_name}: {pattern_users} users ({pct:.1f}%)")
    
    def _analyze_clv_indicators(self):
        """Identify characteristics that suggest higher customer lifetime value"""
        print(f"\n💎 Customer Lifetime Value Indicators:")
        
        # High CLV indicators
        high_clv_traits = {
            "Brand Loyalty": ["Brand-Loyal", "Brand-Savvy"],
            "Premium Shopping": ["Quality & Premium", "Quality-Conscious"],
            "Multiple Interests": 4,  # Users with 4+ primary interests
            "High Engagement": ["Detail-Oriented", "Organized"]
        }
        
        high_clv_users = set()
        
        for trait_name, indicators in high_clv_traits.items():
            trait_users = 0
            
            for idx, row in self.df.iterrows():
                matches_trait = False
                
                if trait_name == "Multiple Interests":
                    # Special case: count interests
                    interests_count = len(row['interests']) if isinstance(row['interests'], list) else 0
                    if interests_count >= indicators:
                        matches_trait = True
                else:
                    # Check personality traits and values
                    user_traits = row['personality_traits'] if isinstance(row['personality_traits'], list) else []
                    user_values = row['values'] if isinstance(row['values'], list) else []
                    all_user_text = str(user_traits) + str(user_values)
                    
                    matches_trait = any(indicator.lower() in all_user_text.lower() for indicator in indicators)
                
                if matches_trait:
                    trait_users += 1
                    high_clv_users.add(idx)
            
            pct = (trait_users / len(self.df)) * 100
            print(f"  {trait_name}: {trait_users} users ({pct:.1f}%)")
        
        # Overall high CLV segment
        total_high_clv = len(high_clv_users)
        clv_pct = (total_high_clv / len(self.df)) * 100
        print(f"\n  🎯 High-CLV Segment: {total_high_clv} users ({clv_pct:.1f}%) with multiple indicators")
        
    def strategic_market_insights(self):
        """Generate strategic insights for business decision making"""
        print("\n" + "=" * 60)
        print("STRATEGIC MARKET INSIGHTS")
        print("=" * 60)
        
        # Content strategy insights
        self._analyze_content_strategy_opportunities()
        
        # Market gap analysis
        self._analyze_market_gaps()
        
        # Geographic expansion opportunities
        self._analyze_geographic_opportunities()
        
    def _analyze_content_strategy_opportunities(self):
        """Identify content themes that would resonate with user segments"""
        print(f"\n📝 Content Strategy Opportunities:")
        
        # Map personality traits to content themes
        content_themes = {
            "How-to & Educational": ["Detail-Oriented", "Organized", "Practical"],
            "Premium & Aspirational": ["Quality-Conscious", "Brand-Savvy", "Discerning"],
            "Lifestyle & Inspirational": ["Creative", "Trend-Aware", "Style-focused"],
            "Value & Comparison": ["Value-Seeking", "Budget-conscious", "Practical yet Indulgent"]
        }
        
        for theme, personality_indicators in content_themes.items():
            theme_audience = 0
            
            for _, row in self.df.iterrows():
                user_traits = row['personality_traits'] if isinstance(row['personality_traits'], list) else []
                user_values = row['values'] if isinstance(row['values'], list) else []
                all_user_text = str(user_traits) + str(user_values)
                
                # Check if user matches this content theme
                matches_theme = any(indicator.lower() in all_user_text.lower() for indicator in personality_indicators)
                
                if matches_theme:
                    theme_audience += 1
            
            if theme_audience > 0:
                pct = (theme_audience / len(self.df)) * 100
                print(f"  {theme}: {theme_audience} users ({pct:.1f}%)")
    
    def _analyze_market_gaps(self):
        """Identify unmet needs based on values vs current retailer ecosystem"""
        print(f"\n🔍 Market Gap Analysis:")
        
        # Define value propositions and check if they're well-served by current retailers
        value_gaps = {
            "Sustainability Focus": {
                "values": ["Sustainability", "Eco-Conscious", "Ingredient Transparency"],
                "retailers": ["Whole Foods", "Thrive Market", "Grove Collaborative"]
            },
            "Tech Integration": {
                "values": ["Efficiency & Convenience", "Smart Home"],
                "retailers": ["Best Buy", "Amazon", "Apple"]
            },
            "Wellness & Self-Care": {
                "values": ["Health & Wellness", "Self-Care"],
                "retailers": ["Sephora", "Ulta", "CVS"]
            }
        }
        
        for gap_name, gap_info in value_gaps.items():
            # Count users who have these values
            value_users = 0
            served_users = 0
            
            for _, row in self.df.iterrows():
                user_values = row['values'] if isinstance(row['values'], list) else []
                user_retailers = []
                
                # Check if user has these values
                has_values = any(
                    any(val_keyword.lower() in str(val).lower() for val_keyword in gap_info['values'])
                    for val in user_values
                )
                
                if has_values:
                    value_users += 1
                    
                    # Check if they shop at relevant retailers
                    if isinstance(row['retailer_preferences'], list):
                        for retailer_info in row['retailer_preferences']:
                            if isinstance(retailer_info, str) and ':' in retailer_info:
                                retailer_name = retailer_info.split(':')[0].strip()
                                user_retailers.append(retailer_name)
                    
                    if any(retailer in user_retailers for retailer in gap_info['retailers']):
                        served_users += 1
            
            if value_users > 0:
                served_pct = (served_users / value_users) * 100
                unmet_users = value_users - served_users
                unmet_pct = (unmet_users / len(self.df)) * 100
                
                print(f"  {gap_name}: {served_pct:.1f}% served, {unmet_users} users ({unmet_pct:.1f}%) potentially underserved")
    
    def _analyze_geographic_opportunities(self):
        """Identify geographic expansion or focus opportunities"""
        print(f"\n🗺️ Geographic Market Opportunities:")
        
        # Analyze income levels by geography
        geo_income_analysis = defaultdict(lambda: defaultdict(int))
        
        for _, row in self.df.iterrows():
            geo_ties = row['geographic_ties']
            
            # Handle different data types properly
            if geo_ties is None or (isinstance(geo_ties, float) and pd.isna(geo_ties)):
                continue
            if isinstance(geo_ties, list) and len(geo_ties) == 0:
                continue
            if isinstance(geo_ties, str) and geo_ties.strip() == '':
                continue
                
            geo_list = geo_ties if isinstance(geo_ties, list) else [geo_ties]
            income = row['income_level']
            
            for geo in geo_list:
                if isinstance(geo, str) and geo.strip().lower() not in ['unclear', 'unknown']:
                    geo_clean = geo.strip()
                    if income:
                        geo_income_analysis[geo_clean][income] += 1
        
        print(f"  Regional Income Distribution:")
        for region, income_dist in geo_income_analysis.items():
            if sum(income_dist.values()) >= 5:  # Only show regions with 5+ users
                total_users = sum(income_dist.values())
                affluent_count = income_dist.get('Affluent', 0) + income_dist.get('Upper Middle Class', 0)
                affluent_pct = (affluent_count / total_users) * 100
                
                print(f"    {region}: {total_users} users, {affluent_pct:.1f}% high-income")
    
    def generate_user_personas(self):
        """Create distinct user personas based on clustering of characteristics"""
        print("\n" + "=" * 60)
        print("USER PERSONAS & SEGMENTS")
        print("=" * 60)
        
        # Simple persona creation based on common characteristic combinations
        personas = defaultdict(list)
        
        for idx, row in self.df.iterrows():
            # Create a simple persona key based on key characteristics
            persona_key = f"{row['gender']}_{row['age_range']}_{row['income_level']}"
            personas[persona_key].append(idx)
        
        print(f"\n👥 Identified {len(personas)} distinct user segments:")
        for persona, user_indices in list(personas.items())[:10]:  # Show top 10
            if len(user_indices) > 1:  # Only show segments with multiple users
                print(f"  {persona.replace('_', ' | ')}: {len(user_indices)} users")
    
    def generate_executive_summary(self):
        """Create a high-level executive summary of key findings"""
        print("\n" + "=" * 60)
        print("EXECUTIVE SUMMARY")
        print("=" * 60)
        
        total_users = len(self.df)
        
        # Key demographics
        top_gender = self.df['gender'].mode().iloc[0] if not self.df['gender'].mode().empty else "Unknown"
        top_age = self.df['age_range'].mode().iloc[0] if not self.df['age_range'].mode().empty else "Unknown"
        top_income = self.df['income_level'].mode().iloc[0] if not self.df['income_level'].mode().empty else "Unknown"
        
        print(f"\n📋 KEY FINDINGS:")
        print(f"  • Analyzed {total_users} user profiles")
        print(f"  • Primary demographic: {top_gender}, {top_age}, {top_income}")
        
        # Top interests (primary interests only)
        primary_interests = []
        for interests_list in self.df['interests']:
            if isinstance(interests_list, list):
                # Only consider first 3 interests as primary
                primary_interests.extend(interests_list[:3])
        top_interest = Counter(primary_interests).most_common(1)[0][0] if primary_interests else "Unknown"
        
        print(f"  • Most common interest: {top_interest}")
        
        # Top retailer
        retailer_mentions = Counter()
        for retailer_list in self.df['retailer_preferences']:
            if isinstance(retailer_list, list):
                for retailer_info in retailer_list:
                    if isinstance(retailer_info, str) and ':' in retailer_info:
                        retailer_name = retailer_info.split(':')[0].strip()
                        retailer_mentions[retailer_name] += 1
        
        top_retailer = retailer_mentions.most_common(1)[0][0] if retailer_mentions else "Unknown"
        print(f"  • Most popular retailer: {top_retailer}")
        
        print(f"\n💡 ACTIONABLE INSIGHTS:")
        print(f"  • Target marketing campaigns toward {top_gender} users aged {top_age}")
        print(f"  • Focus product development on {top_interest}-related features (primary interest)")
        print(f"  • Consider partnership opportunities with {top_retailer}")
        print(f"  • Develop personas around the {top_income} segment")
    
    def run_full_analysis(self):
        """Execute the complete analysis suite"""
        print("🚀 Starting comprehensive user profile analysis...")
        print(f"Dataset: {len(self.data)} user profiles\n")
        
        self.demographic_analysis()
        self.psychographic_analysis()
        self.purchasing_behavior_analysis()
        self.cross_dimensional_insights()
        self.behavioral_segmentation_analysis()
        self.strategic_market_insights()
        self.generate_user_personas()
        self.generate_executive_summary()
        
        print("\n" + "=" * 60)
        print("✅ ANALYSIS COMPLETE")
        print("=" * 60)

# Initialize analyzer and run analysis
if data and len(data) > 0:
    analyzer = UserProfileAnalyzer(data)
    analyzer.run_full_analysis()
else:
    print("❌ No data to analyze")

