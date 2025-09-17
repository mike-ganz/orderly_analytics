import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from wordcloud import WordCloud
import numpy as np
from collections import Counter
import warnings
warnings.filterwarnings('ignore')

class UserProfileVisualizer:
    """
    Advanced visualization suite for user demographic and psychographic analysis
    """
    
    def __init__(self, analyzer):
        self.analyzer = analyzer
        self.df = analyzer.df
        self.data = analyzer.data
        
        # Set up plotting style
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def create_demographic_dashboard(self):
        """Create comprehensive demographic visualizations"""
        
        # Create subplots
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=[
                'Gender Distribution', 'Age Range Distribution',
                'Income Level Distribution', 'Education Level Distribution', 
                'Geographic Distribution', 'Occupation Categories'
            ],
            specs=[[{"type": "pie"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )
        
        # Gender pie chart
        gender_counts = self.df['gender'].value_counts()
        fig.add_trace(go.Pie(
            labels=gender_counts.index, 
            values=gender_counts.values,
            name="Gender"
        ), row=1, col=1)
        
        # Age range bar chart
        age_counts = self.df['age_range'].value_counts()
        fig.add_trace(go.Bar(
            x=age_counts.index, 
            y=age_counts.values,
            name="Age Range"
        ), row=1, col=2)
        
        # Income level bar chart
        income_counts = self.df['income_level'].value_counts()
        fig.add_trace(go.Bar(
            x=income_counts.index, 
            y=income_counts.values,
            name="Income Level"
        ), row=2, col=1)
        
        # Education level bar chart
        edu_counts = self.df['education_level'].value_counts()
        fig.add_trace(go.Bar(
            x=edu_counts.index, 
            y=edu_counts.values,
            name="Education"
        ), row=2, col=2)
        
        # Geographic distribution
        geo_locations = []
        for geo in self.df['geographic_ties'].dropna():
            geo_locations.extend([loc.strip() for loc in geo.split(',')])
        geo_counter = Counter(geo_locations)
        
        fig.add_trace(go.Bar(
            x=list(geo_counter.keys())[:10], 
            y=list(geo_counter.values())[:10],
            name="Geography"
        ), row=3, col=1)
        
        # Occupation categories
        occ_counts = self.df['occupation'].value_counts().head(10)
        fig.add_trace(go.Bar(
            x=occ_counts.values, 
            y=occ_counts.index,
            orientation='h',
            name="Occupation"
        ), row=3, col=2)
        
        fig.update_layout(
            height=1200,
            title_text="User Demographics Dashboard",
            showlegend=False
        )
        
        return fig
    
    def create_psychographic_wordclouds(self):
        """Generate word clouds for psychographic data"""
        
        # Collect all text data
        all_values = []
        all_interests = []
        all_traits = []
        
        for _, row in self.df.iterrows():
            if isinstance(row['values'], list):
                all_values.extend(row['values'])
            if isinstance(row['interests'], list):
                all_interests.extend(row['interests'])
            if isinstance(row['personality_traits'], list):
                all_traits.extend(row['personality_traits'])
        
        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Psychographic Profile Word Clouds', fontsize=16, fontweight='bold')
        
        # Values word cloud
        if all_values:
            values_text = ' '.join(all_values)
            wordcloud_values = WordCloud(width=400, height=300, background_color='white').generate(values_text)
            axes[0, 0].imshow(wordcloud_values, interpolation='bilinear')
            axes[0, 0].set_title('Core Values', fontweight='bold')
            axes[0, 0].axis('off')
        
        # Interests word cloud
        if all_interests:
            interests_text = ' '.join(all_interests)
            wordcloud_interests = WordCloud(width=400, height=300, background_color='white').generate(interests_text)
            axes[0, 1].imshow(wordcloud_interests, interpolation='bilinear')
            axes[0, 1].set_title('Interests & Hobbies', fontweight='bold')
            axes[0, 1].axis('off')
        
        # Personality traits word cloud
        if all_traits:
            traits_text = ' '.join(all_traits)
            wordcloud_traits = WordCloud(width=400, height=300, background_color='white').generate(traits_text)
            axes[1, 0].imshow(wordcloud_traits, interpolation='bilinear')
            axes[1, 0].set_title('Personality Traits', fontweight='bold')
            axes[1, 0].axis('off')
        
        # Top interests bar chart
        interest_counts = Counter(all_interests).most_common(10)
        if interest_counts:
            interests, counts = zip(*interest_counts)
            axes[1, 1].barh(range(len(interests)), counts)
            axes[1, 1].set_yticks(range(len(interests)))
            axes[1, 1].set_yticklabels(interests)
            axes[1, 1].set_title('Top 10 Interests', fontweight='bold')
            axes[1, 1].set_xlabel('Frequency')
        
        plt.tight_layout()
        return fig
    
    def create_retailer_analysis_chart(self):
        """Visualize retailer preferences and shopping patterns"""
        
        # Extract retailer data
        retailer_mentions = Counter()
        retailer_purposes = {}
        
        for retailer_list in self.df['retailer_preferences']:
            if isinstance(retailer_list, list):
                for retailer_info in retailer_list:
                    if isinstance(retailer_info, str) and ':' in retailer_info:
                        retailer_name = retailer_info.split(':')[0].strip()
                        retailer_mentions[retailer_name] += 1
                        
                        # Extract purpose/reason for shopping
                        purpose = retailer_info.split(':', 1)[1].strip()[:50] + "..."
                        if retailer_name not in retailer_purposes:
                            retailer_purposes[retailer_name] = []
                        retailer_purposes[retailer_name].append(purpose)
        
        # Create interactive retailer chart
        top_retailers = retailer_mentions.most_common(10)
        retailers, counts = zip(*top_retailers) if top_retailers else ([], [])
        
        fig = go.Figure()
        
        # Add bar chart
        fig.add_trace(go.Bar(
            x=list(retailers),
            y=list(counts),
            marker_color='lightblue',
            text=[f'{count} users' for count in counts],
            textposition='auto',
        ))
        
        fig.update_layout(
            title='Most Popular Retailers Among Users',
            xaxis_title='Retailers',
            yaxis_title='Number of Users',
            height=500
        )
        
        return fig
    
    def create_demographic_correlation_heatmap(self):
        """Create correlation heatmap for demographic variables"""
        
        # Encode categorical variables for correlation analysis
        demo_encoded = pd.DataFrame()
        
        categorical_cols = ['gender', 'age_range', 'income_level', 'education_level', 'location_type']
        
        for col in categorical_cols:
            if col in self.df.columns:
                # Simple label encoding for correlation
                demo_encoded[col] = pd.Categorical(self.df[col]).codes
        
        if not demo_encoded.empty:
            correlation_matrix = demo_encoded.corr()
            
            fig = go.Figure(data=go.Heatmap(
                z=correlation_matrix.values,
                x=correlation_matrix.columns,
                y=correlation_matrix.columns,
                hoverongaps=False,
                colorscale='RdBu',
                zmid=0
            ))
            
            fig.update_layout(
                title='Demographic Variables Correlation Heatmap',
                height=500
            )
            
            return fig
        else:
            return None
    
    def create_user_persona_sunburst(self):
        """Create sunburst chart showing user persona hierarchy"""
        
        # Create hierarchical data for sunburst
        persona_data = []
        
        for _, row in self.df.iterrows():
            if pd.notna(row['gender']) and pd.notna(row['age_range']) and pd.notna(row['income_level']):
                persona_data.append({
                    'gender': row['gender'],
                    'age': row['age_range'],
                    'income': row['income_level']
                })
        
        if persona_data:
            persona_df = pd.DataFrame(persona_data)
            
            # Count combinations
            persona_counts = persona_df.groupby(['gender', 'age', 'income']).size().reset_index(name='count')
            
            # Prepare data for sunburst
            ids = []
            labels = []
            parents = []
            values = []
            
            # Add root
            ids.append("Total")
            labels.append("All Users")
            parents.append("")
            values.append(len(persona_df))
            
            # Add gender level
            gender_counts = persona_df['gender'].value_counts()
            for gender, count in gender_counts.items():
                ids.append(gender)
                labels.append(gender)
                parents.append("Total")
                values.append(count)
            
            # Add age level
            for gender in gender_counts.index:
                age_counts = persona_df[persona_df['gender'] == gender]['age'].value_counts()
                for age, count in age_counts.items():
                    age_id = f"{gender}-{age}"
                    ids.append(age_id)
                    labels.append(age)
                    parents.append(gender)
                    values.append(count)
            
            fig = go.Figure(go.Sunburst(
                ids=ids,
                labels=labels,
                parents=parents,
                values=values,
                branchvalues="total",
            ))
            
            fig.update_layout(
                title="User Persona Hierarchy (Gender → Age → Income)",
                font_size=12,
                height=600
            )
            
            return fig
        
        return None
    
    def generate_all_visualizations(self, save_plots=True):
        """Generate all visualization dashboards"""
        
        print("🎨 Generating comprehensive visualizations...")
        
        # Create demographic dashboard
        demo_fig = self.create_demographic_dashboard()
        demo_fig.show()
        if save_plots:
            demo_fig.write_html("demographic_dashboard.html")
        
        # Create psychographic word clouds
        psycho_fig = self.create_psychographic_wordclouds()
        plt.show()
        if save_plots:
            psycho_fig.savefig("psychographic_wordclouds.png", dpi=300, bbox_inches='tight')
        
        # Create retailer analysis
        retailer_fig = self.create_retailer_analysis_chart()
        retailer_fig.show()
        if save_plots:
            retailer_fig.write_html("retailer_analysis.html")
        
        # Create correlation heatmap
        corr_fig = self.create_demographic_correlation_heatmap()
        if corr_fig:
            corr_fig.show()
            if save_plots:
                corr_fig.write_html("demographic_correlations.html")
        
        # Create persona sunburst
        persona_fig = self.create_user_persona_sunburst()
        if persona_fig:
            persona_fig.show()
            if save_plots:
                persona_fig.write_html("user_persona_hierarchy.html")
        
        print("✅ All visualizations generated!")
        if save_plots:
            print("📁 Visualization files saved as HTML and PNG in current directory")

# Usage example (to be added to main script):
# visualizer = UserProfileVisualizer(analyzer)
# visualizer.generate_all_visualizations()