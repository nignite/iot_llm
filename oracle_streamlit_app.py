#!/usr/bin/env python3
"""
Streamlit Web Frontend for Oracle CIMS IoT Database Natural Language Queries
Interactive web interface with visualizations and real-time query processing
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time
from oracle_query_interface import OracleQueryInterface
from oracle_domain_mapping import OracleDomainMapper

# Configure Streamlit page
st.set_page_config(
    page_title="Oracle CIMS IoT Database Query Interface",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    color: #1f77b4;
    text-align: center;
    margin-bottom: 2rem;
}

.metric-container {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 0.5rem;
    margin: 0.5rem 0;
}

.query-box {
    background-color: #e8f4f8;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #1f77b4;
}

.success-box {
    background-color: #d4edda;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #28a745;
}

.error-box {
    background-color: #f8d7da;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #dc3545;
}

.sidebar-info {
    background-color: #f8f9fa;
    padding: 0.5rem;
    border-radius: 0.3rem;
    margin: 0.5rem 0;
}
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def init_interface():
    """Initialize the query interface (cached)"""
    return OracleQueryInterface()

@st.cache_data
def get_database_stats():
    """Get database statistics (cached)"""
    interface = init_interface()
    
    stats_queries = {
        "Total Signals": "SELECT COUNT(*) FROM SIGNALITEM",
        "Signal Channels": "SELECT COUNT(*) FROM SIGNALCHANNEL",
        "Channel Groups": "SELECT COUNT(*) FROM CHANNELGROUP",
        "Current Values": "SELECT COUNT(*) FROM SIGNALVALUE",
        "Historical Records": "SELECT COUNT(*) FROM REPDATA",
        "Report Items": "SELECT COUNT(*) FROM REPITEM",
        "Process Periods": "SELECT COUNT(*) FROM PROCINSTANCE",
        "External Systems": "SELECT COUNT(*) FROM ADDRESS"
    }
    
    stats = {}
    for name, query in stats_queries.items():
        try:
            interface.cursor.execute(query)
            result = interface.cursor.fetchone()[0]
            stats[name] = result
        except Exception as e:
            stats[name] = f"Error: {e}"
    
    return stats

@st.cache_data
def get_sample_data():
    """Get sample data for visualizations"""
    interface = init_interface()
    
    # Recent signal values with signal info
    interface.cursor.execute("""
        SELECT 
            sv.SIGID, 
            si.SIGNAME, 
            si.OBJUNIT, 
            sv.SIGNUMVALUE, 
            sv.UPDATETIME,
            si.SIGTYPE
        FROM SIGNALVALUE sv
        JOIN SIGNALITEM si ON sv.SIGID = si.SIGID 
        ORDER BY sv.UPDATETIME DESC 
        LIMIT 1000
    """)
    
    columns = [desc[0] for desc in interface.cursor.description]
    data = interface.cursor.fetchall()
    
    return pd.DataFrame(data, columns=columns)

def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown('<h1 class="main-header">🏭 Oracle CIMS IoT Database Query Interface</h1>', unsafe_allow_html=True)
    st.markdown("**Ask questions about your Oracle CIMS IoT data in natural language**")
    
    # Initialize session state
    if 'query_history' not in st.session_state:
        st.session_state.query_history = []
    
    if 'current_result' not in st.session_state:
        st.session_state.current_result = None
    
    # Sidebar
    with st.sidebar:
        st.header("📊 Database Overview")
        
        # Database statistics
        with st.spinner("Loading database stats..."):
            stats = get_database_stats()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Signals", f"{stats.get('Total Signals', 'N/A'):,}")
            st.metric("Channels", stats.get('Signal Channels', 'N/A'))
            st.metric("Groups", stats.get('Channel Groups', 'N/A'))
            st.metric("Current Values", f"{stats.get('Current Values', 'N/A'):,}")
        
        with col2:
            st.metric("Historical", f"{stats.get('Historical Records', 'N/A'):,}")
            st.metric("Reports", stats.get('Report Items', 'N/A'))
            st.metric("Periods", f"{stats.get('Process Periods', 'N/A'):,}")
            st.metric("Ext Systems", stats.get('External Systems', 'N/A'))
        
        st.divider()
        
        # Sample queries
        st.header("💡 Sample Queries")
        
        sample_queries = [
            "Show me all temperature signals",
            "What are the current signal values?",
            "List channels in group 1",
            "Show historical data from last week",
            "What signals have values above 50?",
            "Count how many signals are online",
            "Show me all pressure sensors",
            "List report items for calculations",
            "What channel groups exist?",
            "Show signals with good quality data"
        ]
        
        for i, query in enumerate(sample_queries):
            if st.button(f"📝 {query}", key=f"sample_{i}", width='stretch'):
                st.session_state.sample_query = query
        
        st.divider()
        
        # Domain mapping info
        st.header("🔗 Oracle Schema Mappings")
        mapper = OracleDomainMapper()
        
        with st.expander("Table Mappings"):
            for domain, table in list(mapper.table_mappings.items())[:10]:
                st.markdown(f"**{domain}** → `{table}`")
        
        with st.expander("Business Terms"):
            for term, synonyms in list(mapper.business_terms.items())[:5]:
                st.markdown(f"**{term}** → {', '.join(synonyms[:3])}")
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("🎯 Ask Your Question")
        
        # Query input
        query_input = st.text_input(
            "Enter your natural language query:",
            value=st.session_state.get('sample_query', ''),
            placeholder="e.g., Show me all temperature signals",
            help="Ask questions about your Oracle CIMS IoT data in plain English"
        )
        
        # Clear sample query from session state
        if 'sample_query' in st.session_state:
            del st.session_state.sample_query
        
        col_query1, col_query2, col_query3 = st.columns([1, 1, 3])
        
        with col_query1:
            execute_query = st.button("🚀 Execute Query", type="primary", width='stretch')
        
        with col_query2:
            clear_history = st.button("🗑️ Clear History", width='stretch')
        
        if clear_history:
            st.session_state.query_history = []
            st.session_state.current_result = None
            st.rerun()
        
        # Execute query
        if execute_query and query_input.strip():
            with st.spinner("Processing your query..."):
                interface = init_interface()
                start_time = time.time()
                result = interface.execute_natural_language_query(query_input.strip())
                duration = time.time() - start_time
                
                # Add to history
                st.session_state.query_history.append({
                    'timestamp': datetime.now(),
                    'query': query_input.strip(),
                    'result': result,
                    'duration': duration
                })
                
                st.session_state.current_result = result
                st.rerun()
    
    with col2:
        st.header("📈 Quick Insights")
        
        # Load sample data for visualization
        with st.spinner("Loading data..."):
            sample_data = get_sample_data()
        
        if not sample_data.empty:
            # Convert timestamp to datetime
            sample_data['UPDATETIME'] = pd.to_datetime(sample_data['UPDATETIME'])
            
            # Signal distribution by type
            st.subheader("Signals by Type")
            type_counts = sample_data['SIGTYPE'].value_counts()
            
            fig_pie = px.pie(
                values=type_counts.values,
                names=['Analog' if x == 1 else 'Digital' if x == 5 else f'Type {x}' for x in type_counts.index],
                title="Signal Type Distribution"
            )
            fig_pie.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_pie, width='stretch')
            
            # Value distribution
            st.subheader("Signal Values")
            if 'SIGNUMVALUE' in sample_data.columns:
                # Filter out None/null values
                valid_values = sample_data.dropna(subset=['SIGNUMVALUE'])
                if not valid_values.empty:
                    fig_hist = px.histogram(
                        valid_values,
                        x='SIGNUMVALUE',
                        title="Distribution of Signal Values",
                        nbins=20
                    )
                    fig_hist.update_layout(height=300)
                    st.plotly_chart(fig_hist, width='stretch')
    
    # Query results section
    if st.session_state.current_result:
        st.header("🔍 Query Results")
        
        result = st.session_state.current_result
        
        if result['success']:
            # Success display
            st.markdown(f"""
            <div class="success-box">
                <strong>✅ Query executed successfully!</strong><br>
                Found <strong>{result['count']:,}</strong> records
                {f" between {result['time_range']['start']} and {result['time_range']['end']}" if result.get('time_range', {}).get('start') else ""}
            </div>
            """, unsafe_allow_html=True)
            
            # SQL query details
            with st.expander("🔧 Technical Details"):
                st.code(result['sql'], language='sql')
                if result.get('params'):
                    st.json(result['params'])
            
            # Results display
            if result['results']:
                st.subheader("📋 Results")
                
                # Convert to DataFrame
                df = pd.DataFrame(result['results'])
                
                # Display controls
                col_display1, col_display2 = st.columns([1, 3])
                
                with col_display1:
                    show_all = st.checkbox("Show all results", value=False)
                    max_rows = len(df) if show_all else min(50, len(df))
                
                # Display table
                st.dataframe(df.head(max_rows), width='stretch')
                
                if len(df) > max_rows:
                    st.info(f"Showing {max_rows} of {len(df):,} results. Check 'Show all results' to see more.")
                
                # Visualization if numeric data is present
                numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
                
                if len(numeric_columns) > 0:
                    st.subheader("📊 Data Visualization")
                    
                    col_viz1, col_viz2 = st.columns(2)
                    
                    with col_viz1:
                        viz_type = st.selectbox("Visualization Type", ["Line Chart", "Bar Chart", "Histogram", "Scatter Plot"])
                    
                    with col_viz2:
                        if len(numeric_columns) > 0:
                            value_col = st.selectbox("Value Column", numeric_columns)
                    
                    # Generate visualization
                    try:
                        if viz_type == "Line Chart" and 'UPDATETIME' in df.columns:
                            df_viz = df.copy()
                            df_viz['UPDATETIME'] = pd.to_datetime(df_viz['UPDATETIME'])
                            fig = px.line(df_viz, x='UPDATETIME', y=value_col, title=f"{value_col} over Time")
                            st.plotly_chart(fig, width='stretch')
                        
                        elif viz_type == "Bar Chart":
                            if len(df) <= 20:  # Only for reasonable number of bars
                                fig = px.bar(df.head(20), x=df.columns[0], y=value_col, title=f"{value_col} by {df.columns[0]}")
                                st.plotly_chart(fig, width='stretch')
                            else:
                                st.info("Too many records for bar chart. Showing first 20.")
                        
                        elif viz_type == "Histogram":
                            fig = px.histogram(df, x=value_col, title=f"Distribution of {value_col}")
                            st.plotly_chart(fig, width='stretch')
                        
                        elif viz_type == "Scatter Plot" and len(numeric_columns) >= 2:
                            y_col = st.selectbox("Y-axis", [col for col in numeric_columns if col != value_col])
                            fig = px.scatter(df, x=value_col, y=y_col, title=f"{y_col} vs {value_col}")
                            st.plotly_chart(fig, width='stretch')
                    
                    except Exception as e:
                        st.error(f"Error creating visualization: {e}")
                
                # Download results
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results as CSV",
                    data=csv,
                    file_name=f"oracle_cims_query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            
            else:
                st.info("No results found for your query.")
        
        else:
            # Error display
            st.markdown(f"""
            <div class="error-box">
                <strong>❌ Query failed</strong><br>
                <strong>Error:</strong> {result['error']}
            </div>
            """, unsafe_allow_html=True)
            
            if result.get('sql'):
                st.code(result['sql'], language='sql')
            
            # Suggestions
            st.markdown("""
            **💡 Suggestions:**
            - Try rephrasing your question
            - Use the sample queries in the sidebar
            - Check the domain mappings to understand available terms
            """)
    
    # Query history section
    if st.session_state.query_history:
        st.header("📚 Query History")
        
        # Show recent queries
        for i, entry in enumerate(reversed(st.session_state.query_history[-10:])):
            timestamp = entry['timestamp'].strftime("%H:%M:%S")
            status = "✅" if entry['result']['success'] else "❌"
            duration = f"{entry['duration']:.3f}s"
            
            with st.expander(f"{status} [{timestamp}] {entry['query'][:60]}..."):
                st.markdown(f"**Query:** {entry['query']}")
                st.markdown(f"**Duration:** {duration}")
                
                if entry['result']['success']:
                    st.markdown(f"**Results:** {entry['result']['count']:,} records found")
                    st.code(entry['result']['sql'], language='sql')
                    
                    if st.button(f"🔄 Re-run Query", key=f"rerun_{i}"):
                        st.session_state.current_result = entry['result']
                        st.rerun()
                else:
                    st.error(f"Error: {entry['result']['error']}")

    # Footer
    st.divider()
    st.markdown("""
    <div style="text-align: center; color: #666; margin-top: 2rem;">
        🏭 Oracle CIMS IoT Database Query Interface | Natural Language Processing for Industrial Data
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()