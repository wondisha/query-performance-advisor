# snowflake_performance_monitor_v2.py
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# ============================================================================
# PAGE CONFIG
# ============================================================================
st.set_page_config(
    page_title="Snowflake Performance Monitor",
    page_icon="❄️",
    layout="wide"
)

st.title("❄️ Snowflake Performance Monitor")
st.caption("Comprehensive monitoring for performance, cost, and optimization")

# ============================================================================
# SIDEBAR - FILTERS & ALERTS
# ============================================================================
st.sidebar.header("🔧 Filters")
days_back = st.sidebar.slider("Days to analyze", 1, 30, 7)

st.sidebar.divider()
st.sidebar.header("🚨 Alert Thresholds")
alert_spillage = st.sidebar.number_input("Remote spillage queries", value=10, min_value=0)
alert_queue = st.sidebar.number_input("Queue time threshold (sec)", value=30, min_value=0)
alert_failed = st.sidebar.number_input("Failed queries threshold", value=50, min_value=0)
alert_cache = st.sidebar.number_input("Min cache hit rate %", value=20, min_value=0, max_value=100)

# ============================================================================
# HELPER FUNCTION
# ============================================================================
def run_query(sql):
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def download_csv(df, filename):
    if not df.empty:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=filename,
            mime="text/csv",
            key=filename
        )

# ============================================================================
# TABS
# ============================================================================
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "📊 Overview", "🏭 Warehouse", "🐢 Slow Queries", "💾 Spillage",
    "💰 Cost", "🔍 Optimization", "❌ Errors", "📈 Trends"
])

# ============================================================================
# TAB 1: OVERVIEW
# ============================================================================
with tab1:
    st.header("Executive Dashboard")
    
    # KPI Row 1
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    df = run_query(f"""
        SELECT COUNT(*) AS cnt 
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
    """)
    total_queries = df['CNT'].iloc[0] if not df.empty else 0
    col1.metric("Total Queries", f"{total_queries:,}")
    
    df = run_query(f"""
        SELECT COUNT(*) AS cnt 
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS != 'SUCCESS'
    """)
    failed_queries = df['CNT'].iloc[0] if not df.empty else 0
    col2.metric("Failed Queries", f"{failed_queries:,}", 
                delta=f"{'⚠️' if failed_queries > alert_failed else '✅'}")
    
    df = run_query(f"""
        SELECT ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS = 'SUCCESS'
    """)
    avg_time = df['AVG_SEC'].iloc[0] if not df.empty and df['AVG_SEC'].iloc[0] else 0
    col3.metric("Avg Query Time", f"{avg_time}s")
    
    df = run_query(f"""
        SELECT COUNT(*) AS cnt 
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND BYTES_SPILLED_TO_REMOTE_STORAGE > 0
    """)
    spillage_count = df['CNT'].iloc[0] if not df.empty else 0
    col4.metric("Remote Spillage", f"{spillage_count:,}",
                delta=f"{'🔴' if spillage_count > alert_spillage else '✅'}")
    
    df = run_query(f"""
        SELECT ROUND(SUM(CREDITS_USED), 2) AS credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
    """)
    credits_used = df['CREDITS'].iloc[0] if not df.empty and df['CREDITS'].iloc[0] else 0
    col5.metric("Credits Used", f"{credits_used:,}")
    
    # Cache Hit Ratio
    df = run_query(f"""
        SELECT 
            ROUND(SUM(CASE WHEN BYTES_SCANNED = 0 AND ROWS_PRODUCED > 0 THEN 1 ELSE 0 END) * 100.0 / 
                  NULLIF(COUNT(*), 0), 1) AS cache_pct
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND QUERY_TYPE = 'SELECT'
        AND EXECUTION_STATUS = 'SUCCESS'
    """)
    cache_hit = df['CACHE_PCT'].iloc[0] if not df.empty and df['CACHE_PCT'].iloc[0] else 0
    col6.metric("Cache Hit Rate", f"{cache_hit}%",
                delta=f"{'🔴' if cache_hit < alert_cache else '✅'}")
    
    st.divider()
    
    # Query Volume Trend
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Query Volume Trend")
        df = run_query(f"""
            SELECT DATE(START_TIME) AS date,
                   COUNT(*) AS total_queries,
                   SUM(CASE WHEN EXECUTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
                   SUM(CASE WHEN EXECUTION_STATUS != 'SUCCESS' THEN 1 ELSE 0 END) AS failed
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            GROUP BY 1 ORDER BY 1
        """)
        if not df.empty:
            st.line_chart(df.set_index('DATE')[['TOTAL_QUERIES', 'SUCCESSFUL', 'FAILED']])
    
    with col2:
        st.subheader("🎯 Cache Efficiency Trend")
        df = run_query(f"""
            SELECT DATE(START_TIME) AS date,
                   ROUND(SUM(CASE WHEN BYTES_SCANNED = 0 AND ROWS_PRODUCED > 0 THEN 1 ELSE 0 END) * 100.0 / 
                         NULLIF(COUNT(*), 0), 1) AS cache_hit_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            AND QUERY_TYPE = 'SELECT'
            AND EXECUTION_STATUS = 'SUCCESS'
            GROUP BY 1 ORDER BY 1
        """)
        if not df.empty:
            st.line_chart(df.set_index('DATE')['CACHE_HIT_PCT'])
    
    st.divider()
    
    # Query Type Distribution
    st.subheader("📊 Query Type Distribution")
    df = run_query(f"""
        SELECT QUERY_TYPE,
               COUNT(*) AS query_count,
               ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """)
    if not df.empty:
        col1, col2 = st.columns([2, 1])
        with col1:
            st.bar_chart(df.set_index('QUERY_TYPE')['QUERY_COUNT'])
        with col2:
            st.dataframe(df, use_container_width=True)

# ============================================================================
# TAB 2: WAREHOUSE
# ============================================================================
with tab2:
    st.header("🏭 Warehouse Utilization")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Credits by Warehouse")
        df = run_query(f"""
            SELECT WAREHOUSE_NAME,
                   ROUND(SUM(CREDITS_USED), 2) AS credits_used,
                   ROUND(SUM(CREDITS_USED_COMPUTE), 2) AS compute_credits,
                   ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 2) AS cloud_credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """)
        if not df.empty:
            st.bar_chart(df.set_index('WAREHOUSE_NAME')['CREDITS_USED'])
            st.dataframe(df, use_container_width=True)
            download_csv(df, "warehouse_credits.csv")
    
    with col2:
        st.subheader("Query Load by Warehouse")
        df = run_query(f"""
            SELECT WAREHOUSE_NAME,
                   COUNT(*) AS query_count,
                   ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_time_sec,
                   ROUND(AVG(QUEUED_OVERLOAD_TIME)/1000, 2) AS avg_queue_sec,
                   ROUND(SUM(QUEUED_OVERLOAD_TIME)/1000, 2) AS total_queue_sec
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            AND WAREHOUSE_NAME IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            download_csv(df, "warehouse_load.csv")
    
    st.divider()
    
    # Queuing Analysis
    st.subheader("⏳ Warehouse Queuing Analysis")
    df = run_query(f"""
        SELECT WAREHOUSE_NAME,
               DATE(START_TIME) AS date,
               COUNT(*) AS queued_queries,
               ROUND(SUM(QUEUED_OVERLOAD_TIME)/1000, 2) AS total_queue_sec,
               ROUND(AVG(QUEUED_OVERLOAD_TIME)/1000, 2) AS avg_queue_sec,
               ROUND(MAX(QUEUED_OVERLOAD_TIME)/1000, 2) AS max_queue_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND QUEUED_OVERLOAD_TIME > 0
        GROUP BY 1, 2 ORDER BY 2 DESC, 4 DESC
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        queued_high = df[df['AVG_QUEUE_SEC'] > alert_queue]
        if not queued_high.empty:
            st.error(f"⚠️ {len(queued_high)} warehouse-days exceeded queue threshold of {alert_queue}s")
        download_csv(df, "warehouse_queuing.csv")
    else:
        st.success("✅ No significant queuing detected")
    
    st.divider()
    
    # Warehouse Efficiency
    st.subheader("⏸️ Warehouse Efficiency (Queries per Credit)")
    df = run_query(f"""
        SELECT wmh.WAREHOUSE_NAME,
               ROUND(SUM(wmh.CREDITS_USED), 2) AS credits_used,
               COUNT(DISTINCT qh.QUERY_ID) AS total_queries,
               ROUND(COUNT(DISTINCT qh.QUERY_ID) * 1.0 / NULLIF(SUM(wmh.CREDITS_USED), 0), 1) AS queries_per_credit
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh 
            ON wmh.WAREHOUSE_NAME = qh.WAREHOUSE_NAME
            AND DATE(wmh.START_TIME) = DATE(qh.START_TIME)
        WHERE wmh.START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1 
        HAVING SUM(wmh.CREDITS_USED) > 0
        ORDER BY 4 ASC LIMIT 15
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        st.info("💡 Low queries_per_credit may indicate idle time or poor auto-suspend settings")

# ============================================================================
# TAB 3: SLOW QUERIES
# ============================================================================
with tab3:
    st.header("🐢 Slow Query Analysis")
    
    threshold_sec = st.slider("Slow query threshold (seconds)", 10, 300, 60)
    
    # Slow Queries List
    st.subheader(f"Queries Taking > {threshold_sec} Seconds")
    df = run_query(f"""
        SELECT QUERY_ID,
               USER_NAME,
               WAREHOUSE_NAME,
               QUERY_TYPE,
               ROUND(TOTAL_ELAPSED_TIME/1000, 2) AS duration_sec,
               ROUND(COMPILATION_TIME/1000, 2) AS compile_sec,
               ROUND(BYTES_SCANNED/1e9, 2) AS gb_scanned,
               ROUND(BYTES_SPILLED_TO_LOCAL_STORAGE/1e9, 2) AS local_spill_gb,
               ROUND(BYTES_SPILLED_TO_REMOTE_STORAGE/1e9, 2) AS remote_spill_gb,
               PARTITIONS_SCANNED,
               PARTITIONS_TOTAL,
               SUBSTR(QUERY_TEXT, 1, 200) AS query_preview
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND TOTAL_ELAPSED_TIME > {threshold_sec * 1000}
        AND EXECUTION_STATUS = 'SUCCESS'
        ORDER BY TOTAL_ELAPSED_TIME DESC
        LIMIT 50
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        download_csv(df, "slow_queries.csv")
        
        st.info("""
        **Optimization Tips:**
        - High `COMPILE_SEC` → Complex query, consider simplifying
        - High spillage → Increase warehouse size
        - High `GB_SCANNED` → Add filters or use clustering
        - `PARTITIONS_SCANNED` close to `PARTITIONS_TOTAL` → Add clustering keys
        """)
    else:
        st.success(f"✅ No queries exceeded {threshold_sec}s threshold")
    
    st.divider()
    
    # Compilation Time Analysis
    st.subheader("⚙️ Query Compilation Time")
    df = run_query(f"""
        SELECT WAREHOUSE_NAME,
               COUNT(*) AS queries,
               ROUND(AVG(COMPILATION_TIME)/1000, 2) AS avg_compile_sec,
               ROUND(MAX(COMPILATION_TIME)/1000, 2) AS max_compile_sec,
               ROUND(SUM(CASE WHEN COMPILATION_TIME > 5000 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_slow_compile
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS = 'SUCCESS'
        AND WAREHOUSE_NAME IS NOT NULL
        GROUP BY 1 ORDER BY 3 DESC LIMIT 15
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        st.info("💡 High compile times suggest complex queries or metadata issues")
    
    st.divider()
    
    # Performance by Query Type
    st.subheader("Performance by Query Type")
    df = run_query(f"""
        SELECT QUERY_TYPE,
               COUNT(*) AS query_count,
               ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_sec,
               ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY TOTAL_ELAPSED_TIME)/1000, 2) AS p95_sec,
               ROUND(MAX(TOTAL_ELAPSED_TIME)/1000, 2) AS max_sec,
               ROUND(SUM(BYTES_SCANNED)/1e12, 2) AS tb_scanned
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS = 'SUCCESS'
        GROUP BY 1 ORDER BY 3 DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        download_csv(df, "query_type_performance.csv")

# ============================================================================
# TAB 4: SPILLAGE
# ============================================================================
with tab4:
    st.header("💾 Query Spillage Analysis")
    
    st.markdown("""
    **Spillage occurs when query processing exceeds warehouse memory:**
    - 🟡 **Local Spillage**: Data spills to local SSD (moderate impact)
    - 🔴 **Remote Spillage**: Data spills to cloud storage (severe impact - 10x slower)
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🟡 Local Storage Spillage")
        df = run_query(f"""
            SELECT USER_NAME,
                   WAREHOUSE_NAME,
                   COUNT(*) AS spill_count,
                   ROUND(SUM(BYTES_SPILLED_TO_LOCAL_STORAGE)/1e9, 2) AS total_spill_gb,
                   ROUND(AVG(BYTES_SPILLED_TO_LOCAL_STORAGE)/1e6, 2) AS avg_spill_mb
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            AND BYTES_SPILLED_TO_LOCAL_STORAGE > 0
            GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 20
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            download_csv(df, "local_spillage.csv")
        else:
            st.success("✅ No local spillage detected")
    
    with col2:
        st.subheader("🔴 Remote Storage Spillage (Critical)")
        df = run_query(f"""
            SELECT USER_NAME,
                   WAREHOUSE_NAME,
                   COUNT(*) AS spill_count,
                   ROUND(SUM(BYTES_SPILLED_TO_REMOTE_STORAGE)/1e9, 2) AS total_spill_gb,
                   ROUND(AVG(BYTES_SPILLED_TO_REMOTE_STORAGE)/1e6, 2) AS avg_spill_mb
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            AND BYTES_SPILLED_TO_REMOTE_STORAGE > 0
            GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 20
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            st.error("⚠️ Remote spillage severely impacts performance. Consider larger warehouse or query optimization.")
            download_csv(df, "remote_spillage.csv")
        else:
            st.success("✅ No remote spillage detected")
    
    st.divider()
    
    # Spillage Trend
    st.subheader("📈 Spillage Trend Over Time")
    df = run_query(f"""
        SELECT DATE(START_TIME) AS date,
               COUNT(*) AS queries_with_spillage,
               ROUND(SUM(BYTES_SPILLED_TO_LOCAL_STORAGE)/1e9, 2) AS local_spill_gb,
               ROUND(SUM(BYTES_SPILLED_TO_REMOTE_STORAGE)/1e9, 2) AS remote_spill_gb
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND (BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
        GROUP BY 1 ORDER BY 1
    """)
    if not df.empty:
        st.line_chart(df.set_index('DATE')[['LOCAL_SPILL_GB', 'REMOTE_SPILL_GB']])
    
    st.divider()
    
    # Top Spillage Queries
    st.subheader("🔝 Top Spillage Queries")
    df = run_query(f"""
        SELECT QUERY_ID,
               USER_NAME,
               WAREHOUSE_NAME,
               ROUND(TOTAL_ELAPSED_TIME/1000, 2) AS duration_sec,
               ROUND(BYTES_SPILLED_TO_LOCAL_STORAGE/1e9, 2) AS local_gb,
               ROUND(BYTES_SPILLED_TO_REMOTE_STORAGE/1e9, 2) AS remote_gb,
               SUBSTR(QUERY_TEXT, 1, 150) AS query_preview
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND (BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
        ORDER BY BYTES_SPILLED_TO_REMOTE_STORAGE DESC, BYTES_SPILLED_TO_LOCAL_STORAGE DESC
        LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        download_csv(df, "spillage_queries.csv")

# ============================================================================
# TAB 5: COST
# ============================================================================
with tab5:
    st.header("💰 Cost Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Daily Credit Consumption")
        df = run_query(f"""
            SELECT DATE(START_TIME) AS date,
                   ROUND(SUM(CREDITS_USED), 2) AS total_credits,
                   ROUND(SUM(CREDITS_USED_COMPUTE), 2) AS compute,
                   ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 2) AS cloud_services
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            GROUP BY 1 ORDER BY 1
        """)
        if not df.empty:
            st.line_chart(df.set_index('DATE')['TOTAL_CREDITS'])
            st.dataframe(df, use_container_width=True)
            download_csv(df, "daily_credits.csv")
    
    with col2:
        st.subheader("Credits by Service Type")
        df = run_query(f"""
            SELECT SERVICE_TYPE,
                   ROUND(SUM(CREDITS_USED), 2) AS credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            GROUP BY 1 ORDER BY 2 DESC
        """)
        if not df.empty:
            st.bar_chart(df.set_index('SERVICE_TYPE')['CREDITS'])
            st.dataframe(df, use_container_width=True)
    
    st.divider()
    
    # Top Users by Compute
    st.subheader("👤 Top Users by Compute Hours")
    df = run_query(f"""
        SELECT USER_NAME,
               WAREHOUSE_NAME,
               COUNT(*) AS query_count,
               ROUND(SUM(TOTAL_ELAPSED_TIME)/3600000, 2) AS compute_hours,
               ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 4) AS cloud_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND WAREHOUSE_NAME IS NOT NULL
        GROUP BY 1, 2 ORDER BY 4 DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        download_csv(df, "user_compute.csv")
    
    st.divider()
    
    # Storage Costs
    st.subheader("📦 Storage Usage Trend")
    df = run_query(f"""
        SELECT DATE(USAGE_DATE) AS date,
               ROUND(AVG(STORAGE_BYTES)/1e12, 3) AS storage_tb,
               ROUND(AVG(STAGE_BYTES)/1e12, 3) AS stage_tb,
               ROUND(AVG(FAILSAFE_BYTES)/1e12, 3) AS failsafe_tb
        FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
        WHERE USAGE_DATE >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 1
    """)
    if not df.empty:
        st.line_chart(df.set_index('DATE'))
        st.dataframe(df, use_container_width=True)
    
    st.divider()
    
    # Data Transfer
    st.subheader("🔄 Data Transfer Costs")
    df = run_query(f"""
        SELECT TARGET_CLOUD,
               TARGET_REGION,
               ROUND(SUM(BYTES_TRANSFERRED)/1e9, 2) AS gb_transferred
        FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1, 2 ORDER BY 3 DESC
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No data transfer activity in this period")

# ============================================================================
# TAB 6: OPTIMIZATION
# ============================================================================
with tab6:
    st.header("🔍 Optimization Recommendations")
    
    # High Partition Scans
    st.subheader("🔴 Queries with High Partition Scans")
    df = run_query(f"""
        SELECT WAREHOUSE_NAME,
               USER_NAME,
               COUNT(*) AS high_scan_queries,
               ROUND(AVG(PARTITIONS_SCANNED * 100.0 / NULLIF(PARTITIONS_TOTAL, 0)), 1) AS avg_pct_scanned,
               ROUND(SUM(BYTES_SCANNED)/1e12, 2) AS total_tb_scanned
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND PARTITIONS_TOTAL > 100
        AND PARTITIONS_SCANNED * 100.0 / NULLIF(PARTITIONS_TOTAL, 0) > 80
        AND EXECUTION_STATUS = 'SUCCESS'
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        st.warning("💡 High partition scan % indicates missing or ineffective clustering keys")
        download_csv(df, "high_partition_scans.csv")
    else:
        st.success("✅ No major full-scan issues detected")
    
    st.divider()
    
    # Repeated Queries
    st.subheader("🔵 Repeated Queries (Caching Opportunities)")
    df = run_query(f"""
        SELECT QUERY_PARAMETERIZED_HASH,
               COUNT(*) AS execution_count,
               COUNT(DISTINCT USER_NAME) AS unique_users,
               ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_sec,
               ROUND(SUM(TOTAL_ELAPSED_TIME)/1000, 2) AS total_sec,
               SUBSTR(MAX(QUERY_TEXT), 1, 150) AS sample_query
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND QUERY_TYPE = 'SELECT'
        AND EXECUTION_STATUS = 'SUCCESS'
        GROUP BY 1
        HAVING COUNT(*) > 10
        ORDER BY 5 DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        st.info("💡 Repeated queries may benefit from materialized views or result caching")
        download_csv(df, "repeated_queries.csv")
    
    st.divider()
    
    # Cloud Services
    st.subheader("☁️ High Cloud Services Usage")
    df = run_query(f"""
        SELECT USER_NAME,
               COUNT(*) AS query_count,
               ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 4) AS cloud_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND CREDITS_USED_CLOUD_SERVICES > 0
        GROUP BY 1
        HAVING SUM(CREDITS_USED_CLOUD_SERVICES) > 0.1
        ORDER BY 3 DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
    
    st.divider()
    
    # Auto-Recommendations
    st.subheader("📋 Auto-Generated Recommendations")
    
    recommendations = []
    
    # Check spillage
    if spillage_count > alert_spillage:
        recommendations.append(f"🔴 **Remote Spillage**: {spillage_count} queries spilled to remote storage. Increase warehouse size or optimize queries.")
    
    # Check queuing
    queue_df = run_query(f"""
        SELECT COUNT(*) AS cnt FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND QUEUED_OVERLOAD_TIME > {alert_queue * 1000}
    """)
    if not queue_df.empty and queue_df['CNT'].iloc[0] > 10:
        recommendations.append(f"🟡 **Warehouse Queuing**: {queue_df['CNT'].iloc[0]} queries waited > {alert_queue}s. Consider multi-cluster or larger warehouse.")
    
    # Check failures
    if failed_queries > alert_failed:
        recommendations.append(f"🟡 **High Failure Rate**: {failed_queries} failed queries. Review error patterns in Errors tab.")
    
    # Check cache
    if cache_hit < alert_cache:
        recommendations.append(f"🟡 **Low Cache Hit Rate**: Only {cache_hit}% cache hits. Consider query patterns and result caching.")
    
    if recommendations:
        for rec in recommendations:
            st.markdown(rec)
    else:
        st.success("✅ No critical optimization issues detected!")

# ============================================================================
# TAB 7: ERRORS
# ============================================================================
with tab7:
    st.header("❌ Query Error Analysis")
    
    # Error KPIs
    col1, col2, col3 = st.columns(3)
    
    df = run_query(f"""
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT ERROR_CODE) AS unique_errors,
               COUNT(DISTINCT USER_NAME) AS affected_users
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS = 'FAIL'
    """)
    if not df.empty:
        col1.metric("Failed Queries", f"{df['TOTAL'].iloc[0]:,}")
        col2.metric("Unique Error Types", df['UNIQUE_ERRORS'].iloc[0])
        col3.metric("Affected Users", df['AFFECTED_USERS'].iloc[0])
    
    st.divider()
    
    # Error Distribution
    st.subheader("Error Types Distribution")
    df = run_query(f"""
        SELECT ERROR_CODE,
               SUBSTR(ERROR_MESSAGE, 1, 100) AS error_message,
               COUNT(*) AS error_count,
               COUNT(DISTINCT USER_NAME) AS affected_users
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS = 'FAIL'
        AND ERROR_CODE IS NOT NULL
        GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        download_csv(df, "error_distribution.csv")
    else:
        st.success("✅ No errors in this period")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Users with Most Errors")
        df = run_query(f"""
            SELECT USER_NAME,
                   COUNT(*) AS failed_queries,
                   COUNT(DISTINCT ERROR_CODE) AS unique_errors,
                   ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 1) AS pct_of_errors
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            AND EXECUTION_STATUS = 'FAIL'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
        """)
        if not df.empty:
            st.bar_chart(df.set_index('USER_NAME')['FAILED_QUERIES'])
            st.dataframe(df, use_container_width=True)
    
    with col2:
        st.subheader("Error Trend")
        df = run_query(f"""
            SELECT DATE(START_TIME) AS date,
                   COUNT(*) AS failed_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
            AND EXECUTION_STATUS = 'FAIL'
            GROUP BY 1 ORDER BY 1
        """)
        if not df.empty:
            st.line_chart(df.set_index('DATE')['FAILED_QUERIES'])
    
    st.divider()
    
    # Recent Failed Queries
    st.subheader("Recent Failed Queries")
    df = run_query(f"""
        SELECT QUERY_ID,
               START_TIME,
               USER_NAME,
               WAREHOUSE_NAME,
               ERROR_CODE,
               SUBSTR(ERROR_MESSAGE, 1, 100) AS error_message,
               SUBSTR(QUERY_TEXT, 1, 150) AS query_preview
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS = 'FAIL'
        ORDER BY START_TIME DESC
        LIMIT 30
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        download_csv(df, "failed_queries.csv")

# ============================================================================
# TAB 8: TRENDS
# ============================================================================
with tab8:
    st.header("📈 Trends & Comparisons")
    
    # Week-over-Week
    st.subheader("📊 Week-over-Week Comparison")
    df = run_query("""
        SELECT 'This Week' AS period,
               COUNT(*) AS queries,
               SUM(CASE WHEN EXECUTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
               SUM(CASE WHEN EXECUTION_STATUS != 'SUCCESS' THEN 1 ELSE 0 END) AS failed,
               ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_time_sec,
               ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 2) AS cloud_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
        UNION ALL
        SELECT 'Last Week',
               COUNT(*),
               SUM(CASE WHEN EXECUTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END),
               SUM(CASE WHEN EXECUTION_STATUS != 'SUCCESS' THEN 1 ELSE 0 END),
               ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2),
               ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 2)
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -14, CURRENT_TIMESTAMP())
        AND START_TIME < DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        
        # Calculate deltas
        if len(df) == 2:
            this_week = df[df['PERIOD'] == 'This Week'].iloc[0]
            last_week = df[df['PERIOD'] == 'Last Week'].iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            query_delta = ((this_week['QUERIES'] - last_week['QUERIES']) / max(last_week['QUERIES'], 1)) * 100
            col1.metric("Query Volume", f"{this_week['QUERIES']:,}", f"{query_delta:+.1f}%")
            
            fail_delta = this_week['FAILED'] - last_week['FAILED']
            col2.metric("Failed Queries", f"{this_week['FAILED']:,}", f"{fail_delta:+.0f}")
            
            time_delta = this_week['AVG_TIME_SEC'] - last_week['AVG_TIME_SEC']
            col3.metric("Avg Query Time", f"{this_week['AVG_TIME_SEC']}s", f"{time_delta:+.2f}s")
            
            credit_delta = this_week['CLOUD_CREDITS'] - last_week['CLOUD_CREDITS']
            col4.metric("Cloud Credits", f"{this_week['CLOUD_CREDITS']}", f"{credit_delta:+.2f}")
    
    st.divider()
    
    # Hourly Pattern
    st.subheader("🕐 Hourly Query Pattern")
    df = run_query(f"""
        SELECT HOUR(START_TIME) AS hour,
               COUNT(*) AS queries,
               ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 1
    """)
    if not df.empty:
        st.bar_chart(df.set_index('HOUR')['QUERIES'])
    
    st.divider()
    
    # Concurrency Analysis
    st.subheader("👥 Peak Concurrency by Hour")
    df = run_query(f"""
        SELECT DATE_TRUNC('HOUR', START_TIME) AS hour,
               WAREHOUSE_NAME,
               COUNT(*) AS concurrent_queries,
               ROUND(AVG(QUEUED_OVERLOAD_TIME)/1000, 2) AS avg_queue_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{min(days_back, 7)}, CURRENT_TIMESTAMP())
        AND WAREHOUSE_NAME IS NOT NULL
        GROUP BY 1, 2
        HAVING COUNT(*) > 10
        ORDER BY 3 DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
    
    st.divider()
    
    # User Activity Pattern
    st.subheader("👤 User Activity")
    df = run_query(f"""
        SELECT USER_NAME,
               COUNT(DISTINCT DATE(START_TIME)) AS active_days,
               COUNT(*) AS total_queries,
               ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT DATE(START_TIME)), 0), 1) AS queries_per_day,
               ROUND(AVG(TOTAL_ELAPSED_TIME)/1000, 2) AS avg_query_sec
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD(DAY, -{days_back}, CURRENT_TIMESTAMP())
        GROUP BY 1 ORDER BY 3 DESC LIMIT 20
    """)
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        download_csv(df, "user_activity.csv")

# ============================================================================
# SIDEBAR STATUS
# ============================================================================
st.sidebar.divider()
st.sidebar.subheader("🚦 Health Status")

if spillage_count > alert_spillage:
    st.sidebar.error(f"🔴 Spillage: {spillage_count}")
else:
    st.sidebar.success(f"✅ Spillage: {spillage_count}")

if failed_queries > alert_failed:
    st.sidebar.warning(f"🟡 Failures: {failed_queries}")
else:
    st.sidebar.success(f"✅ Failures: {failed_queries}")

if cache_hit < alert_cache:
    st.sidebar.warning(f"🟡 Cache: {cache_hit}%")
else:
    st.sidebar.success(f"✅ Cache: {cache_hit}%")

# ============================================================================
# FOOTER
# ============================================================================
st.divider()
st.markdown("""
---
**❄️ Snowflake Performance Monitor v2.0** | Built with Cortex Code  
Data Source: `SNOWFLAKE.ACCOUNT_USAGE` views | Auto-refreshes on interaction
""")
