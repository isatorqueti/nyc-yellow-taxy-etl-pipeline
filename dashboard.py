import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path

st.title("🚕 NYC Yellow Taxi Dashboard")

@st.cache_resource
def load_data():
    path = Path(__file__).resolve().parent
    parquet_path = path / 'data' / 'processed' / 'yellow_taxi_dataset.parquet'
    con = duckdb.connect(database=':memory:')
    con.execute(f"CREATE VIEW yellow_taxi_data AS SELECT * FROM read_parquet('{parquet_path}')")
    return con

con = load_data()

@st.cache_data
def get_years_from_db():
    return con.execute("""
        SELECT DISTINCT EXTRACT(YEAR FROM TRY_CAST(date AS DATE))::INT AS year
        FROM yellow_taxi_data
        WHERE TRY_CAST(date AS DATE) IS NOT NULL
        ORDER BY 1
    """).df()['year'].tolist()

@st.cache_data
def get_months_from_db(year_formatted: str):
    # Se não houver ano selecionado, retorna vazio para não dar erro
    if not year_formatted:
        return []
    return con.execute(f"""
        SELECT DISTINCT EXTRACT(MONTH FROM TRY_CAST(date AS DATE))::INT AS month
        FROM yellow_taxi_data
        WHERE EXTRACT(YEAR FROM TRY_CAST(date AS DATE)) IN ({year_formatted})
        ORDER BY 1
    """).df()['month'].tolist()

@st.cache_data
def get_vendors_from_db():
    return con.execute("""
        SELECT DISTINCT VendorID 
        FROM yellow_taxi_data 
        WHERE VendorID IS NOT NULL 
        ORDER BY 1
    """).df()['VendorID'].tolist()

@st.cache_data
def get_payments_from_db():
    return con.execute("""
        SELECT DISTINCT payment_label 
        FROM yellow_taxi_data 
        WHERE payment_label IS NOT NULL 
        ORDER BY 1
    """).df()['payment_label'].tolist()

# Filtros
st.sidebar.header("Filters")
condition = []
# Calendar
year_db = get_years_from_db()
selected_year = st.sidebar.multiselect("Year", options=year_db, default=year_db)
year_formatted = ", ".join(map(str, selected_year))

month_db = get_months_from_db(year_formatted)
selected_month = st.sidebar.multiselect(
    "Month", 
    options=month_db, 
    default=month_db
)

only_weekend = st.sidebar.checkbox("Only Weekend")

if selected_year:
    condition.append(f"EXTRACT(YEAR FROM TRY_CAST(date AS DATE)) IN ({year_formatted})")

if selected_month:
    meses_formatados = ", ".join(map(str, selected_month))
    condition.append(f"EXTRACT(MONTH FROM TRY_CAST(date AS DATE)) IN ({meses_formatados})")

if only_weekend:
    condition.append("is_weekend = TRUE")

# Vendor
vendors_db = get_vendors_from_db()
selected_vendor = st.sidebar.multiselect("Vendor", options=vendors_db, default=vendors_db)
if selected_vendor:
    vendors_formatted = ", ".join(map(str, selected_vendor))
    condition.append(f"VendorID IN ({vendors_formatted})")

# Payment
payment_db = get_payments_from_db()
selected_payment = st.sidebar.multiselect(
    "Payment Type", 
    options=payment_db, 
    default=payment_db
)
if selected_payment:
    payment_formatted = ", ".join([f"'{p}'" for p in selected_payment])
    condition.append(f"payment_label IN ({payment_formatted})")


if condition:
    where = "WHERE " + " AND ".join(condition)
else:
    where = ""

query = f"""
    SELECT 
        COUNT(*) AS total_trips,
        SUM(total_amount) AS total_revenue
    FROM yellow_taxi_data
    {where}
"""

filter_df = con.execute(query).df()

def abbreviate(num):
    if num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.1f}K"
    else:
        return f"{num:.2f}"

col1, col2, col3 = st.columns(3)
total_trips = con.execute(f"SELECT COUNT(*) FROM yellow_taxi_data {where}").fetchone()[0]
col1.metric("Total Trips", abbreviate(total_trips))
total_revenue = con.execute(f"SELECT SUM(total_amount) FROM yellow_taxi_data {where}").fetchone()[0]
col2.metric("Total Revenue", f"US$ {abbreviate(total_revenue)}")
avg_fare = con.execute(f"SELECT AVG(fare_amount) FROM yellow_taxi_data {where}").fetchone()[0]
col3.metric("Average Fare", f"US$ {abbreviate(avg_fare)}")

tab_hour, tab_vendor, tab_payment, tab_weekday = st.tabs(
        ["Hourly", "By Vendor", "By Payment Type", "By Day Of Week"]
    )

# View por hora
with tab_hour:
    con.execute(f"""
        CREATE OR REPLACE VIEW hourly AS
        SELECT
            hour_of_day,
            COUNT(*) AS total_trips,
            AVG(total_amount) AS avg_ticket,
            AVG(fare_amount) AS avg_fare
        FROM yellow_taxi_data
        {where}
        GROUP BY 1
        ORDER BY 1
    """)
    df_hourly = con.execute("SELECT * FROM hourly").df()
    chart_col1, chart_col2 = st.columns(2)
    chart_col1.line_chart(df_hourly.set_index('hour_of_day')[['total_trips']])
    chart_col2.bar_chart(df_hourly.set_index('hour_of_day')[['avg_fare']])

# View por fornecedor
with tab_vendor:
    con.execute(f"""
        CREATE OR REPLACE VIEW vendor AS
        SELECT
            VendorID,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            AVG(trip_distance) AS avg_distance,
            AVG(fare_amount) AS avg_fare,
            AVG(speed_mph) AS avg_speed
        FROM yellow_taxi_data
        {where}
        GROUP BY 1
        ORDER BY 1
    """)
    df_vendor = con.execute("SELECT * FROM vendor").df().round(2)
    st.bar_chart(df_vendor.set_index('VendorID')[['total_trips']])
    st.dataframe(df_vendor)

# View por tipo de pagamento
with tab_payment:
    con.execute(f"""
        CREATE OR REPLACE VIEW payment_type AS
        SELECT
            payment_label,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            AVG(fare_amount) AS avg_fare,
            AVG(tip_amount) AS avg_tip,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percent_trips
        FROM yellow_taxi_data
        {where}
        GROUP BY 1
        ORDER BY 2 DESC
    """)
    df_payment = con.execute("SELECT * FROM payment_type").df().round(2)
    df_payment['payment_label'] = pd.Categorical(
    df_payment['payment_label'], 
    categories=df_payment['payment_label'].tolist(), 
    ordered=True
    )
    st.bar_chart(df_payment.set_index('payment_label')[['total_trips']])
    st.dataframe(df_payment)

# View por dia da semana
with tab_weekday:
    con.execute(f"""
    CREATE OR REPLACE VIEW weekly AS
    SELECT
        CASE
        WHEN day_of_week = 0 THEN 'Monday'
        WHEN day_of_week = 1 THEN 'Tuesday'
        WHEN day_of_week = 2 THEN 'Wednesday'
        WHEN day_of_week = 3 THEN 'Thursday'
        WHEN day_of_week = 4 THEN 'Friday'
        WHEN day_of_week = 5 THEN 'Saturday'
        WHEN day_of_week = 6 THEN 'Sunday'
        END AS day_of_week,
        COUNT(*) AS total_trips,
        AVG(trip_distance) AS avg_distance,
        AVG(fare_amount) AS avg_fare
    FROM yellow_taxi_data
    {where}
    GROUP BY 1
    """)
    df_weekly = con.execute("SELECT * FROM weekly").df().round(2)
    df_weekly['day_of_week'] = pd.Categorical(
    df_weekly['day_of_week'],
    categories=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
    ordered=True
    )
    df_weekly = df_weekly.sort_values('day_of_week')
    st.bar_chart(df_weekly.set_index('day_of_week')[['total_trips']])
    st.dataframe(df_weekly)

st.divider()
st.subheader("📈 Year-On-Year Trend Comparison")

trend_query = f"""
    WITH prepared_data AS (
        SELECT 
            EXTRACT(YEAR FROM TRY_CAST(date AS DATE))::VARCHAR AS year,
            strftime(TRY_CAST(date AS DATE), '%m-%d') AS day_month,
            total_amount
        FROM yellow_taxi_data
        {where}
    )
    SELECT 
        year,
        day_month,
        COUNT(*) AS total_trips,
        SUM(total_amount) AS total_revenue
    FROM prepared_data
    WHERE year IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2, 1
"""
df_trend = con.execute(trend_query).df()

chosen_metric = st.radio("",
    options=["Total Trips", "Total Revenue (US$)"],
    horizontal=True,
    label_visibility="collapsed"
)
value_column = 'total_trips' if chosen_metric == "Total Trips" else 'total_revenue'
df_pivot = df_trend.pivot(
        index='day_month', 
        columns='year', 
        values=value_column
)
st.line_chart(df_pivot)