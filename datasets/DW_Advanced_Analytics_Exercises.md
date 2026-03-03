# 🏢 Corporate Data Warehouse & Advanced Analytics
## Complete Exercise Workbook — Trainer Edition

---

## 📦 YOUR DATASETS (Star Schema)

```
                    ┌──────────────┐
                    │  dim_date    │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────▼───────┐    ┌──────────────┐
│ dim_customer ├────►  fact_sales  ◄────┤ dim_product  │
└──────────────┘    └──────┬───────┘    └──────────────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
       ┌──────▼───┐  ┌─────▼─────┐ ┌───▼──────────┐
       │dim_store │  │dim_employee│ │fact_inventory│
       └──────────┘  └───────────┘ └──────────────┘
                           │
                    ┌──────▼───────┐
                    │fact_attendance│
                    └──────────────┘
```

| File | Description | Rows |
|---|---|---|
| `dim_date.csv` | Calendar table (2021–2024) | 1,461 |
| `dim_customer.csv` | 500 customers with segments | 500 |
| `dim_product.csv` | 40 products, 5 categories | 40 |
| `dim_store.csv` | 20 retail branches | 20 |
| `dim_employee.csv` | 200 employees, 7 departments | 200 |
| `fact_sales.csv` | 5,000 sales transactions | 5,000 |
| `fact_attendance.csv` | HR attendance records | 1,000 |
| `fact_inventory.csv` | Store-level stock data | 800 |

---

# 🟢 MODULE 1 — Data Warehouse Foundations (SQL)

> **Goal:** Build a real Star Schema, practice dimensional modeling

### Exercise 1.1 — Build the DW Schema
```sql
-- Task: Create these tables in PostgreSQL / SQLite
-- Then load the CSVs into them.

CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day INT, month INT, month_name VARCHAR(20),
    quarter INT, year INT, week_number INT,
    day_name VARCHAR(15), is_weekend BOOLEAN
);

CREATE TABLE dim_customer (
    customer_id VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(50), last_name VARCHAR(50),
    gender VARCHAR(10), age INT, city VARCHAR(50),
    segment VARCHAR(30), registration_date DATE,
    email VARCHAR(100), loyalty_score FLOAT
);

-- ✏️ YOUR TASK: Write CREATE TABLE for all 8 tables
-- with proper data types and primary keys
```

**Challenge:** Add foreign key constraints to `fact_sales`.

---

### Exercise 1.2 — Basic KPI Queries
```sql
-- 1. Total Revenue, Total Cost, Total Profit for 2023
SELECT 
    SUM(revenue) AS total_revenue,
    SUM(cost) AS total_cost,
    SUM(profit) AS total_profit,
    ROUND(SUM(profit)/SUM(revenue)*100, 2) AS profit_margin_pct
FROM fact_sales fs
JOIN dim_date dd ON fs.date = dd.full_date
WHERE dd.year = 2023;

-- ✏️ YOUR TASK: Repeat for each year and compare YoY growth
```

---

### Exercise 1.3 — Sales by Dimension (GROUP BY)
```sql
-- ✏️ Write queries to find:
-- Q1. Monthly revenue trend for 2023
-- Q2. Top 5 products by profit
-- Q3. Revenue by customer segment
-- Q4. Best performing store by region
-- Q5. Revenue by payment method and channel
```

---

### Exercise 1.4 — Window Functions (Advanced SQL)
```sql
-- Running Total Revenue by Month
SELECT 
    dd.year,
    dd.month_name,
    SUM(fs.revenue) AS monthly_revenue,
    SUM(SUM(fs.revenue)) OVER (
        PARTITION BY dd.year ORDER BY dd.month
    ) AS running_total
FROM fact_sales fs
JOIN dim_date dd ON fs.date = dd.full_date
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- ✏️ YOUR TASKS:
-- Q1. Rank products by revenue within each category using RANK()
-- Q2. Find Month-over-Month revenue growth % using LAG()
-- Q3. Calculate each customer's % contribution to total revenue
-- Q4. Find the TOP customer per city using ROW_NUMBER()
-- Q5. Calculate 3-month moving average of sales
```

---

### Exercise 1.5 — YoY & MoM Growth Analysis
```sql
-- ✏️ Write a query that shows for each month:
-- current_year_revenue | prev_year_revenue | YoY_growth_pct
-- HINT: Use LAG() with PARTITION BY month
```

---

# 🔵 MODULE 2 — Python: Data Warehouse ETL Pipeline

> **Goal:** Build a real ETL pipeline using Pandas & NumPy

### Exercise 2.1 — Extract & Profile Data
```python
import pandas as pd
import numpy as np

# Load all datasets
fact_sales = pd.read_csv('fact_sales.csv')
dim_customer = pd.read_csv('dim_customer.csv')
dim_product = pd.read_csv('dim_product.csv')
dim_date = pd.read_csv('dim_date.csv')
dim_store = pd.read_csv('dim_store.csv')

# ✏️ YOUR TASK — Data Profiling Report:
# For each dataset print:
# - Shape, dtypes, null counts, duplicate count
# - For numeric cols: mean, median, std, min, max, skewness
# - For categorical cols: value_counts (top 5)
# Write a function: def profile_dataframe(df, name): ...
```

---

### Exercise 2.2 — Transform: Build the Master Sales Table
```python
# ✏️ YOUR TASK: Merge all dimension tables with fact_sales
# Result should have columns from all dimensions

master_sales = fact_sales \
    .merge(dim_customer, on='customer_id', how='left') \
    .merge(dim_product, on='product_id', how='left') \
    .merge(dim_store, on='store_id', how='left')

# Then add date parts:
master_sales['date'] = pd.to_datetime(master_sales['date'])
master_sales['year'] = master_sales['date'].dt.year
# ✏️ Add: month, quarter, week, day_name, is_weekend

# CHALLENGE: Identify and handle any orphan records (mismatched keys)
```

---

### Exercise 2.3 — Feature Engineering
```python
# ✏️ Create these new columns in master_sales:

# 1. profit_margin_pct = profit / revenue * 100
# 2. revenue_per_unit = revenue / quantity
# 3. discount_band = pd.cut(discount_pct, bins=[-0.01,0,0.05,0.15,1],
#                           labels=['No Discount','Low','Medium','High'])
# 4. customer_age_group = pd.cut(age, bins=[0,30,45,60,100],
#                               labels=['Young','Middle','Senior','Elder'])
# 5. is_profitable = profit > 0
# 6. days_since_registration (from sale date - registration_date)
```

---

### Exercise 2.4 — Aggregation & Business Metrics
```python
# ✏️ Calculate these corporate KPIs:

# 1. Monthly Revenue Summary Table
monthly = master_sales.groupby(['year','month']).agg(
    total_revenue=('revenue','sum'),
    total_profit=('profit','sum'),
    total_orders=('sale_id','count'),
    avg_order_value=('revenue','mean'),
    unique_customers=('customer_id','nunique')
).reset_index()

# 2. Product Performance Matrix
# Group by category & product: revenue, profit, units_sold, margin%

# 3. Customer RFM Table (Recency, Frequency, Monetary)
# Recency = days since last purchase
# Frequency = number of purchases
# Monetary = total spend

# 4. Store Performance: revenue, profit, orders per store

# CHALLENGE: Identify the top 20% of customers driving 80% of revenue (Pareto)
```

---

### Exercise 2.5 — RFM Customer Segmentation
```python
import pandas as pd
from datetime import datetime

# ✏️ Complete this RFM segmentation:

snapshot_date = pd.Timestamp('2025-01-01')
master_sales['date'] = pd.to_datetime(master_sales['date'])

rfm = master_sales.groupby('customer_id').agg(
    recency=('date', lambda x: (snapshot_date - x.max()).days),
    frequency=('sale_id', 'count'),
    monetary=('revenue', 'sum')
).reset_index()

# Score each metric 1-5 using pd.qcut
# Then create RFM_segment labels:
# Champions, Loyal, At Risk, Lost, New Customers etc.
```

---

# 🟡 MODULE 3 — Advanced Analytics & Statistics

> **Goal:** Go beyond reporting — find insights using statistics

### Exercise 3.1 — Cohort Analysis
```python
# ✏️ Build a Customer Cohort Retention Table:
# - Cohort = month of customer's FIRST purchase
# - Track what % of those customers bought again in subsequent months
# - Output: cohort_month × period_number matrix (like a heatmap)

# Steps:
# 1. Find first purchase date per customer
# 2. Assign cohort (year-month of first purchase)  
# 3. Calculate period number for each subsequent purchase
# 4. Pivot table: cohort × period → retention %
```

---

### Exercise 3.2 — ABC Analysis (Inventory Optimization)
```python
# ✏️ Classify products using ABC Analysis:
# A = top 20% products contributing 70% of revenue
# B = next 30% contributing 20% of revenue  
# C = bottom 50% contributing 10% of revenue

# Steps:
# 1. Calculate total revenue per product
# 2. Sort descending, calculate cumulative %
# 3. Assign A/B/C label
# 4. Cross-analyze with inventory stock levels
#    → Are A-class products well-stocked?
```

---

### Exercise 3.3 — Sales Forecasting with Moving Averages
```python
import matplotlib.pyplot as plt

# ✏️ Build a forecasting model:
# 1. Create monthly revenue time series (2021–2024)
# 2. Plot the trend
# 3. Apply 3-month and 6-month Simple Moving Average
# 4. Apply Exponential Weighted Moving Average (ewm)
# 5. Forecast next 3 months using linear trend
# 6. Plot actual vs forecasted on the same chart

monthly_ts = master_sales.groupby(['year','month'])['revenue'].sum().reset_index()
monthly_ts['period'] = pd.to_datetime(monthly_ts[['year','month']].assign(day=1))
monthly_ts = monthly_ts.set_index('period').sort_index()
monthly_ts['MA3'] = monthly_ts['revenue'].rolling(3).mean()
monthly_ts['MA6'] = monthly_ts['revenue'].rolling(6).mean()
monthly_ts['EWM'] = monthly_ts['revenue'].ewm(span=3).mean()
```

---

### Exercise 3.4 — Correlation & Heatmap Analysis
```python
import matplotlib.pyplot as plt
import numpy as np

# ✏️ Find what drives profit:
# 1. Select numeric columns: revenue, profit, quantity, 
#    discount_pct, loyalty_score, age
# 2. Compute correlation matrix
# 3. Plot a heatmap with annotations
# 4. Write 3 business insights from the correlations

# CHALLENGE: Does discount actually increase total profit?
# Group by discount_band and compare avg profit margin
```

---

### Exercise 3.5 — Employee Performance Analytics (HR)
```python
# Load HR data
employees = pd.read_csv('dim_employee.csv')
attendance = pd.read_csv('fact_attendance.csv')

# ✏️ Build HR Analytics Dashboard Data:

# 1. Attendance rate per department (% Present days)
# 2. Average overtime hours by department
# 3. Salary vs Performance Rating scatter analysis
# 4. Department headcount and avg salary
# 5. Identify employees with attendance < 70% but high salary
#    → Flag for HR review

# CHALLENGE: Calculate cost of absenteeism:
# (absent_days / total_days) × salary = estimated cost
```

---

# 🟠 MODULE 4 — Data Visualization (Matplotlib + Business Charts)

### Exercise 4.1 — Executive Dashboard (6-panel figure)
```python
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

fig = plt.figure(figsize=(20, 14))
fig.suptitle('Corporate Executive Dashboard — 2024', fontsize=20, fontweight='bold')

gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.4, wspace=0.35)

# ✏️ Create these 6 charts:
# Panel 1 (top-left, wide): Monthly Revenue vs Profit line chart with dual axis
# Panel 2 (top-right): Revenue by Category — horizontal bar chart
# Panel 3 (mid-left): Customer Segment donut chart (revenue share)
# Panel 4 (mid-center): Top 10 Products bar chart
# Panel 5 (mid-right): Sales Channel breakdown (stacked bar)
# Panel 6 (bottom, wide): Store performance heatmap by region

plt.savefig('executive_dashboard.png', dpi=150, bbox_inches='tight')
```

---

### Exercise 4.2 — Waterfall Chart (CFO Favorite)
```python
# ✏️ Build a Profit Waterfall Chart showing:
# Starting Revenue → Deduct Cost → Deduct Discounts → Net Profit
# Each bar should be: green (positive), red (negative), blue (total)
# This is a classic Finance chart used in board presentations
```

---

### Exercise 4.3 — Bubble Chart: Product Portfolio Analysis
```python
# ✏️ Build a bubble chart where:
# X-axis = Total Revenue
# Y-axis = Profit Margin %
# Bubble Size = Units Sold
# Color = Product Category
# Labels = Product Name (for top 10 only)
# Add quadrant lines at median revenue and median margin
# Label quadrants: "Stars", "Cash Cows", "Dogs", "Question Marks"
```

---

# 🔴 MODULE 5 — Capstone Projects

### 🏆 Project 1: End-to-End Sales Analytics Report
**Deliverable:** Python script that auto-generates a full business report

Tasks:
1. Load and clean all datasets
2. Build master_sales table
3. Calculate 10 KPIs (revenue, profit, margin, growth, AOV, etc.)
4. Generate 5 business charts
5. Export summary to Excel with multiple sheets
6. Write business recommendations in comments

---

### 🏆 Project 2: Customer Intelligence Dashboard
**Deliverable:** Complete customer analytics pipeline

Tasks:
1. RFM Segmentation (score and label all 500 customers)
2. Cohort retention analysis
3. Customer lifetime value (CLV) estimation
4. High-value customer profile (what do top customers look like?)
5. Churn risk scoring (customers who haven't bought in 6+ months)

---

### 🏆 Project 3: Inventory Optimization Report
**Deliverable:** Inventory health report for Operations team

Tasks:
1. ABC analysis of products
2. Identify out-of-stock risk (stock < reorder_level)
3. Overstock detection (stock > 3× average monthly sales)
4. Category-level inventory vs sales correlation
5. Recommend reorder quantities per product

---

### 🏆 Project 4: HR Analytics Dashboard
**Deliverable:** People analytics report for HR Director

Tasks:
1. Workforce composition (dept, gender, city, role)
2. Attendance analysis by department
3. Salary equity analysis (same role, different pay?)
4. Performance vs Salary quadrant analysis
5. Absenteeism cost estimation

---

# 📋 KPIs Reference Cheat Sheet

| KPI | Formula |
|---|---|
| Gross Profit | Revenue - Cost |
| Profit Margin % | (Profit / Revenue) × 100 |
| AOV (Avg Order Value) | Total Revenue / Total Orders |
| Customer Acquisition Rate | New Customers / Total Customers |
| YoY Growth % | (This Year - Last Year) / Last Year × 100 |
| MoM Growth % | (This Month - Last Month) / Last Month × 100 |
| Inventory Turnover | Cost of Goods Sold / Avg Inventory Value |
| Attendance Rate | Present Days / Total Working Days × 100 |
| CLV (simple) | AOV × Purchase Frequency × Avg Customer Lifespan |
| Discount Impact | Revenue Without Discount - Actual Revenue |

---

# 🗺️ Learning Path

```
Week 1-2:  Module 1 (SQL DW)        → Build schema, write KPI queries
Week 3-4:  Module 2 (Python ETL)    → Build pipeline, feature engineering
Week 5-6:  Module 3 (Analytics)     → RFM, Cohort, Forecasting
Week 7:    Module 4 (Visualization) → Executive dashboards
Week 8:    Module 5 (Capstone)      → Choose 2 projects to complete
```

---

*Datasets generated for training purposes. All names and values are synthetic.*
