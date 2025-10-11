# Data Engineering Project: Analyzing Customer Buying Behavior in Retail
Building an end-to-end data pipeline for analyzing customer buying behavior using a public dataset on online retail transactions

# *Project Overview*
This project focuses on building an end-to-end data pipeline for analyzing customer buying behavior using a public dataset on online retail transactions. The dataset captures invoice details, product descriptions, quantities, prices, and customer information from a UK-based online retailer.
# *Problem Being Solved*
Retail businesses often face challenges in understanding customer purchasing patterns due to fragmented or unclean data. This leads to issues like overstocking low-demand items, understocking popular products, ineffective marketing, and lost revenue from unidentified customer segments. The data pipeline solves this by automating the ingestion, cleaning, validation, and storage of transaction data, enabling reliable analysis of buying trends, such as peak purchase times, top products by revenue, and customer lifetime value.

# *Business Impact and Leverage*
Impact: By providing clean, validated data in a scalable warehouse, businesses can reduce inventory costs by 10-20% through better demand forecasting, increase sales by 15% via targeted promotions based on customer segments, and improve customer retention by identifying at-risk buyers early. For example, insights from this pipeline could reveal that certain products generate 80% of revenue, allowing reallocation of marketing budgets.
Leverage: The pipeline leverages cloud-based storage (BigQuery) for cost-effective querying at scale and Airflow for orchestration, ensuring reliability and schedulability. This setup allows integration with BI tools like Tableau or machine learning models for predictive analytics, turning raw data into actionable business intelligence.

# *Data Source*
I used web search tools to identify a suitable public dataset. The chosen one is the "Online Retail" dataset from the UCI Machine Learning Repository, which includes over 500,000 transaction records from 2010-2011. It's freely available and suitable for analyzing customer behavior.

Source URL: https://archive.ics.uci.edu/dataset/352/online+retail
Format: Excel (XLSX), but we'll convert to CSV for processing.
Key Fields: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country.
Size: Approximately 25MB, with 541,909 rows.
License: Public domain, no restrictions for use.

To access it programmatically, the code below downloads the ZIP file, extracts the XLSX, and converts it to CSV.

# *Project Plan*
1.  Data Sourcing and Extraction: Download the dataset from UCI, extract, and load into a Pandas DataFrame.
2.  Data Cleansing: Handle missing values, remove duplicates, correct data types, and filter invalid entries (e.g., negative quantities).
3.  Data Validation: Use schema checks and business rules (e.g., ensure prices are positive, dates are valid) with libraries like Pandera.
4.  Data Warehousing: Load cleaned data into Google BigQuery for scalable storage and querying. Assume a GCP project is set up with service account credentials.
5.  Orchestration with Airflow: Define a DAG to schedule and orchestrate the ETL process.
6.  Insights and Visualization: Query the data warehouse to generate insights (e.g., top products by revenue, customer purchase frequency) and visualize using    Matplotlib/Seaborn.

Assumptions:
1.  Python environment with libraries: pandas, requests, zipfile, openpyxl, pandera, google-cloud-bigquery, apache-airflow.
2.  GCP setup: BigQuery dataset retail_analysis and table transactions. Use a service account key JSON for authentication.
3.  Run on a local machine or cloud VM; Airflow needs to be installed and running.
