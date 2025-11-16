# Real Time ECommerce Data Processing & Analytics using Medallion Architecture on Azure Cloud

## Architecture
This project follows a modern **Medallion Architecture (Bronze → Silver → Gold)** built on Azure cloud to enable scalable, real-time e-commerce data processing and analytics.

🔹 **Data Ingestion (Bronze Layer)**
Multiple data sources (GitHub HTTP endpoints and SQL tables) are ingested using Azure Data Factory and stored as raw files in Azure Data Lake Storage Gen2.

🔹 **Data Transformation (Silver Layer)**
Raw data is cleaned, modeled, and enriched in Azure Databricks using PySpark.
Additional enrichment tables are fetched from MongoDB and merged with processed datasets.

🔹 **Data Storage & Serving (Gold Layer)**
The transformed and curated Gold datasets are stored back into ADLS Gen2, optimized in Parquet format for analytics workloads.

🔹 **Analytics & Visualization**
Azure Synapse Serverless SQL reads directly from ADLS to enable fast querying without needing provisioning. Finally, Power BI is used to build interactive dashboards for revenue analysis, customer insights, and operational KPIs.

<img width="1219" height="693" alt="image" src="https://github.com/user-attachments/assets/2b3540df-6098-42b0-8d68-65d9795aaf54" />

## Medallion Architecture
![Alt text](https://github.com/SIDDHARTH107/Real-Time-E-Commerce-Data-Processing-and-Analytics-using-Medallion-architecture-on-Azure-Cloud/blob/main/1719345910378.gif?raw=true)

<img width="1355" height="727" alt="image" src="https://github.com/user-attachments/assets/73f99e14-da68-4a71-8c3c-6c1d78c74c90" />

**Microsoft Azure** gives us 2 modes: Pay as you go and trying for free. But, if we are choosing free, it will give us the free $200 credits to use it for 1 month. 
<img width="1890" height="730" alt="image" src="https://github.com/user-attachments/assets/9574d94b-e921-45da-a467-3e3adedb0f19" />

## 🚀 Tech Stack
- **Data Ingestion**: Azure Data Factory
- **Storage**: Azure Data Lake Storage Gen2
- **Transformation**: Azure Databricks (PySpark)
- **Analytics**: Azure Synapse Analytics
- **Visualization**: Microsoft Power BI
- **Databases**: PostgreSQL, MongoDB Atlas, MongoDB Compass
- **Languages**: Python, SQL
- **Spreadsheet**: Microsoft Excel, Google Sheets
- **IDE (Integrated Development Environment**: Windsurf

## 📊 Key Features
- Automated data ingestion from HTTP and SQL/NoSQL sources
- Medallion architecture implementation
- Real-time data processing with PySpark
- Interactive Power BI dashboard
- Serverless SQL querying
  
## POWER BI DASHBOARD
<img width="838" height="476" alt="image" src="https://github.com/user-attachments/assets/2f662506-ff05-4e9b-8c56-1c534773122c" />

## Business Insights from Gold Layer table
<img width="859" height="326" alt="image" src="https://github.com/user-attachments/assets/ff9e7819-252b-4e44-af79-c72199816df8" />

## 🔗 Connect
- 💼 [LinkedIn](https://www.linkedin.com/in/siddharthmohapatra-dataanalyst/)
- 🐦 [Twitter](https://x.com/SidDS2000)
- 📧 [Email](siddharth.m33@gmail.com)
