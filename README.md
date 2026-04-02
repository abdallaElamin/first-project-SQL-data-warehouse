**Data Warehouse Project**

This project focuses on designing and implementing a modern data warehouse solution using SQL Server as the core database engine. The solution integrates data from six CSV files and applies a layered architecture approach to ensure scalability, data quality, and efficient analytics.

The project follows the **Medallion Architecture** (Bronze, Silver, Gold layers), where data flows through three separate databases, each serving a distinct purpose:

**1. Bronze Layer (Raw Data)**
The Bronze database is used to ingest raw data directly from the CSV files using a *Truncate and Insert* approach.

* Data is loaded **as-is** without any transformation or modeling
* Object type: Tables
* Purpose: Preserve original source data for traceability and auditing

**2. Silver Layer (Cleansed Data)**
The Silver database stores cleaned and standardized data loaded from the Bronze layer.

* Data transformations include: data cleaning, type casting, renaming, and validation
* Data is still not modeled at this stage
* Object type: Tables
* Purpose: Provide high-quality, consistent data for downstream processing

**3. Gold Layer (Business Model)**
The Gold database represents the final analytical layer where data is modeled for reporting and business intelligence.

* Implements **star schema design** with fact and dimension structures
* Applies business logic, data integration, and aggregation
* Object type: Views (no direct data loading)
* Purpose: Deliver optimized datasets for analytics and reporting

The ETL process extracts data from CSV sources, loads it into the Bronze layer, transforms it in the Silver layer, and finally presents it in the Gold layer for analytical consumption.

This architecture ensures:

* Clear separation of concerns across data layers
* Improved data quality and consistency
* Scalability and maintainability
* Support for advanced analytics using tools like SQL Server Analysis Services and Power BI

This project demonstrates strong capabilities in data engineering, ETL design, data modeling, and building enterprise-level data warehouse solutions.
