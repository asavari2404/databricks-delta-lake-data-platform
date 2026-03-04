Data Quality Engineering with Databricks & Delta Lake

This project demonstrates the implementation of data quality engineering pipelines using Databricks, Apache Spark, and Delta Lake. The goal is to build automated validation workflows that profile raw datasets, enforce data quality expectations, detect anomalies, and route invalid records for remediation.
The implementation showcases practical experience with Databricks Data Quality (DQX) and modern data engineering patterns used in production data platforms.

Architecture Overview:
              Raw Data Sources
        (Seattle Pet License Dataset,
        Air Traffic Landing Statistics)

                     │
                     ▼
             Bronze Layer
        Raw Data Ingestion (Delta)

                     │
                     ▼
             Data Profiling
        Schema & Distribution Analysis

                     │
                     ▼
          Data Quality Validation
           (Databricks DQX Rules)

                     │
         ┌───────────┴───────────┐
         ▼                       ▼
   Valid Records           Invalid Records
     (Silver Layer)        (Quarantine Table)

                     │
                     ▼
              Curated Data
               (Gold Layer)
        Analytics & Reporting Ready


Project 1 – Data Profiling and Validation
Seattle Pet License Dataset
Objective

Perform dataset profiling and build automated quality checks to ensure data integrity before downstream analytics consumption.

Key Tasks
1. Data Profiling
Performed exploratory analysis using Spark to identify:
Missing values
Data type inconsistencies
Duplicate records
Value distribution anomalies
Data Quality Rules
Implemented validation rules including:
Null value checks
Data type validation
Duplicate record detection
Range validation
Custom Validation Rules

2. Additional business logic checks such as:
Date validity validation
Logical consistency checks
Domain-specific constraints
Execution
Ran automated validation pipelines
Generated data quality metrics
Flagged problematic records for investigation

Project 2 – Automated Data Quality Pipeline
Air Traffic Landing Statistics Dataset
Objective: 
Build an automated data validation pipeline using Databricks Data Quality (DQX) to detect and remediate common data quality violations.
The dataset intentionally contains data issues to simulate real-world scenarios.
Simulated Data Quality Issues:
1. Null values
2. Invalid data types
3. Out-of-range values
4. Future timestamps
5. Duplicate records

Data Quality Pipeline
Step 1 – Data Profiling
Analyzed dataset structure and distribution to identify anomalies.

Step 2 – Define Data Quality Expectations
Created validation rules using Databricks DQX, including:
Schema validation
Null checks
Range constraints
Date validation
Primary key uniqueness

Step 3 – Detect Bad Records
Executed validation checks to identify records violating expectations.

Step 4 – Quarantine Invalid Records
Implemented logic to:
1. Route failed records to quarantine tables
2. Track validation metrics
3. Preserve clean data for analytics workloads

Key Data Engineering Concepts Demonstrated
Data Profiling
Data Quality Engineering
Automated Validation Pipelines
Expectation-based Data Validation
Data Cleansing and Remediation
Data Governance Practices

Technologies Used:
| Technology             | Purpose                                    |
| ---------------------- | ------------------------------------------ |
| Databricks             | Data processing and pipeline orchestration |
| Apache Spark (PySpark) | Distributed data processing                |
| Delta Lake             | Reliable data storage and versioning       |
| Databricks DQX         | Data quality validation framework          |
| SQL                    | Data querying and validation logic         |

Medallion Architecture Implementation:
The project follows the Medallion Architecture pattern, a widely used design approach in modern data lakehouse platforms such as Databricks.
1. Bronze Layer – Raw Data Ingestion
Raw datasets are ingested into Delta tables without modification.
Data is stored exactly as received to maintain a reliable source of truth.

2. Silver Layer – Data Cleaning & Validation
Data quality rules are applied using Databricks DQX.
Invalid records are detected and routed to quarantine tables.
Schema validation and transformation logic are applied.

3. Gold Layer – Analytics Ready Data
Curated datasets are created after validation and cleaning.
Data is structured for analytics, reporting, and downstream consumption.

Repository Structure:
databricks-data-quality-project/
│
├── notebooks/
│   ├── data_profiling.ipynb
│   ├── dq_rules_generation.ipynb
│   ├── dq_validation_pipeline.ipynb
│
├── datasets/
│   ├── seattle_pet_license.csv
│   ├── air_traffic_landing_stats.csv
│
├── README.md

Skills Demonstrated:
Data Engineering
Databricks Development
Spark Data Processing
Data Quality Engineering
Delta Lake Architecture
Data Validation Pipelines

Key Outcomes
Built automated pipelines to identify and manage data quality issues.
Implemented scalable Spark-based validation workflows.
Demonstrated practical experience with Databricks, Delta Lake, and data quality engineering frameworks.
