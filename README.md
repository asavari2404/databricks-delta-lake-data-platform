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

Slowly Changing Dimension (SCD) Implementation with Delta Lake
This project also demonstrates the implementation of Slowly Changing Dimensions (SCD) Type 1 and Type 2 using Delta Lake and Change Data Feed (CDF) in Databricks. The objective was to design scalable data pipelines capable of handling record updates, deletions, and historical tracking while maintaining clean analytics-ready tables in the Gold layer.
The implementation follows modern data lakehouse architecture principles, separating raw ingestion, transformation, and curated analytical datasets.
SCD Type 1 Implementation
The SCD Type 1 pipeline was designed to overwrite existing records when updates occur while preserving deleted records in a separate archival table.

Key Design Decisions
1. Update Strategy
2. Incoming records replace existing records in the target table without preserving historical versions.
3. Archive Handling for Deleted Records
4. Deleted records are captured and stored in a dedicated archive table within the Gold layer.
5. The archive table retains Change Data Feed (CDF) metadata attributes to preserve change lineage.
6. Clean Analytics Table
The Gold SCD Type 1 table excludes CDF metadata fields to maintain a clean schema for analytics consumption.

Benefits:
1. Simplified data model for reporting use cases
2. Efficient handling of updates while preserving deleted record history
3. Clean schema for downstream analytics workloads

SCD Type 2 Implementation
The SCD Type 2 pipeline was implemented to track historical changes by maintaining multiple versions of records.

Key Design Features
1. Historical Version Tracking
2. Each update generates a new record version while retaining previous versions for historical analysis.
3. Schema Standardization
CDF metadata attributes were removed from the final Gold layer tables to maintain a clean schema.
4. Effective Date Management
Internal system columns were renamed for clarity:
__start_date → start_date
__end_date → end_date
5.Date fields were converted to date datatype to ensure consistent temporal tracking.
6. Row Status Logic
Implemented status tracking to identify active and inactive records:
A → Active record
I → Inactive historical record

Benefits
1. Full historical tracking of dimension changes
2. Improved data lineage and change traceability
3. Clean schema design for analytics and reporting systems

Data Engineering Concepts Demonstrated
1. Slowly Changing Dimensions (SCD Type 1 & Type 2)
2. Delta Lake Change Data Feed (CDF)
3. Data Archival Strategies
4. Data Versioning and Historical Tracking
5. Schema Standardization for Analytics
6. Lakehouse Gold Layer Design

Technologies Used
1. Databricks
2. Apache Spark (PySpark)
3. Delta Lake
4. Change Data Feed (CDF)
5. SQL

Key Outcome:
Built scalable data pipelines capable of handling record updates, deletions, and historical tracking while maintaining clean and analytics-ready Gold layer tables in a Delta Lake-based data platform.
