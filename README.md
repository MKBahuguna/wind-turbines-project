# Wind Turbines Data Processing Pipeline

## Overview

This project implements a scalable and testable data processing pipeline for a renewable energy company managing a wind turbine farm. The pipeline processes raw turbine data, cleans and analyzes it, and stores the results for further analysis. It is built using **Python** and **PySpark**, packaged as a Python wheel, and executed on **Databricks** as part of a scheduled workflow.

---

## Problem Statement

The turbine farm generates power measured in megawatts (MW) based on wind conditions. The raw data contains missing values, outliers, and anomalies that need to be addressed.

---

## Design

### Pipeline Stages

1. **Data Loading**
   - Loads raw CSV files containing turbine measurements for groups of turbines.
   - Each file corresponds to a specific group of turbines and is updated daily with the last 24 hours of data.
   - It's filtered based on `start_date` and `end_date` provided within the pipeline.

2. **Data Processing**
   - Replaces missing values and outliers using mean imputation.
   - Detects anomalies based on power output deviation.
   - Calculates summary statistics such as min, max, and average power output.

3. **Output**
   - Stores cleaned data, anomalies, and summary statistics in Databricks Unity Catalog Delta tables for downstream consumption.

### Assumptions
- Data for each turbine group is stored in Databricks Workspace Shared folder (e.g., `/Workspace/Shared/data/*.csv`). In order to change the path, database name and schema name to actuals ones, the variable values need to be updated in `src/pipeline/config.py`.
- Outliers are defined as values beyond 3 standard deviations from the mean.
- Anomalies are defined as power outputs outside 2 standard deviations from the mean.
- Delta tables are used for persistence to ensure scalability and ACID compliance.
- The pipeline is saved in `databricks_workflow.yml` file and can be copied to Databricks workflow. The wheel path has to be changed to the actual one.
- CICD pipeline is not set up, logging is not implemented, Unit tests cover less then 80% of the code due to time limitation constraint, however normally I would do it as well.
- It uses `Pytest` for unit tests, `Flake8` for style guide.

---