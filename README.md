
# Ramil-Data-Pipeline-Databricks

## Project Description

This project demonstrates a data pipeline built using Databricks. The pipeline integrates a data source and a data sink, showcasing how to extract, transform, and load (ETL) data efficiently within the Databricks environment.

---

## Requirements

- **Data Pipeline:** Created using Databricks.
- **Data Source and Sink:** Includes at least one data source and one data sink.
- **CI/CD Integration:** Configured CI/CD pipeline for automated testing and deployment.

---

## Pipeline Overview

1. **Data Source:** 
   - The pipeline ingests data from [describe source, e.g., CSV files, API, or database].
   - Ensure the data source is configured in the Databricks workspace.

2. **Data Transformation:** 
   - Transformation logic implemented using PySpark.
   - Sample transformations include filtering, aggregation, and joining.

3. **Data Sink:** 
   - Processed data is stored in [describe sink, e.g., Azure Blob Storage, Databricks Delta table].

---

## CI/CD Pipeline

A CI/CD pipeline is integrated to ensure robust testing and seamless deployment. The pipeline includes:
- Automated testing for code functionality and performance.
- Deployment scripts for transferring changes to the production workspace.

---

## ETL Pipeline

The pipeline has been set up and utilizes the scripts located under the mylib directory. First, data is retrieved using Pandas, after which a Spark DataFrame is created and loaded into the warehouse. Finally, based on the loaded data, transformations are applied, and the data is loaded into different table withing same warehouse.




---

## How to Run

1. **Setup Databricks Workspace:**
   - Configure your Databricks environment with the necessary cluster settings.
   - Upload the script or notebook into the Databricks workspace.

2. **Configure Data Source and Sink:**
   - Specify the paths or connections for the data source and sink.

3. **Run Pipeline:**
   - Execute the notebook or script to initiate the pipeline.

4. **Validate Output:**
   - Verify the processed data in the sink location.

---

## License

This project is open-source and available under the MIT License.

---

---

## Images

The following images are included in the `data` folder for reference:
- Data pipeline architecture diagram.
- Sample data source and sink configurations.

