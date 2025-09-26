# Data-Engineering-Project-Azure
# Project Overview 
This project demonstrates data engineering pipelines built on Azure. It uses Azure Data Factory to build pipelines and Databricks (PySpark) for transformations. The pipeline is designed to handle incremental data loads, implement Slowly Changing Dimensions (SCD) Type 1, and organize data in a Star Schema model for analytics.

##  Architecture  

 <img width="1920" height="1124" alt="Project Architecture" src="https://github.com/user-attachments/assets/2735513a-c935-495d-9133-82f9830471c6" />

- **Data Ingestion**: Raw data ingested into Raw Container of Azure Data Lake Storage (ADLS)  
- **Data Orchestration**: Pipeline built in Azure Data Factory (ADF)  
- **Data Transformation**:  
  - Data transformed using Azure Databricks (PySpark)
  - Transformed Data stored into Silver layer of ADLS
  - Implemented incremental loading logic  
  - Applied SCD Type 1 for dimension tables  
  - Created star schema fact & dimension tables  
- **Data Storage**: Final curated data stored in the Gold Layer of ADLS
  
## Azure Data Factory Pipelines

- **source_prep** : Ingests raw CSV data into Azure SQL Database (`source_data` table) using Copy activity with column mappings.
<img width="1823" height="770" alt="source_prep" src="https://github.com/user-attachments/assets/51f659df-e607-4fbd-a856-0ef35e1851b9" />

- **increm_data_pipeline** :
    -  Implements incremental loading with watermark logic
    -  Writes Parquet to Bronze layer
    -  Updates watermark table
    -  Triggers Databricks notebooks to build Silver and Gold (dimensions and fact table) layer.
<img width="1733" height="687" alt="increm_pipeline" src="https://github.com/user-attachments/assets/3e5a14ab-79c9-4760-acf3-80eef6811de8" />

  
##  Tech Stack  
- SQL Database – Data Source  
- Azure Data Factory (ADF) – Pipeline orchestration  
- Azure Data Lake Storage (ADLS) – Data storage layers (Bronze, Silver, Gold)  
- Azure Databricks (PySpark) – Data transformation and SCD Type 1 logic  
- Star Schema – Data model  

##  Key Features  

- **Incremental Pipeline**: Efficiently processes new data  
- **SCD Type 1**: Performs UPSERT 
- **Star Schema**: Simplifies reporting and analytics  
