# Data-Engineering-Project-Azure
# Project Overview 
This project demonstrates a data engineering pipeline built on Azure. It uses Azure Data Factory to build pipelines and Databricks (PySpark) for transformations. The pipeline is designed to handle incremental data loads, implement Slowly Changing Dimensions (SCD) Type 1, and organize data in a Star Schema model for analytics.

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

  
##  Tech Stack  

- SQL Database – Data Source  
- Azure Data Factory (ADF) – Pipeline orchestration  
- Azure Data Lake Storage (ADLS) – Data storage layers (Raw, Processed, Gold)  
- Azure Databricks (PySpark) – Data transformation and SCD Type 1 logic  
- Star Schema – Data model  

##  Key Features  

- **Incremental Pipeline**: Efficiently processes new data  
- **SCD Type 1**: Performs UPSERT 
- **Star Schema**: Simplifies reporting and analytics  
