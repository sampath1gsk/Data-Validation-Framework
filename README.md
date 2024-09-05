
# Data Validation Framework

The **Data Validation Framework** is a Python-based tool designed for validating data between source and target datasets using Pandas DataFrames. It is versatile and supports the following:

- **Sources and Targets:** SQL Server, Snowflake, and file-based (e.g., CSV, Excel).
- **Data Storage:** Results can be stored in SQL Server, Snowflake, or file-based formats.

This framework is particularly useful for maintaining data integrity during migration processes, ensuring that data is accurately transferred and consistent across different systems.

## Features

- **Data Validation:** Compare data between source and target tables to ensure accuracy and consistency.
- **Datatype Validation:** Validate that data types in source and target datasets match according to predefined mappings.
- **Count Validation:** Check that record counts in source and target datasets match.
- **Duplicate Validation:** Identify and handle duplicate records in source and target datasets.
- **Flexible Input:** Supports both database tables and file-based sources for validation.

## Installation

To set up and use the Data Validation Framework, follow these steps:

1. **Clone the Repository**

   ```bash
   git clone https://github.com/sampath1gsk/Data-Validation-Framework.git
   cd Data-Validation-Framework
   ```

2. **Install Dependencies**

   In the same directory, run the `install_dependencies.bat` file to install all required Python packages:

   ```bash
   .\install_dependencies.bat
   ```

   This batch file will install all necessary dependencies.

3. **Prepare Input Files**

   Ensure that `Data_validation_input.xlsx` is placed in the same directory as the script.

## Usage

1. **Configure the Input File**

   - **`Data_validation_input.xlsx:`** This file should contain details about the source and target databases or files, including table names, column names, and data type mappings. Ensure this file is correctly set up before running the script.

2. **Run the Framework**

   Execute the following command to start the validation process:

   ```bash
   python data_validation_framework.py
   ```

   This will:
   
   - **Connect to the Databases:** Establish connections to the source, target, and output databases using credentials provided through the `inputserver_details.py` UI.
   - **Load and Preprocess Input Data:** Read configuration and datatype mappings from `Data_validation_input.xlsx`.
   - **Perform Validations:** Execute various validation checks, including count, datatype, duplicates, and data consistency.
   - **Write Results:** Write the validation results to the specified output destination.

## Script Details

The project includes several key scripts:

- **`validate_count.py:`** Contains functions for validating record counts between source and target datasets.
- **`validate_data.py:`** Includes functions for overall data validation and comparison.
- **`validate_duplicates.py:`** Provides functionality for identifying and managing duplicate records.
- **`validate_datatypes.py:`** Contains functions for validating that data types match according to the datatype mappings.
- **`inputserver_details.py:`** Provides a UI for entering source, target, and output database credentials.
- **`help_functions.py:`** Stores various utility functions required by the main validation scripts.

