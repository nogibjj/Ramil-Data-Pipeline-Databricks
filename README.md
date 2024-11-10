[![CI](https://github.com/nogibjj/Ramil_PySpark_Data_Processing/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Ramil_PySpark_Data_Processing/actions/workflows/cicd.yml)

# Ramil's PySpark Data Processing Project

## Project Overview
This project is focused on data processing using PySpark, a powerful and flexible tool for handling large-scale data processing. The primary goal of this project is to leverage PySpark's distributed processing capabilities to load, transform, and analyze big datasets efficiently. PySpark process the Clubs.csv file located in data folder.

## Features
- Data ingestion and cleaning using PySpark DataFrames
- Exploratory data analysis (EDA) to understand key data insights
- Data transformation and aggregation with PySpark SQL
- Scalable codebase that can handle large datasets with minimal adjustments

## Prerequisites
- Python 3.8 or above
- Apache Spark with PySpark library (If you use Github Workspace it already done)
- Recommended: Jupyter Notebook or IDE with PySpark support

### Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/nogibjj/Ramil_PySpark_Data_Processing.git
    cd Ramil_PySpark_Data_Processing
    ```
2. Set up a virtual environment (Automatically done in Github Workspace):
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3. Install the required packages:
    ```bash
    make install
    ```


## Usage
1. Activate the virtual environment (if not already active).
2. Run the data processing script:
    ```bash
    python main.py
    ```
3. Explore the results in the output directory.

4. For testing the code:
    ```bash
        make test
    ```
5. For linting:
    ```bash
        make lint
    ```
6. For formating:
    ```bash
        make format
    ```


## Project Structure
```
├── data                # Directory for datasets
├── scripts             # Python scripts for various tasks
├── README.md           # Project documentation
└── requirements.txt    # Python package dependencies
```

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## PySpark example images



