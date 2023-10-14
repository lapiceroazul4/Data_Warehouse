# Date_Warehouse

## Description

This project aims to build a Data Warehouse from a Kaggle dataset and an API, facilitating data extraction, transformation, storage, and analysis for informed decision-making

## Prerequisites

Before getting started with this project, make sure you have the following components installed or ready:

- [Python](https://www.python.org/)
- [Database (can be local or cloud-based, if it's local I recommend using MySQL)](https://www.mysql.com/)

## Environment Setup

Here are the steps to set up your development environment:

1. **create a virtual enviroment**: Run the following command to create a virtual enviroment called venv:

   ```bash
   python -m venv venv

2. **activate your venv**: Run the following commands to activate the enviroment:

   ```bash
   cd venv/bin
   source activate

3. **Install Dependencies**: Once you're in the venv run the following command to install the necessary dependencies:

   ```bash
   pip install -r requirements.txt

4. **Create db_config**: Yo need to create a json file called "db_config" with the following information, make sure you replace the values with the correspondent information :

   ```bash
   {
    "user" : "myuser",
    "passwd" : "mypass",
    "server" : "XXX.XX.XX.XX",
    "database" : "demo_db"
   }  

5. **Run the Airbnb Notebook**: Now go to Airbnb folder and run main_def.ipynb

6. **Create api_config**: Yo need to create a json file called "api_config" with the following information, make sure you replace the values with the correspondent information: Remember this information it's provided in:  https://rapidapi.com/traveltables/api/cost-of-living-and-prices/

   ```bash
      {
        "X-RapidAPI-Key": "MyRapidAPIKey",
        "X-RapidAPI-Host": "MyRapidAPIHost"
      }

7. **Run the API Notebook**: Now go to API folder and run eda.ipynb


## Contact

If you have any questions or suggestions, feel free to contact me at lapiceroazul@proton.me
