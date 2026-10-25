import pandas as pd
from logger import setup_logger
from airtable_api import AirtableAPI
from pipefy_api import PipefyAPI
import os
from dotenv import load_dotenv

class FileCleaner:
    """
    FileCleaner class provides methods to clean and normalize data from Airtable and Pipefy
    for use in the data sync pipeline. It handles field cleaning, sales data cleaning,
    and project data cleaning/merging.
    """
    def __init__(self):
        """
        Initialize FileCleaner and set up logger.
        """
        load_dotenv()

        self.projects_table = os.getenv("PROJECTS_TABLE_ID")
        self.logger = setup_logger("file_cleaner")

    def field_cleaner(self, df, field):
        """
        Clean a specific field in a DataFrame by removing unwanted characters.
        Args:
            df (pd.DataFrame): The DataFrame containing the field.
            field (str): The field/column name to clean.
        Returns:
            list: Cleaned field values as a list.
        """
        clean_field = df[field].astype(str).str.replace(r'[^a-zA-Z0-9\s\,/@.]', '', regex=True)
        clean_field = [x if pd.notnull(x) else '' for x in clean_field]

        return clean_field

    def clean_sales_data(self):
        """
        Clean and normalize sales data from Airtable.
        Returns:
            pd.DataFrame: Cleaned sales data.
        Raises:
            ValueError: If no sales data is found.
        """
        sales_df = AirtableAPI().get_sales_records()

        if sales_df.empty:
            self.logger.warning("No sales_df data found.")
            raise ValueError("No sales_df data found. Please check the Airtable API connection or the table name.")
        
        # Select relevant columns
        sales_df = sales_df[['Opportunity Code', 'Opportunity Type', 'Opportunity Stage', 'Owner',
        'Sales Channel', 'Solution Type', 'Solution (Module)', 'Client Type', 'Win Date', 'Created',
        'Created By', 'Financial Year', 'Airtable ID' ]]

        # Normalize nested 'Created By' and 'Owner' fields
        Created_by_df = pd.json_normalize(sales_df['Created By'])[['name', 'email']]
        Created_by_df = Created_by_df.rename(columns={'name': 'Created By Name', 'email': 'Created By Email'})

        sales_df = pd.concat([sales_df.drop(columns=['Created By']), Created_by_df], axis=1)

        owner_df = pd.json_normalize(sales_df['Owner'])[['name', 'email']]
        owner_df = owner_df.rename(columns={'name': 'Owner Name', 'email': 'Owner Email'})

        sales_df = pd.concat([sales_df.drop(columns=['Owner']), owner_df], axis=1)

        # Clean specific fields
        sales_df['Solution Type'] = self.field_cleaner(sales_df, 'Solution Type')
        sales_df['Sales Channel'] = self.field_cleaner(sales_df, 'Sales Channel')
        sales_df['Solution (Module)'] = self.field_cleaner(sales_df, 'Solution (Module)')
        sales_df['Client Type'] = self.field_cleaner(sales_df, 'Client Type')

        # Ensure Win Date is None if not present
        sales_df['Win Date'] = [x if pd.notnull(x) else None for x in sales_df['Win Date']]

        self.logger.info("Sales data cleaned successfully.")

        return sales_df
    
    def clean_projects_data(self):
        """
        Clean and merge project data from Pipefy with sales data from Airtable.
        Returns:
            pd.DataFrame: Cleaned and merged project data.
        Raises:
            ValueError: If no project data is found.
        """
        projects_df = PipefyAPI().get_pipefy_records(self.projects_table)
        sales_df = self.clean_sales_data()

        if projects_df.empty:
            self.logger.warning("No projects_df data found.")
            raise ValueError("No projects_df data found. Please check the Airtable API connection or the table name.")
        
        # Clean and normalize opportunity stage
        sales_df['Oppurtunity Stage'] = sales_df['Opportunity Stage'].str.replace(r'[^a-zA-Z]', '', regex=True) 
        sales_df['Oppurtunity Stage'] = sales_df['Oppurtunity Stage'].str.replace('nan', '')

        # Merge sales data into projects data based on Project Code
        for index, row in projects_df.iterrows():
            if row['Project Code'] in sales_df['Opportunity Code'].values:
                project_stage = sales_df[sales_df['Opportunity Code'] == row['Project Code']]['Oppurtunity Stage'].values[0]

                projects_df.at[index, 'Project Status'] = project_stage
                project_owner = sales_df[sales_df['Opportunity Code'] == row['Project Code']]['Owner Name'].values[0]

                projects_df.at[index, 'Project Owner'] = project_owner
                project_owner_email = sales_df[sales_df['Opportunity Code'] == row['Project Code']]['Owner Email'].values[0]

                projects_df.at[index, 'Project Owner Email'] = project_owner_email
                projects_df.at[index, 'Airtable ID'] = sales_df[sales_df['Opportunity Code'] == row['Project Code']]['Airtable ID'].values[0]

        self.logger.info("Projects data cleaned successfully.")

        return projects_df
