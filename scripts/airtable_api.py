from pyairtable import Api
from dotenv import load_dotenv
import pandas as pd
import os
from logger import setup_logger

class AirtableAPI:
    """
    AirtableAPI class provides methods to interact with the Airtable API, including
    retrieving records and creating webhooks for different tables.
    """
    def __init__(self):
        """
        Initialize AirtableAPI with credentials from environment variables and set up logger.
        """
        load_dotenv()
        self.airtable_token = os.getenv("AIRTABLE_TOKEN")
        self.api = Api(self.airtable_token)
        self.logger = setup_logger("airtable_api")

    def get_sales_records(self):
        """
        Retrieve all sales records from the Airtable Sales table.
        Returns:
            pd.DataFrame: DataFrame containing all sales records.
        """
        try:
            table = self.api.table(os.getenv("AIRTABLE_SALES_BASE_ID"), os.getenv("AIRTABLE_SALES_TABLE_ID"))
            table_records = table.all()
            self.logger.info("Airtable records retrieved successfully.")

            if not table_records:
                self.logger.warning("No records found in the Airtable table.")
                return pd.DataFrame()
        
            self.logger.info(f"Found {len(table_records)} records in the Sales table.")

            fields_records = []

            for record in table_records:
                fields = record['fields']
                record_id = record['id']
                fields['Airtable ID'] = record_id
                fields_records.append(fields)

            airtable_df = pd.DataFrame(fields_records)
            return airtable_df
        except Exception as e:
            self.logger.error(f"Error retrieving Sales records: {e}")

    def create_employee_webhook(self):
        """
        Create a webhook for the employee table in Airtable.
        """
        try:
            webhook_url = f"{os.getenv('WEBHOOK_BASE_URL')}/employee/webhook"
            spec = {
                "options": {
                    "filters": {
                        "dataTypes": ["tableData"],
                        "recordChangeScope": os.getenv("AIRTABLE_EMPLOYEE_TABLE_ID")
                    }
                }
            }
            base = self.api.base(os.getenv("AIRTABLE_EMPLOYEE_BASE_ID"))
            webhook = base.add_webhook(webhook_url, spec=spec)
            self.logger.info(f"Employee webhook created successfully: {webhook}")
        except Exception as e:
            self.logger.error(f"Error creating Employee webhook: {e}")

    def create_sales_webhook(self):
        """
        Create a webhook for the sales table in Airtable.
        """
        try:
            webhook_url = f"{os.getenv('WEBHOOK_BASE_URL')}/sales/webhook"
            spec = {
                "options": {
                    "filters": {
                        "dataTypes": ["tableData"],
                        "recordChangeScope": os.getenv("AIRTABLE_SALES_TABLE_ID")
                    }
                }
            }
            base = self.api.base(os.getenv("AIRTABLE_SALES_BASE_ID"))
            webhook = base.add_webhook(webhook_url, spec=spec)
            self.logger.info(f"Sales webhook created successfully: {webhook}")
        except Exception as e:
            self.logger.error(f"Error creating Sales webhook: {e}")

    def create_test_webhook(self):
        """
        Create a webhook for the test table in Airtable.
        """
        try:
            webhook_url = f"{os.getenv('WEBHOOK_BASE_URL')}/test/webhook"
            spec = {
                "options": {
                    "filters": {
                        "dataTypes": ["tableData"],
                        "recordChangeScope": os.getenv("AIRTABLE_TEST_TABLE_ID")
                    }
                }
            }
            base = self.api.base(os.getenv("AIRTABLE_TEST_BASE_ID"))
            webhook = base.add_webhook(webhook_url, spec=spec)
            self.logger.info(f"Test webhook created successfully: {webhook}")
        except Exception as e:
            self.logger.error(f"Error creating Test webhook: {e}")

