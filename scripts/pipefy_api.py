import requests
import pandas as pd
from dotenv import load_dotenv
import os
from logger import setup_logger
import json

class PipefyAPI:
    """
    PipefyAPI class provides methods to interact with the Pipefy API, including authentication,
    record retrieval, insertion, update, and deletion for Pipefy tables.
    """
    def __init__(self):
        """
        Initialize PipefyAPI with credentials from environment variables and set up logger.
        Retrieves an access token for API requests.
        """
        load_dotenv()
        self.token_url = "https://app.pipefy.com/oauth/token"
        self.request_url = "https://api.pipefy.com/graphql"
        self.body = {
            "grant_type": "client_credentials",
            "client_id": os.getenv("PIPEFY_CLIENT_ID"),
            "client_secret": os.getenv("PIPEFY_CLIENT_SECRET"),
        }
        self.logger = setup_logger("pipefy_api")

        try:
           self.access_token = self.get_access_token()
        except Exception as e:
           self.logger.error(f"Error initializing PipefyAPI: {e}")

    def get_access_token(self):
        """
        Get access token from Pipefy API using client credentials.
        Returns:
            str: Access token string.
        Raises:
            Exception: If the request fails.
        """
        try:
          response = requests.post(self.token_url, data=self.body)
          response.raise_for_status()
          token = response.json().get("access_token")
          return token
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get access token: {response.status_code} - {e}")
    
    def make_request(self, headers=None, data=None):
        """
        Make a request to the Pipefy API GraphQL endpoint.
        Args:
            headers (dict): Optional headers to include in the request.
            data (dict): The GraphQL query/mutation and variables.
        Returns:
            dict: The JSON response from the API.
        Raises:
            Exception: If the request fails.
        """
        headers = headers or {}
        headers["Authorization"] = f"Bearer {self.access_token}"

        try:
            response = requests.post(self.request_url, headers=headers, json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Request failed: {response.status_code} - {e}")

    def get_pipefy_records(self, table_id):
        """
        Retrieve all records from a Pipefy table by table_id.
        Args:
            table_id (str or int): The Pipefy table ID.
        Returns:
            pd.DataFrame: DataFrame containing all records from the table.
        """
        records_objects = f""" query ($after: String) {{
      table(id: "{table_id}") {{
        table_records(first: 50, after: $after) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          edges {{
            node {{
              record_fields {{
                name
                value
              }}
              id
              created_at
              title
            }}
          }}
        }}
      }}
    }}"""

        try:
            self.logger.info("Access token retrieved successfully.")
            field_records = []
            after_cursor = None
            has_next_page = True

            while has_next_page:
                variables = {"after": after_cursor}
                api_call = self.make_request(data={
                    "query": records_objects, "variables": variables
                })

                records = api_call["data"]["table"]["table_records"]["edges"]

                if not records:
                    self.logger.warning("No records found in the Pipefy table.")
                    return pd.DataFrame()

                for record in records:
                    # Add extra fields for easier DataFrame construction
                    record['node']['record_fields'].append({'name': 'Created At', 'value': record['node']['created_at']})
                    record['node']['record_fields'].append({'name': 'Pipefy ID', 'value': record['node']['id']})
                    record['node']['record_fields'].append({'name': 'Title', 'value': record['node']['title']})

                    fields = record["node"]["record_fields"]
                    field_records.append({field["name"]: field["value"] for field in fields})

                page_info = api_call["data"]["table"]["table_records"]["pageInfo"]
                has_next_page = page_info["hasNextPage"]
                after_cursor = page_info["endCursor"]

            self.logger.info(f"Found {len(field_records)} records in the Pipefy table.")
            pipefy_df = pd.DataFrame(field_records)
            return pipefy_df
        except Exception as e:
            self.logger.error(e)

    def insert_pipefy_record(self, table_id, title, record_data):
        """
        Insert a new record into the Pipefy table.
        Args:
            table_id (str or int): The Pipefy table ID.
            title (str): The title for the new record.
            record_data (list): List of field dicts for the new record.
        Returns:
            str: The ID of the inserted record.
        Raises:
            Exception: If the insertion fails.
        """
        mutation = f"""
        mutation {{
          createTableRecord(input: {{
            table_id: {table_id},
            title: "{title}",
            fields_attributes: {record_data}
          }}) {{
            table_record {{
              id
            }}
          }}
        }}"""

        try:
            response = self.make_request(data={"query": mutation})

            if "errors" in response:
                raise Exception(str(response['errors']))

            self.logger.info("Record inserted successfully.")
            return response["data"]["createTableRecord"]["table_record"]['id']
        except Exception as e:
            self.logger.error(f"Failed to insert record: {e}")
    
    def update_pipefy_record(self, record_id, field_id, value):
        """
        Update an existing record in the Pipefy table.
        Args:
            record_id (str or int): The Pipefy record ID.
            field_id (str): The field ID to update.
            value (str): The new value for the field.
        Returns:
            dict or None: The updated record info, or None if update fails.
        """
        mutation = f"""
        mutation {{
          setTableRecordFieldValue (input: {{
            table_record_id: {record_id},
            field_id: "{field_id}",
            value: "{value}"
          }}) {{
            table_record {{
              id
              title
            }}
          }}
        }}"""

        try:
            response = self.make_request(data={"query": mutation})
            self.logger.info("Record updated successfully.")
            return response["data"]["setTableRecordFieldValue"]["table_record"]
        except Exception as e:
            self.logger.error(f"Failed to update record: {e}")
            return None
        
    def update_record_title(self, record_id, new_title):
        """
        Update the title of an existing Pipefy record.
        Args:
            record_id (str or int): The Pipefy record ID.
            new_title (str): The new title for the record.
        Returns:
            dict or None: The updated record info, or None if update fails.
        """
        mutation = f"""
        mutation {{
          updateTableRecord(input: {{
            id: {record_id},
            title: "{new_title}"
          }}) {{
            table_record {{
              id
              title
            }}
          }}
        }}"""

        try:
            response = self.make_request(data={"query": mutation})
            self.logger.info("Record title updated successfully.")
            return response["data"]["updateTableRecord"]["table_record"]
        except Exception as e:
            self.logger.error(f"Failed to update record title: {e}")
            return None

    def delete_pipefy_record(self, record_id):
        """
        Delete a record from the Pipefy table.
        Args:
            record_id (str or int): The Pipefy record ID to delete.
        """
        try:
            mutation = f"""
          mutation {{
            deleteTableRecord(input: {{
              id: {record_id}
            }}) {{
              success
            }}
          }}"""

            response = self.make_request(data={"query": mutation})

            if "errors" in response:
              raise Exception(str(response['errors']))

            if not response["data"]["deleteTableRecord"]["success"]:
                self.logger.error(f"Failed to delete record. Record ID: {record_id}")

            self.logger.info(f"Record deleted successfully. Record ID: {record_id}")
        except Exception as e:
            self.logger.error(f"Failed to delete record: {e}")

