from database import Database
from pipefy_api import PipefyAPI
from logger import setup_logger
import base64
import hmac
import hashlib
import json
import re
from dotenv import load_dotenv
import os

class Middleman:
    """
    Middleman class acts as a bridge between the database, Pipefy, and Airtable APIs.
    It handles webhook verification, payload processing, and data synchronization between systems.
    """
    def __init__(self):
        """
        Initialize the Middleman with database, Pipefy, and Airtable API clients, and logger.
        Set table IDs for employees and projects.
        """

        load_dotenv()

        self.database = Database()
        self.pipefy = PipefyAPI()
        self.logger = setup_logger("middleman")
        self.employee_table_id = os.getenv("EMPLOYEE_TABLE_ID")
        self.projects_table_id = os.getenv("PROJECTS_TABLE_ID")

    def verify_airtable_webhook(self, body: str, header: str, secret: str) -> dict:
        """
        Recreates Airtable webhook signature validation.
        Validates the webhook request by comparing the computed HMAC signature with the header.

        Args:
            body (str): The raw JSON string received in the webhook POST body.
            header (str): The value from X-Airtable-Content-MAC header.
            secret (str): The base64-encoded MAC secret from Airtable.

        Returns:
            dict: The parsed webhook payload.

        Raises:
            ValueError: If the signature does not match.
        """
        # Step 1: Decode the base64 secret
        decoded_secret = base64.b64decode(secret)

        # Step 2: Convert the body to bytes
        if isinstance(body, bytes):
            body_bytes = body
        else:
            body_bytes = body.encode("utf-8")

        # Step 3: Compute the HMAC-SHA256 signature
        hmac_obj = hmac.new(decoded_secret, body_bytes, hashlib.sha256)
        expected_signature = "hmac-sha256=" + hmac_obj.hexdigest()

        # Step 4: Compare against the header
        if not hmac.compare_digest(expected_signature, header):
            raise ValueError("Signature mismatch")

        # Step 5: Return parsed JSON
        return json.loads(body)

    def get_webhook_cursor(self, webhook_id):
        """
        Retrieve the current cursor value for a given webhook from the database.

        Args:
            webhook_id (str): The webhook identifier.

        Returns:
            int: The current cursor value or 0 if not found.
        """
        cursor_value = self.database.get_webhook_cursor(webhook_id)
        return cursor_value

    def process_webhook(self, webhook_id, payload, timestamp, cursor_value):
        """
        Store the webhook payload in the database with its cursor and timestamp.

        Args:
            webhook_id (str): The webhook identifier.
            payload (dict): The webhook payload data.
            timestamp (str): The timestamp of the payload.
            cursor_value (int): The cursor value for the payload.
        """
        self.database.insert_webhook_payload(webhook_id, payload, timestamp, cursor_value)

    def _list_of_dicts_to_graphql_input(self, lst):
        """
        Helper to convert a list of dicts to a GraphQL input string (unquoted keys, quoted string values).
        """
        def dict_to_graphql_input(d):
            items = []
            for k, v in d.items():
                if isinstance(v, str):
                    items.append(f'{k}: "{v}"')
                else:
                    items.append(f'{k}: {v}')
            return "{" + ", ".join(items) + "}"
        return "[" + ", ".join(dict_to_graphql_input(d) for d in lst) + "]"

    def process_sales_payload(self, payload):
        """
        Process sales/project-related payloads from Airtable and synchronize with Pipefy.
        Handles creation, update, and deletion of project records.

        Args:
            payload: The payload object containing sales/project data and changes.
        """
        if payload.error:
            self.logger.error(f"Error in sales payload: {payload.code} - {payload.error}")
            return
        
        # Mapping of Airtable field IDs to logical field names
        field_names = {
            "fldjea0RkOhDvFEwu": "project_code",
            "fldILk1gQnsXBXr6N": "project_status",
            "fldFKQ6gz23yXPemZ": "owner"
        }

        # Handle created records
        for table_id, table_data in payload.changed_tables_by_id.items():
            for record_id, record in table_data.created_records_by_id.items():
                fields = record.cell_values_by_field_id
                title = fields.get("fldjea0RkOhDvFEwu", record_id)
                project_status = fields.get("fldILk1gQnsXBXr6N")['name'] if isinstance(fields.get("fldILk1gQnsXBXr6N"), dict) else ""

                inserted_fields = [
                    {"field_id": "project_code", "field_value": title},
                    {"field_id": "project_status", "field_value": re.sub(r'[^a-zA-Z]', '', project_status)},
                    {"field_id": "project_owner", "field_value": fields.get("fldFKQ6gz23yXPemZ")['name'] if isinstance(fields.get("fldFKQ6gz23yXPemZ"), dict) else ""},
                    {"field_id": "project_owner_email", "field_value": fields.get("fldFKQ6gz23yXPemZ")['email'] if isinstance(fields.get("fldFKQ6gz23yXPemZ"), dict) else ""}
                ]

                # Use the helper to convert inserted_fields to GraphQL input
                fields_attributes_str = self._list_of_dicts_to_graphql_input(inserted_fields)
                pipefy_id = self.pipefy.insert_pipefy_record(self.projects_table_id, title, fields_attributes_str)

                airtable_db_insert = {
                    "opportunity_code": title,
                    "opportunity_stage": re.sub(r'[^a-zA-Z]', '', project_status),
                    "owner_name": fields.get("fldFKQ6gz23yXPemZ")['name'] if isinstance(fields.get("fldFKQ6gz23yXPemZ"), dict) else "",
                    "owner_email": fields.get("fldFKQ6gz23yXPemZ")['email'] if isinstance(fields.get("fldFKQ6gz23yXPemZ"), dict) else "",
                    "airtable_id": record_id,
                }

                self.database.insert_airtable_sale(airtable_db_insert)

                pipefy_db_insert = {
                    "title": title,
                    "project_code": title,
                    "project_status": re.sub(r'[^a-zA-Z]', '', project_status),
                    "project_owner": fields.get("fldFKQ6gz23yXPemZ")['name'] if isinstance(fields.get("fldFKQ6gz23yXPemZ"), dict) else "",
                    "project_owner_email": fields.get("fldFKQ6gz23yXPemZ")['email'] if isinstance(fields.get("fldFKQ6gz23yXPemZ"), dict) else "",
                    "pipefy_id": pipefy_id,
                    "airtable_id": record_id,
                }

                self.database.insert_pipefy_project(pipefy_db_insert)

        # Handle changed records
        for table_id, table_data in payload.changed_tables_by_id.items():
            for record_id, change in table_data.changed_records_by_id.items():
                pipefy_id = self.database.get_project_id(record_id)

                new_changes = change.current.cell_values_by_field_id

                db_update = {}

                airtable_names = {
                    "fldjea0RkOhDvFEwu": "opportunity_code",
                    "fldILk1gQnsXBXr6N": "opportunity_stage",
                    "fldFKQ6gz23yXPemZ": "owner"
                }
                

                for field_id, value in new_changes.items():
                    if field_id in field_names:
                        field_name = field_names[field_id]
                        airtable_name = airtable_names[field_id]

                        # Special handling for project_code field (update title in Pipefy)
                        if field_name == "project_code":
                            self.pipefy.update_record_title(pipefy_id, value)
                            self.pipefy.update_pipefy_record(pipefy_id, field_name, value)
                            db_update[airtable_name] = value
                            continue

                        # Special handling for project_status field (extract 'name' if dict)
                        if field_name == "project_status":
                            value = re.sub(r'[^a-zA-Z]', '', value['name']) if isinstance(value, dict) else ""
                            self.pipefy.update_pipefy_record(pipefy_id, field_name, value)
                            db_update[airtable_name] = value
                            continue
                        
                        # Special handling for owner field (update both name and email)
                        if field_name == "owner":
                            
                            owner_name = value['name'] if isinstance(value, dict) else ""
                            owner_email = value['email'] if isinstance(value, dict) else ""
                            self.pipefy.update_pipefy_record(pipefy_id, "project_owner", owner_name)
                            self.pipefy.update_pipefy_record(pipefy_id, "project_owner_email", owner_email)
                            db_update["owner_name"] = owner_name
                            db_update["owner_email"] = owner_email
                            continue

                        if value is not None:
                            self.pipefy.update_pipefy_record(pipefy_id, field_name, value)
                            db_update[airtable_name] = value

                self.database.update_airtable_sale(record_id, db_update)
            # Handle destroyed (deleted) records
            for record_id in table_data.destroyed_record_ids:
                pipefy_id = self.database.get_project_id(record_id)

                self.pipefy.delete_pipefy_record(pipefy_id)
                self.database.delete_airtable_project(record_id)

    def process_employee_payload(self, payload):
        """
        Process employee-related payloads from Airtable and synchronize with Pipefy.
        Handles creation, update, and deletion of employee records.

        Args:
            payload: The payload object containing employee data and changes.
        """
        if payload.error:
            self.logger.error(f"Error in employee payload: {payload.code} - {payload.error}")
            return
        
        # Mapping of Airtable field IDs to logical field names
        field_names = {
            'fldGzFp2d6CgUdbDe': 'boxfusioner',
            'fldRfXvBW2928LYTe': 'supervisor',
            'fldKbbTM3Fvt78E0N': 'email',
            'fldoWHmConTYp8lQ7': 'status'
        }

        # Handle created records
        for table_id, table_data in payload.changed_tables_by_id.items():
            for record_id, record in table_data.created_records_by_id.items():
                fields = record.cell_values_by_field_id
                title = fields.get("fldoDJ7AeUcw1iqsz", record_id)

                inserted_fields = [
                    {"field_id": "boxfusioner", "field_value": title},
                    {"field_id": "email", "field_value": fields.get("fldKbbTM3Fvt78E0N", "")},
                    {"field_id": "status", "field_value": fields.get("fldoWHmConTYp8lQ7")['name'] if isinstance(fields.get("fldoWHmConTYp8lQ7"), dict) else ""},
                    {"field_id": "supervisor", "field_value": fields.get("fldRfXvBW2928LYTe", "")}
                ]

                # Use the helper to convert inserted_fields to GraphQL input
                fields_attributes_str = self._list_of_dicts_to_graphql_input(inserted_fields)
                pipefy_id = self.pipefy.insert_pipefy_record(self.employee_table_id, title, fields_attributes_str)

                airtable_db_insert = {
                    "id": record_id,
                    "employee": title,
                    "email": fields.get("fldKbbTM3Fvt78E0N", ""),
                    "status": fields.get("fldoWHmConTYp8lQ7")['name'] if isinstance(fields.get("fldoWHmConTYp8lQ7"), dict) else "",
                    "supervisor": fields.get("fldRfXvBW2928LYTe", "")
                }

                self.database.insert_airtable_employee(airtable_db_insert)

                pipefy_db_insert = {
                    "boxfusioner": title,
                    "title": title,
                    "pipefy_id": pipefy_id,
                    "airtable_id": record_id,
                    "email": fields.get("fldKbbTM3Fvt78E0N", ""),
                    "status": fields.get("fldoWHmConTYp8lQ7")['name'] if isinstance(fields.get("fldoWHmConTYp8lQ7"), dict) else "",
                    "supervisor": fields.get("fldRfXvBW2928LYTe", "")
                }

                self.database.insert_pipefy_employee(pipefy_db_insert)

        # Handle changed records
        for table_id, table_data in payload.changed_tables_by_id.items():
            # Iterate over changed records
            for record_id, change in table_data.changed_records_by_id.items():
                pipefy_id = self.database.get_employee_id(record_id)

                new_changes = change.current.cell_values_by_field_id

                db_update = {}

                airtable_names = {
                    "fldGzFp2d6CgUdbDe": "employee",
                    "fldKbbTM3Fvt78E0N": "email",
                    "fldoWHmConTYp8lQ7": "status",
                    "fldRfXvBW2928LYTe": "supervisor"
                }
                
                for field_id, value in new_changes.items():
                    if field_id in field_names:
                        field_name = field_names[field_id]
                        airtable_name = airtable_names[field_id]

                        if field_name == "boxfusioner":
                            # Special handling for boxfusioner field (use title as value)
                            value = value if isinstance(value, str) else ""
                            self.pipefy.update_record_title(pipefy_id, value)
                            self.pipefy.update_pipefy_record(pipefy_id, field_name, value)
                            db_update[airtable_name] = value
                            continue

                        # Special handling for status field (extract 'name' if dict)
                        if field_name == "status":
                            value = value['name'] if isinstance(value, dict) else ""
                            self.pipefy.update_pipefy_record(pipefy_id, field_name, value)
                            db_update[airtable_name] = value
                            continue

                        if value is not None:
                            self.pipefy.update_pipefy_record(pipefy_id, field_name, value)
                            db_update[airtable_name] = value

                self.database.update_airtable_employee(record_id, db_update)
            # Handle destroyed (deleted) records
            for record_id in table_data.destroyed_record_ids:
                pipefy_id = self.database.get_employee_id(record_id)

                self.pipefy.delete_pipefy_record(pipefy_id)
                self.database.delete_airtable_employee(record_id)
