import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
from logger import setup_logger
from pipefy_api import PipefyAPI
from filecleaner import FileCleaner


class Database:
    """
    Database class manages all database operations, including table creation, triggers,
    record insertion, updates, deletions, and retrievals for the data sync pipeline.
    """
    def __init__(self):
        """
        Initialize Database connection using environment variables and set up logger.
        """
        load_dotenv()
        self.DB_NAME = os.getenv("DB_NAME")
        self.DB_USER = os.getenv("DB_USER")
        self.DB_PASS = os.getenv("DB_PASS")
        self.DB_HOST = os.getenv("DB_HOST")
        self.projects_table = os.getenv("PROJECTS_TABLE_ID")
        self.logger = setup_logger("database")
        try:
            self.conn = psycopg2.connect(
                database=self.DB_NAME,
                user=self.DB_USER,
                password=self.DB_PASS,
                host=self.DB_HOST
            )
            self.logger.info("Connected to the database successfully.")
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    def create_tables(self):
        """
        Create all required tables in the database if they do not exist.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS projects (
                        project_id SERIAL PRIMARY KEY,
                        title TEXT NOT NULL,
                        project_code TEXT NOT NULL,
                        project_status TEXT,
                        project_owner TEXT,
                        project_owner_email TEXT,
                        created_at TIMESTAMP,
                        pipefy_id INT
                    );
                               
                    CREATE TABLE IF NOT EXISTS sales (
                        opportunity_id SERIAL PRIMARY KEY,
                        opportunity_code TEXT NOT NULL,
                        opportunity_type TEXT,
                        opportunity_stage TEXT,
                        owner_name TEXT,
                        owner_email TEXT,
                        sales_channel TEXT,
                        solution_type TEXT,
                        solution_module TEXT,
                        client_type TEXT,
                        win_date DATE,
                        created_at TIMESTAMP,
                        created_by_name TEXT,
                        created_by_email TEXT,
                        financial_year TEXT,
                        airtable_id TEXT
                    );
                               
                    CREATE TABLE IF NOT EXISTS cleaned_projects (
                        project_id SERIAL PRIMARY KEY,
                        title TEXT NOT NULL,
                        project_code TEXT NOT NULL,
                        project_status TEXT,
                        project_owner TEXT,
                        project_owner_email TEXT,
                        created_at TIMESTAMP,
                        pipefy_id INT,
                        airtable_id TEXT
                    );
                               
                    CREATE TABLE IF NOT EXISTS webhooks (
                        id SERIAL PRIMARY KEY,
                        webhook_id TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TEXT,
                        current_cursor INT DEFAULT 0
                    );
                """)
            self.conn.commit()
            self.logger.info("Tables created successfully.")

        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            self.conn.rollback()

    def create_employee_triggers(self):
        """
        Create triggers and functions for employee-related table changes.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE OR REPLACE FUNCTION delete_related_employee()
                    RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS
                    $$
                    BEGIN
                        DELETE FROM pipefy WHERE id = OLD.pipefy_id;
                        DELETE FROM airtable WHERE id = OLD.airtable_id;

                        RETURN NULL;
                    END;
                    $$;
                               
                    CREATE OR REPLACE FUNCTION insert_related_employee()
                    RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS
                    $$
                    BEGIN
                        INSERT INTO pipefy(id, boxfusioner, title, email, status, supervisor)
                        VALUES(NEW.pipefy_id, NEW.boxfusioner, NEW.title, NEW.email, NEW.status, NEW.supervisor);
                        RETURN NULL;
                    END;
                    $$;

                    CREATE OR REPLACE FUNCTION update_related_employee()
                    RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS
                    $$
                    BEGIN
                        IF (NEW.status IS DISTINCT FROM OLD.status
                            OR NEW.email IS DISTINCT FROM OLD.email
                            OR NEW.supervisor IS DISTINCT FROM OLD.supervisor
                            OR NEW.employee IS DISTINCT FROM OLD.employee) THEN

                            UPDATE cleaned_pipefy
                            SET
                                boxfusioner = CASE WHEN NEW.employee IS DISTINCT FROM OLD.employee THEN NEW.employee ELSE boxfusioner END,
                                title = CASE WHEN NEW.employee IS DISTINCT FROM OLD.employee THEN NEW.employee ELSE title END,
                                status = CASE WHEN NEW.status IS DISTINCT FROM OLD.status THEN NEW.status ELSE status END,
                                email = CASE WHEN NEW.email IS DISTINCT FROM OLD.email THEN NEW.email ELSE email END,
                                supervisor = CASE WHEN NEW.supervisor IS DISTINCT FROM OLD.supervisor THEN NEW.supervisor ELSE supervisor END
                            WHERE airtable_id = OLD.id;
                        END IF;
                        RETURN NULL;
                    END;
                    $$;

                    CREATE TRIGGER after_delete_employee_trigger
                    AFTER DELETE ON cleaned_pipefy
                    FOR EACH ROW
                    EXECUTE FUNCTION delete_related_employee();

                    CREATE TRIGGER after_update_employee_trigger
                    AFTER UPDATE ON airtable
                    FOR EACH ROW
                    EXECUTE FUNCTION update_related_employee();
                               
                    CREATE TRIGGER after_insert_employee_trigger
                    AFTER INSERT ON cleaned_pipefy
                    FOR EACH ROW
                    EXECUTE FUNCTION insert_related_employee();
                """)
            self.conn.commit()
            self.logger.info("Employee triggers created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating employee triggers: {e}")
            self.conn.rollback()

    def create_sales_triggers(self):
        """
        Create triggers and functions for sales/project-related table changes.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE OR REPLACE FUNCTION delete_related_projects()
                    RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS
                    $$
                    BEGIN
                        DELETE FROM sales WHERE opportunity_code = OLD.project_code;
                        DELETE FROM projects WHERE project_code = OLD.project_code;

                        RETURN NULL;
                    END;
                    $$;
                               
                    CREATE OR REPLACE FUNCTION insert_related_project()
                    RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS
                    $$
                    BEGIN
                        INSERT INTO projects(title, project_code, project_status, project_owner, project_owner_email, pipefy_id)
                        VALUES(NEW.title, NEW.project_code, NEW.project_status, NEW.project_owner, NEW.project_owner_email, NEW.pipefy_id);
                        RETURN NULL;
                    END;
                    $$;

                    CREATE OR REPLACE FUNCTION update_related_projects()
                    RETURNS TRIGGER
                    LANGUAGE PLPGSQL
                    AS
                    $$
                    BEGIN
                        IF (
							NEW.opportunity_code IS DISTINCT FROM OLD.opportunity_code OR
                            NEW.opportunity_stage IS DISTINCT FROM OLD.opportunity_stage OR
                            NEW.owner_name IS DISTINCT FROM OLD.owner_name OR
                            NEW.owner_email IS DISTINCT FROM OLD.owner_email
                        ) THEN
                            UPDATE cleaned_projects 
                            SET 
								project_code = CASE 
                                                    WHEN NEW.opportunity_code IS DISTINCT FROM OLD.opportunity_code 
                                                    THEN NEW.opportunity_code 
                                                    ELSE project_code 
                                                END,
                                project_status = CASE 
                                                    WHEN NEW.opportunity_stage IS DISTINCT FROM OLD.opportunity_stage 
                                                    THEN NEW.opportunity_stage 
                                                    ELSE project_status 
                                                END,
                                project_owner = CASE 
                                                    WHEN NEW.owner_name IS DISTINCT FROM OLD.owner_name 
                                                    THEN NEW.owner_name 
                                                    ELSE project_owner 
                                                END,
                                project_owner_email = CASE 
                                                        WHEN NEW.owner_email IS DISTINCT FROM OLD.owner_email 
                                                        THEN NEW.owner_email 
                                                        ELSE project_owner_email 
                                                    END
                            WHERE project_code = OLD.opportunity_code;
                        END IF;

                        RETURN NULL;
                    END;
                    $$;

                    CREATE TRIGGER after_delete_project_trigger
                    AFTER DELETE ON cleaned_projects
                    FOR EACH ROW
                    EXECUTE FUNCTION delete_related_projects();

                    CREATE TRIGGER after_update_project_trigger
                    AFTER UPDATE ON sales
                    FOR EACH ROW
                    EXECUTE FUNCTION update_related_projects();
                               
                    CREATE TRIGGER after_insert_project_trigger
                    AFTER INSERT ON cleaned_projects
                    FOR EACH ROW
                    EXECUTE FUNCTION insert_related_project();
                """)
            self.conn.commit()
            self.logger.info("Sales triggers created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating sales triggers: {e}")
            self.conn.rollback()

    def insert_projects_records(self):
        """
        Insert project records from Pipefy into the projects table.
        """
        try:
            pipefy = PipefyAPI()
            pipefy_df = pipefy.get_pipefy_records(self.projects_table)

            if pipefy_df.empty:
                self.logger.warning("Pipefy dataframe is empty.")
                return
            
            with self.conn.cursor() as cursor:
                for _, row in pipefy_df.iterrows():
                    cursor.execute("""
                        INSERT INTO projects (title, project_code, project_status, project_owner, project_owner_email, created_at, pipefy_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (row['Title'], row['Project Code'], row['Project Status'], row['Project Owner'], row['Project Owner Email'], row['Created At'], row['Pipefy ID']))
                self.logger.info("Inserted records into projects table.")
            self.conn.commit()
            self.logger.info("Projects records committed to the database.")
        except Exception as e:
            self.logger.error(f"Error inserting records into projects table: {e}")
            self.conn.rollback()

    def insert_sales_records(self):
        """
        Insert sales records from cleaned sales data into the sales table.
        """
        try:
            sales = FileCleaner().clean_sales_data()

            if sales.empty:
                raise ValueError("Sales dataframe is empty. Please check the data cleaning process.")
            
            with self.conn.cursor() as cursor:
                for _, row in sales.iterrows():
                    cursor.execute("""
                        INSERT INTO sales (opportunity_code, opportunity_type, opportunity_stage, owner_name, owner_email, sales_channel, solution_type, solution_module, client_type, win_date, created_at, created_by_name, created_by_email, financial_year, airtable_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (row['Opportunity Code'], row['Opportunity Type'], row['Opportunity Stage'], row['Owner Name'], row['Owner Email'], row['Sales Channel'], row['Solution Type'], row['Solution (Module)'], row['Client Type'], row['Win Date'], row['Created'], row['Created By Name'], row['Created By Email'], row['Financial Year'], row['Airtable ID']))
                self.logger.info("Inserted records into sales table.")
            self.conn.commit()
            self.logger.info("Sales records committed to the database.")
        except Exception as e:
            self.logger.error(f"Error inserting records into sales table: {e}")
            self.conn.rollback()

    def insert_cleaned_projects(self):
        """
        Insert cleaned project records into the cleaned_projects table.
        """
        try:
            clean_projects = FileCleaner().clean_projects_data()

            if clean_projects.empty:
                raise ValueError("Cleaned projects dataframe is empty. Please check the data cleaning process.")
            
            with self.conn.cursor() as cursor:
                for _, row in clean_projects.iterrows():
                    cursor.execute("""
                        INSERT INTO cleaned_projects (title, project_code, project_status, project_owner, project_owner_email, created_at, pipefy_id, airtable_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (row['Title'], row['Project Code'], row['Project Status'], row['Project Owner'], row['Project Owner Email'], row['Created At'], row['Pipefy ID'], row['Airtable ID']))
                self.logger.info("Inserted records into cleaned_projects table.")
            self.conn.commit()
            self.logger.info("Cleaned projects records committed to the database.")
        except Exception as e:
            self.logger.error(f"Error inserting records into cleaned_projects table: {e}")
            self.conn.rollback()
    
    def get_webhook_cursor(self, webhook_id):
        """
        Retrieve the current cursor value for a given webhook from the webhooks table.
        Args:
            webhook_id (str): The webhook identifier.
        Returns:
            int: The current cursor value or 0 if not found.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT current_cursor FROM webhooks WHERE webhook_id = %s
                ORDER BY current_cursor DESC
            """, (webhook_id,))
            result = cursor.fetchone()

            self.logger.info(f"Retrieved cursor for webhook {webhook_id}: {result[0] if result else 0}")
            return result[0] if result else 0

    def insert_webhook_payload(self, webhook_id, payload, timestamp, cursor_value):
        """
        Insert a webhook payload into the webhooks table.
        Args:
            webhook_id (str): The webhook identifier.
            payload (dict): The webhook payload data.
            timestamp (str): The timestamp of the payload.
            cursor_value (int): The cursor value for the payload.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO webhooks (webhook_id, payload, created_at, current_cursor)
                    VALUES (%s, %s, %s, %s)
                """, (webhook_id, payload, timestamp, cursor_value))
            self.conn.commit()
            self.logger.info(f"Inserted webhook payload for {webhook_id} with cursor {cursor_value}.")
        except Exception as e:
            self.logger.error(f"Error inserting webhook payload: {e}")
            self.conn.rollback()

    def get_employee_id(self, airtable_id):
        """
        Retrieve the Pipefy ID for an employee given their Airtable ID.
        Args:
            airtable_id (str): The Airtable record ID.
        Returns:
            int or None: The Pipefy ID or None if not found.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT pipefy_id FROM cleaned_pipefy WHERE airtable_id = %s
            """, (airtable_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def get_project_id(self, airtable_id):
        """
        Retrieve the Pipefy ID for a project given its Airtable ID.
        Args:
            airtable_id (str): The Airtable record ID.
        Returns:
            int or None: The Pipefy ID or None if not found.
        """
        with self.conn.cursor() as cursor:
            cursor.execute("""
                SELECT pipefy_id FROM cleaned_projects WHERE airtable_id = %s
            """, (airtable_id,))
            result = cursor.fetchone()
            return result[0] if result else None

    def insert_airtable_employee(self, employee_data):
        """
        Insert a new employee record into the Airtable table.
        Args:
            employee_data (dict): A dictionary containing employee data.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO airtable (id, employee, supervisor, email, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (employee_data['id'], employee_data['employee'], employee_data['supervisor'], employee_data['email'], employee_data['status']))
            self.conn.commit()
            self.logger.info(f"Inserted new Airtable employee: {employee_data['employee']}.")
        except Exception as e:
            self.logger.error(f"Error inserting new Airtable employee: {e}")
            self.conn.rollback()

    def insert_pipefy_employee(self, employee_data):
        """
        Insert a new employee record into the Pipefy table.
        Args:
            employee_data (dict): A dictionary containing employee data.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO cleaned_pipefy (boxfusioner, email, status, supervisor, title, pipefy_id, airtable_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (employee_data['boxfusioner'], employee_data['email'], employee_data['status'], employee_data['supervisor'], employee_data['title'], employee_data['pipefy_id'], employee_data['airtable_id']))
            self.conn.commit()
            self.logger.info(f"Inserted new employee into Cleaned Pipefy: {employee_data['boxfusioner']}.")
        except Exception as e:
            self.logger.error(f"Error inserting new employee into Cleaned Pipefy: {e}")
            self.conn.rollback()

    def update_airtable_employee(self, airtable_id, employee_data):
        """
        Update an existing employee record in the Airtable table.
        Args:
            airtable_id (str): The Airtable record ID to update.
            employee_data (dict): A dictionary containing updated employee data.
        """
        try:
            if not employee_data:
                self.logger.info(f"No fields to update for employee with ID {airtable_id}. Skipping update.")
                return

            with self.conn.cursor() as cursor:
                # Dynamically build the SET clause and values based on employee_data keys
                set_clauses = []
                values = []
                for key, value in employee_data.items():
                    set_clauses.append(f"{key} = %s")
                    values.append(value)
                set_clause_str = ", ".join(set_clauses)
                values.append(airtable_id)  # Add the id for the WHERE clause
                query = f"""
                    UPDATE airtable
                    SET {set_clause_str}
                    WHERE id = %s
                """
                cursor.execute(query, values)
            self.conn.commit()
            self.logger.info(f"Updated employee with ID {airtable_id}.")
        except Exception as e:
            self.logger.error(f"Error updating employee with ID {airtable_id}: {e}")
            self.conn.rollback()

    def delete_airtable_employee(self, airtable_id):
        """
        Delete an employee record from the Airtable table by ID.
        Args:
            airtable_id (str): The Airtable record ID to delete.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM airtable WHERE id = %s
                """, (airtable_id,))
            self.conn.commit()
            self.logger.info(f"Deleted employee with ID {airtable_id}.")
        except Exception as e:
            self.logger.error(f"Error deleting employee with ID {airtable_id}: {e}")
            self.conn.rollback()

    def insert_airtable_sale(self, project_data):
        """
        Insert a new project record into the sales table.
        Args:
            project_data (dict): A dictionary containing project data.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO sales (opportunity_code, opportunity_stage, owner_name, owner_email, airtable_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (project_data['opportunity_code'], project_data['opportunity_stage'], project_data['owner_name'], project_data['owner_email'], project_data['airtable_id']))
            self.conn.commit()
            self.logger.info(f"Inserted new project: {project_data['opportunity_code']}.")
        except Exception as e:
            self.logger.error(f"Error inserting new project: {e}")
            self.conn.rollback()

    def insert_pipefy_project(self, project_data):
        """
        Insert a new project record into the Pipefy table.
        Args:
            project_data (dict): A dictionary containing project data.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO cleaned_projects (title, project_code, project_status, project_owner, project_owner_email, pipefy_id, airtable_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (project_data['title'], project_data['project_code'], project_data['project_status'], project_data['project_owner'], project_data['project_owner_email'], project_data['pipefy_id'], project_data['airtable_id']))
            self.conn.commit()
            self.logger.info(f"Inserted new project into Pipefy: {project_data['title']}.")
        except Exception as e:
            self.logger.error(f"Error inserting new project into Pipefy: {e}")
            self.conn.rollback()

    def update_airtable_sale(self, airtable_id, project_data):
        """
        Update an existing project record in the sales table.
        Args:
            airtable_id (str): The Airtable record ID to update.
            project_data (dict): A dictionary containing updated project data.
        """
        try:
            if not project_data:
                self.logger.info(f"No fields to update for project with ID {airtable_id}. Skipping update.")
                return
            
            with self.conn.cursor() as cursor:
                # Dynamically build the SET clause and values based on project_data keys
                set_clauses = []
                values = []
                for key, value in project_data.items():
                    set_clauses.append(f"{key} = %s")
                    values.append(value)
                set_clause_str = ", ".join(set_clauses)
                values.append(airtable_id)  # Add the id for the WHERE clause
                query = f"""
                    UPDATE sales
                    SET {set_clause_str}
                    WHERE airtable_id = %s
                """
                cursor.execute(query, values)
            self.conn.commit()
            self.logger.info(f"Updated project with ID {airtable_id}.")
        except Exception as e:
            self.logger.error(f"Error updating project with ID {airtable_id}: {e}")
            self.conn.rollback()

    def delete_airtable_project(self, airtable_id):
        """
        Delete a project record from the sales table by Airtable ID.
        Args:
            airtable_id (str): The Airtable record ID to delete.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM sales WHERE airtable_id = %s
                """, (airtable_id,))
            self.conn.commit()
            self.logger.info(f"Deleted project with ID {airtable_id}.")
        except Exception as e:
            self.logger.error(f"Error deleting project with ID {airtable_id}: {e}")
            self.conn.rollback()

    def close(self):
        """
        Close the database connection and log the closure.
        """
        self.conn.close()
        self.logger.info("Database connection closed.")
