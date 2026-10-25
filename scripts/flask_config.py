"""Flask config."""
import os
from dotenv import load_dotenv

load_dotenv()

SALES_WEBHOOK_SECRET = os.getenv('SALES_WEBHOOK_SECRET')
EMPLOYEE_WEBHOOK_SECRET = os.getenv('EMPLOYEE_WEBHOOK_SECRET')
TEST_WEBHOOK_SECRET = os.getenv('TEST_WEBHOOK_SECRET')
AIRTABLE_API_KEY = os.getenv('AIRTABLE_TOKEN')