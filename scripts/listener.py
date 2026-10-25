from flask import Flask, request
from pyairtable import Api
import json
from middleman import Middleman
from logger import setup_logger

middleman = Middleman()
logger = setup_logger("listener")

app = Flask(__name__)
app.config.from_pyfile('flask_config.py')

@app.route('/sales/webhook', methods=['POST'])
def sales_webhook():
    body = request.data
    event = json.loads(body)
    airtable = Api(app.config["AIRTABLE_API_KEY"])
    webhook = airtable.base(event['base']['id']).webhook(event['webhook']['id'])
    cursor = middleman.get_webhook_cursor(event['webhook']['id']) + 1

    for payload in webhook.payloads(cursor=cursor):
        middleman.process_sales_payload(payload)

        json_payload = json.dumps({
            "source": payload.action_metadata.source,
            "source_metadata": payload.action_metadata.source_metadata,
            "payload": str(payload.changed_tables_by_id)
        })

        middleman.process_webhook(event['webhook']['id'], json_payload, payload.timestamp, payload.cursor)

    return ("", 204)

@app.route('/employee/webhook', methods=['POST'])
def employee_webhook():
    body = request.data
    event = json.loads(body)
    airtable = Api(app.config["AIRTABLE_API_KEY"])
    webhook = airtable.base(event['base']['id']).webhook(event['webhook']['id'])
    cursor = middleman.get_webhook_cursor(event['webhook']['id']) + 1

    for payload in webhook.payloads(cursor=cursor):
        middleman.process_employee_payload(payload)

        json_payload = json.dumps({
            "source": payload.action_metadata.source,
            "source_metadata": payload.action_metadata.source_metadata,
            "payload": str(payload.changed_tables_by_id)
        })

        middleman.process_webhook(event['webhook']['id'], json_payload, payload.timestamp, payload.cursor)

    return ("", 204)

@app.route('/test/webhook', methods=['POST'])
def test_webhook():
    body = request.data
    event = json.loads(body)
    airtable = Api(app.config["AIRTABLE_API_KEY"])
    webhook = airtable.base(event['base']['id']).webhook(event['webhook']['id'])
    cursor = middleman.get_webhook_cursor(event['webhook']['id']) + 1

    for payload in webhook.payloads(cursor=cursor):
        middleman.process_employee_payload(payload)

        json_payload = json.dumps({
            "source": payload.action_metadata.source,
            "source_metadata": payload.action_metadata.source_metadata,
            "payload": str(payload.changed_tables_by_id)
        })

        middleman.process_webhook(event['webhook']['id'], json_payload, payload.timestamp, payload.cursor)

    return ("", 204)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)