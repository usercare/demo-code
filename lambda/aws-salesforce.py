from __future__ import print_function

"""
This file contains a sample AWS Lambda function that can be called from the UserCare service.
This function should taken as a sample. It isn't as robust as a real function might need to be.
We treat Salesforce as the source, synchronizing on the Contact ID, call it to retrieve the 
information and then push the retrieved values into UserCare's database.
This function has two call modes: `ticket_created` and `session`.
`ticket_created`
event types are called immediately upon creation of a ticket by the end user. It is expected that this
function will update the user profile in UserCare quite quickly. The aim is that the call will
complete before the agent opens the ticket, which could be a matter of a few seconds.
`session`
event types are called whenever the user creates a new session. As this happens all
the time it is recommended that you store the customer id or IDFA to one side (maybe in a DynamoDB
data store) and then periodically process the list and update UserCare in the background.

psuedo-code:
a) call to SFDC to get token
b) call SFDC using customer ID (passed into via event)
c) update salutation, name, title, email
"""

import base64
from datetime import datetime, timedelta, tzinfo
import json
import logging
import pytz
import requests
import pprint
import sys

# Setting for your CRM of choice (other systems might use different auth mechanisms)
CRM_CLIENT_ID = 'FROM_YOUR_SALESFORCE_SETUP'
CRM_CLIENT_SECRET = 'FROM_YOUR_SALESFORCE_SETUP'
CRM_USERNAME = 'CRM_USERNAME'
CRM_PASSWORD = 'CRM_PASSWORD'

# NOTE: SFDC requires TLS > 1.0
def fetch_token():
    url = "https://login.salesforce.com/services/oauth2/token"
    querystring = {"grant_type":"password","client_id":CRM_CLIENT_ID,"client_secret":CRM_CLIENT_SECRET,"username":CRM_USERNAME,"password":CRM_PASSWORD}
    return requests.post(url, data=querystring)

token_response = fetch_token().json()
token = token_response['access_token']

def fetch_contact(id):
    url = "https://na35.salesforce.com/services/data/v20.0/sobjects/Contact/" + id
    querystring = {"fields":"Salutation,FirstName,LastName,Title,Email"}
    headers = {'authorization':"Bearer " + token}
    return requests.get(url, headers=headers, params=querystring)

# Settings for the web service connection to update the UserCare user profile
PUBLISHER_ADMIN_USERNAME = u'CHANGE_THIS_TO_ADMIN_USER_INFO'
PUBLISHER_ADMIN_PASSWORD = u'CHANGE_THIS_TO_ADMIN_USER_PASSWORD'
PUBLISHER_API_KEY = u'CHANGE_THIS_TO_YOUR_API_KEY'

# Constants
# The UserCare host for the web service
CUSTOMER_SYNC_HOST = u'sync.usercare.com'
# The overall URL for the web service
CUSTOMER_SYNC_URL = u'https://' + CUSTOMER_SYNC_HOST + u'/api/v1/' + PUBLISHER_API_KEY + u'/sync_customers'
# The authentication header for the web service
HTTP_BASIC_AUTHORIZATION = base64.b64encode(PUBLISHER_ADMIN_USERNAME + u':' + PUBLISHER_ADMIN_PASSWORD)

# Logging - reduce level to WARNING or ERROR for production
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    """
    AWS Lambda Function main entry point.
    Accepts the following event parameters:
    event_type - Event type triggering invocation, 'session' or 'ticket_created'.
    id - Externally defined customer id set via SDK or customer sync, optional.
    IDFA - Device identification supplied if id not set, optional.
    timestamp - Customer sync timestamp, ('2016-01-01T00:00:00.000Z', UTC), optional.
    When the UserCare server sends these events all the fields will be present.
    However, some of the values may be set to None in the event. Your code should
    check for that an handle accordingly.
    """

    # Extract customer sync parameters from the event
    # id param is CRM id
    event_type = event.get('event_type','session')
    id = event.get('id',None)
    IDFA = event.get('IDFA',None)
    timestamp = parse_iso_8601_timestamp(event.get('timestamp',u'2016-05-29T11:45:13.381Z'))
    logger.info("got event: " + json.dumps(event))

    # Ensure that the timestamp of last sync update was more than 10 seconds ago
    customer_sync_data_timestamp = pytz.UTC.localize(datetime.now())
    if timestamp is not None and (customer_sync_data_timestamp - timestamp).total_seconds() < 10:
        logger.info("Last update was less than 10 seconds ago")
        return
   
    contact = fetch_contact(id).json()

    # Build customer sync data object
    customer_sync_data = {
        u'customers': [{
            u'id': contact['Id'],
            u'IDFA': IDFA,
            u'email': contact['Email'],
            u'first_name': contact['FirstName'],
            u'last_name': contact['LastName'],
            u'first_session': format_iso_8601_timestamp(parse_iso_8601_timestamp(u'2016-01-01T00:00:00.000Z')),
            u'properties': {
                u'Salutation': contact['Salutation'],
                u'Title': contact['Title'],
            },
            u'timestamp': format_iso_8601_timestamp(customer_sync_data_timestamp)
        }]
    }

    # Convert the data structure to JSON to post to UserCare
    customer_sync_data_json = json.dumps(customer_sync_data)

    # Asynchronous sync customer data request
    response = requests.post(CUSTOMER_SYNC_URL, data=customer_sync_data_json,
        headers={
            u'Authorization': u'Basic ' + HTTP_BASIC_AUTHORIZATION,
            u'Content-Type': u'application/json'
        }
     )

    # Raise and error back to the Lambda function caller if the sync fails
    if response.status_code != 200:
        raise RuntimeError(u'Customer sync post failed, status: {0}, message: {1}'.format(response.status_code, response.content))

    # Check sync customer response to make sure we have no errors
    response_json = json.loads(response.content)
    created_count = response_json[u'created_count']
    updated_count = response_json[u'updated_count']
    error_count = response_json[u'error_count']
    # If we do raise an error back to the Lambda function caller
    if error_count != 0:
        raise RuntimeError(u'Customer sync post response errors: {0}'.format(error_count))

    # Send response back to caller
    return None


class UtcTZInfo(tzinfo):
    """
    UTC timezone used for timestamps.
    """

    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)

UTC = UtcTZInfo()


def parse_iso_8601_timestamp(timestamp):
    """
    Parse ISO 8601 formatted timestamp strings.
    :param timestamp: UTC timestamp in ISO 8601 format
    :return: UTC datetime timestamp
    """

    if timestamp is None or len(timestamp) != 24 or timestamp[-1] != 'Z':
        return None
    return datetime.strptime(timestamp[:-1]+u'000', u'%Y-%m-%dT%H:%M:%S.%f').replace(tzinfo=UTC)


def format_iso_8601_timestamp(timestamp):
    """
    Format datetime ISO 8601 formatted timestamp strings.
    :param timestamp: UTC datetime timestamp
    :return: UTC timestamp in ISO 8601 format
    """

    if timestamp is None:
        return None
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z'

if __name__ == '__main__':
    """
    Command line main entry point.
    Usage:
    Examples:
    `> python aws-salesforce.py ticket_created -id 00341000003EXY5`
    `> python aws-salesforce.py session -idfa AEBE52E7-03EE-455A-B3C4-E57283966239`
    """

    # Parse command line arguments
    event_type = unicode(sys.argv[1])
    id = None
    IDFA = None
    if sys.argv[2] == '-id':
        id = unicode(sys.argv[3])
    elif sys.argv[2] == '-idfa':
        IDFA = unicode(sys.argv[3])
        timestamp = unicode(sys.argv[4]) if len(sys.argv) == 5 else None

    # Invoke AWS Lambda Function handler
    lambda_handler({
        u'event_type': event_type,
        u'id': id,
        u'IDFA': IDFA,
        u'timestamp': timestamp
    }, None)