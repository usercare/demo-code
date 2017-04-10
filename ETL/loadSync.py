#!/usr/bin/python
'''preamble: 
Take input arg
If arg URL open & convert to file, else directly treat as file
Operate on file, chunking input to avoid overloading API, transform into JSON
Submit to Sync API endpoint 

reference: POST /api/v1/<Publisher_API_KEY>/sync_customers/ HTTP/1.1
https://doc.agent.ai/developer/integrations/#customer-information-sync-api
'''

import sys
import json
import requests
import StringIO
import base64

# CONSTANTS...SETTABLE OPTIONS FOR DIFFERENT CONFIGURATIONS
API_KEY = 'change_me' #prod
SYNC_SERVER = 'my.agent.ai' #prod
PUBLISHER_ADMIN_USERNAME = 'admin@company.com'
PUBLISHER_ADMIN_PASSWORD = 'changeme'
CHUNK_SIZE = 400000 #current limit of API

if len(sys.argv)<3:
    print "Error: You must include the input file name & app group name as command-line args."
    print "Usage: python %s <input_file> <app_group_name>" % sys.argv[0]
    sys.exit(1)

in_obj = sys.argv[1]
if (in_obj.startswith('http')): #it's a URL to a remote file
	r = requests.get(in_obj)
	input_file = StringIO.StringIO(r.content)
else: #it's a local file
	input_file = open(sys.argv[1], 'r')
toss = input_file.readline() #toss header
app_group_name = sys.argv[2]
keys = ['id','last_name']
url = "https://" + SYNC_SERVER + "/api/v1/" + API_KEY + "/sync_customers/"
# The authentication header for the web service
HTTP_BASIC_AUTHORIZATION = base64.b64encode(PUBLISHER_ADMIN_USERNAME + ':' + PUBLISHER_ADMIN_PASSWORD)
readChunk = input_file.readlines(CHUNK_SIZE)

while readChunk:
    customers = [] #reset customer list
    for line in readChunk:
        line = line.rstrip()
        line = line.decode('latin-1') #spanish,italian,brazilian,mexican
        cust_list = line.split(";")
        cust_obj = dict(zip(keys, cust_list)) #gets first 2
        prop_list={}
        prop_list['properties'] = {"blacklisted":cust_list[-3],"member_status":cust_list[-2],"member_modified":cust_list[-1]}
        cust_obj.update(prop_list)
        customers.append(cust_obj)

    payload = {"app_group": app_group_name, "customers":customers}
    payload_json = json.dumps(payload,ensure_ascii=False).encode('latin-1') #save it back to database with proper encoding, ensure_ascii defaults to True
    #print payload_json
    #sys.exit(0)
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'authorization': "Basic " + HTTP_BASIC_AUTHORIZATION
    }
    response = requests.request("POST", url, data=payload_json, headers=headers)
    print(response.text)
    readChunk = input_file.readlines(CHUNK_SIZE) #read in next chunk

input_file.close()
