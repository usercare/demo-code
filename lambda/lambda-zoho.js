var request = require('request');

// Settings for the web service connection to update the UserCare user profile
var PUBLISHER_ADMIN_USERNAME = 'CHANGE_THIS_TO_ADMIN_USER_INFO';
var PUBLISHER_ADMIN_PASSWORD = 'CHANGE_THIS_TO_ADMIN_USER_PASSWORD';
var PUBLISHER_API_KEY = 'CHANGE_THIS_TO_YOUR_API_KEY';
// Setting for your CRM of choice (other systems might use different auth mechanisms)
var CRM_KEY = 'CHANGE_THIS_TO_ZOHO_CRM_API_KEY';

var CUSTOMER_SYNC_HOST = 'sync.usercare.com';
// The overall URL for the web service
CUSTOMER_SYNC_URL = 'https://' + CUSTOMER_SYNC_HOST + '/api/v1/' + PUBLISHER_API_KEY + '/sync_customers';

exports.handler = function (event, context) {
    var b = new Buffer(PUBLISHER_ADMIN_USERNAME + ':' + PUBLISHER_ADMIN_PASSWORD);
    var HTTP_BASIC_AUTHORIZATION = b.toString('base64');
    
    var event_type = event.event_type;
    var id = event.id;
    var IDFA = event.IDFA;
    var timestamp = event.timestamp;
    
    build_contact(id, function (customer_sync_data) {
    
        var options = {
            uri: CUSTOMER_SYNC_URL,
            body: JSON.stringify(customer_sync_data),
            headers: {'Authorization': 'Basic ' + HTTP_BASIC_AUTHORIZATION}
        };
    
        request.post(options, function(error, response, body) {
            if (error) {
                return console.error('post failed: ', error);
            }
            context.succeed('Success!');
        });
    });
}

function clean_contact(obj, callback) {
    callback(JSON.parse(obj).response.result.Contacts.row.FL);
}

function get_field(arr, value, callback) {
    arr.filter(function(field) {
        if (field.val == value) {
            callback(field);
        }
    });
}

function build_contact (id, callback) {
    get_contact(id, function(result) {
        clean_contact(result, function(arr) {
                get_field(arr, 'CONTACTID', function(want) {
                    idval = (want.content);
                });
                get_field(arr, 'Email', function(want){
                     emailval = want.content;
                });
                get_field(arr, 'First Name', function(want){
                    fname = want.content;
                });
                get_field(arr, 'Last Name', function(want){
                    lname = want.content;
                });
                get_field(arr, 'Salutation', function(want){
                        sal = want.content;
                });
                get_field(arr, 'Title', function(want){
                        title = want.content;
                });
            customer_sync_data = {
                customers : [{ id: idval,
                                IDFA:null,
                                first_name:fname,
                                last_name:lname,
                                email:emailval,
                                properties:{ Salutation:sal,
                                             Title:title}
                             }]
           }
        });
        callback(customer_sync_data);
    });
}

function get_contact(id, callback) {
    var options = { 
        url: 'https://crm.zoho.com/crm/private/json/Contacts/getSearchRecordsByPDC',
        qs: 
         { authtoken: CRM_KEY,
           scope: 'crmapi',
           searchColumn: 'contactid',
           searchValue: id}
    };

    request(options, function (error, response, body) {
        if (!error && response.statusCode == 200) {
            callback(body);
        } else { 
            callback(error);
        }
    });
}