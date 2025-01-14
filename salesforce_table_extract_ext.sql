--setup database and schema
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE DATABASE SALESFORCE;
USE SCHEMA SALESFORCE.PUBLIC;
USE WAREHOUSE XS_WH;

--create a network rule and secrets
CREATE OR REPLACE NETWORK RULE SALESFORCE.PUBLIC.SALESFORCE_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
-- regadring Salesforce url:
-- Does not work - <your org>.develop.lightning.force.com
-- Works - <your org>.develop.my.salesforce.com
  
  VALUE_LIST = ('login.salesforce.com','<your org>.develop.my.salesforce.com');


  
--create secrets
CREATE OR REPLACE SECRET SALESFORCE.PUBLIC.USERNAME
  TYPE = GENERIC_STRING
   -- insert login id for Salesforce, email format
  SECRET_STRING = '<your login email>';

CREATE OR REPLACE SECRET SALESFORCE.PUBLIC.PASSWORD
  TYPE = GENERIC_STRING
  -- insert password to login to Salesforce
  SECRET_STRING = '<your password>';

CREATE OR REPLACE SECRET SALESFORCE.PUBLIC.TOKEN
  TYPE = GENERIC_STRING
  -- reset security token, won't see reset option if have login IP ranges on your profile
  --   had to remove IP login range
    SECRET_STRING = '<your secret string>';

 

--create external access integration
--must be created by accountadmin role
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION SALESFORCE_API_INTEGRATION
  ALLOWED_NETWORK_RULES = (SALESFORCE.PUBLIC.SALESFORCE_NETWORK_RULE)
  ALLOWED_AUTHENTICATION_SECRETS = (SALESFORCE.PUBLIC.USERNAME,SALESFORCE.PUBLIC.PASSWORD,SALESFORCE.PUBLIC.TOKEN)
  ENABLED = true;

CREATE OR REPLACE FUNCTION SALESFORCE.PUBLIC.GET_DATA_UDTF()
RETURNS TABLE (json_data variant)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'get_data'
EXTERNAL_ACCESS_INTEGRATIONS = (SALESFORCE_API_INTEGRATION)
PACKAGES = ('simple-salesforce')
SECRETS = ('username' = SALESFORCE.PUBLIC.USERNAME, 'password'=SALESFORCE.PUBLIC.PASSWORD, 'token'=SALESFORCE.PUBLIC.TOKEN)
AS
$$
import _snowflake
from simple_salesforce import Salesforce

class get_data:
    def process(self, ):
        # Retrieve Salesforce credentials from Snowflake secrets
        username = _snowflake.get_generic_secret_string('username')
        password = _snowflake.get_generic_secret_string('password')
        security_token = _snowflake.get_generic_secret_string('token')

        # Connect to Salesforce using simple-salesforce
        sf = Salesforce(username=username, password=password, security_token=security_token)

        # Define the SOQL query
        query = "SELECT Id, Industry, Name FROM Account LIMIT 10"
        accounts = sf.query(query)

        # Execute the query
        result = sf.query(query)

        # Return the records
        for row in result['records']:
            yield(row,)
$$
;



--test UDTF
SELECT *
FROM TABLE(SALESFORCE.PUBLIC.GET_DATA_UDTF());

--parse
WITH JSON_RESULTS AS
    (SELECT *
    FROM TABLE(SALESFORCE.PUBLIC.GET_DATA_UDTF()))
SELECT
    JSON_DATA:Id::string ID,
    JSON_DATA:Industry::string INDUSTRY,
    JSON_DATA:Name::string NAME,
    JSON_DATA:attributes.type::string TYPE,
    JSON_DATA:attributes.url::string URL,
    JSON_DATA raw_data
FROM JSON_RESULTS
;

