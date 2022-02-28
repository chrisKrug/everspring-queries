# Queries Banner tables to produce
# result set containing
# colleges and departments
# to populate Everspring's 'Organization' table

# Import modules
import cx_Oracle    # connecting to Oracle DB
import pandas as pd # data wrangling and creating csv
import configparser # reading credentials from central location

config = configparser.ConfigParser()
config.read('/home/crkrug/street_talk.ini')

user = config.get('banner','user')
password = config.get('banner','password')
dsn = config.get('banner','dsn')

# Connect to Banner/Oracle DB
connection = cx_Oracle.connect(user=user, password=password,dsn=dsn)
cursor = connection.cursor()
query = """
SELECT DISTINCT
DEPT_CODE, --organizationId X
COLL_CODE, --parentId O
-- COLL_DESC, /* I don't think they need this */
DEPT_DESC, --name X
CASE
WHEN DEPT_DESC IS NOT NULL THEN 'Department'
ELSE
CASE
WHEN COLL_DESC LIKE '%College%' THEN 'College'
ELSE 'School'
END
END AS ORGANIZATION_TYPE --organizationType X
--longDescription O
--shortDescription O
--status 0
FROM AS_CATALOG_SCHEDULE
ORDER BY DEPT_CODE
"""
# Query DB
cursor.execute(query)

# Construct Pandas Data Frame
row_list = []
for row in cursor:
    row_list.append(tuple(row))

table = pd.DataFrame(row_list,columns=['DEPT_CODE','COLL_CODE', #'COLL_DESC'
                                       'DEPT_DESC','ORGANIZATION_TYPE'])

print(table)

# Close connection to Banner
connection.close()

# Produce CSV file
table.to_csv('organizations.csv',index=False)

# Send file to Everspring's FTP server

