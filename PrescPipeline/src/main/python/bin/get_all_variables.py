import os

### Set Environment Variables
os.environ['envn'] = 'PROD'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'sparkuser1'
os.environ['password'] = 'user123'

### Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']

### Set Other Variables
appName="USA Prescriber Research Report"
current_path =  os.getcwd()

staging_dim_city="PrescPipeline/staging/dimension_city"
staging_fact = "PrescPipeline/staging/fact"

output_city="PrescPipeline/output/dimension_city"
output_fact="PrescPipeline/output/presc"
