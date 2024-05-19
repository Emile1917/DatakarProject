
from google.cloud import bigquery
from google.oauth2 import service_account

class ConnectionBigQuery :
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.credentialsPath = "credentials/gcp-bigquery-credentials.json"
            self.credentials = service_account.Credentials.from_service_account_file(self.credentialsPath)
            self.client = bigquery.Client(credentials=self.credentials)
            self.bq = bigquery

    def getConnection(self):
        return self.client        

    
    def getBigQuery(self):
        return self.bq

    def querying(self,query,job_config) :
        try :
            client = self.client
            pandas_df = client.query_and_wait(query,job_config=job_config).to_dataframe()
            return pandas_df
        except  Exception as e:
            print("La requête à échoué",e)    
        
   

    
