import sys

sys.path.append('./')

from scripts.connection.ConnectionBigQuery import ConnectionBigQuery

class Extraction:

    def __init__(self, project, dataset_id, tables_path):
        self.conn = ConnectionBigQuery()
        self.client = self.conn.getConnection()
        self.bq = self.conn.getBigQuery()
        self.project = project
        self.dataset_id = dataset_id
        self.tables_path = tables_path

    def get_all_tables_from_db(self):
        query = (
            'SELECT table_name FROM `{}.INFORMATION_SCHEMA.TABLES` '.format(self.dataset_id)
        )
        tables = list(self.conn.querying(query, None)['table_name'])
        return tables

    def write_tables_locally(self):
        tables = self.get_all_tables_from_db()
        for t in tables:
            dataset_ref = self.bq.DatasetReference(self.project, self.dataset_id)
            table_ref = dataset_ref.table(t)
            table = self.client.get_table(table_ref)
            df = self.client.list_rows(table).to_dataframe()
            df.to_csv(self.tables_path + t + '.csv', index=False)

if __name__ == "__main__":
    project = "test-data-engineer-090621"
    dataset_id = "test_dataset"
    tables_path = './data/tables/'

    handler = Extraction(project, dataset_id, tables_path)
    handler.write_tables_locally()
