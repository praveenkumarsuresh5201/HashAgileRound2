from elasticsearch import Elasticsearch, helpers
import pandas as pd
import warnings
from elasticsearch.exceptions import ElasticsearchWarning
import json

warnings.filterwarnings("ignore", category=ElasticsearchWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

client = Elasticsearch(
    "http://localhost:9200",
    verify_certs=False,
    ssl_show_warn=False
)

def create_collection(p_collection_name):
    try:
        if not client.indices.exists(index=p_collection_name):
            response = client.indices.create(
                index=p_collection_name,
                body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    },
                    "mappings": {
                        "properties": {
                            "Department": {"type": "keyword"},
                            "Gender": {"type": "keyword"},
                            "Employee ID": {"type": "keyword"}
                        }
                    }
                }
            )
            print(f"Collection {p_collection_name} created successfully")
        else:
            print(f"Collection {p_collection_name} already exists")
    except Exception as error:
        print("Error creating collection:", str(error))

def index_data(p_collection_name, p_exclude_column):
    try:
        df = pd.read_csv("EmployeeSampleData1.csv", encoding='ISO-8859-1')
        print(f"Read {len(df)} records from CSV file")
        
        if p_exclude_column in df.columns:
            df = df.drop(columns=[p_exclude_column])
        
        df = df.where(pd.notnull(df), None)
        
        records = df.to_dict('records')
        
        actions = [
            {
                "_index": p_collection_name,
                "_source": record
            }
            for record in records
        ]
        
        success, failed = helpers.bulk(
            client,
            actions,
            stats_only=True,
            raise_on_error=False
        )
        
        print(f"Successfully indexed {success} documents in {p_collection_name}")
        if failed:
            print(f"Failed to index {failed} documents")
            
    except Exception as error:
        print("Error indexing data:", str(error))
        print("Please ensure your CSV file exists and has the correct format")

def search_by_column(p_collection_name, p_column_name, p_column_value):
    try:
        client.indices.refresh(index=p_collection_name)
        
        response = client.search(
            index=p_collection_name,
            body={
                "query": {
                    "match": {
                        p_column_name: p_column_value
                    }
                },
                "size": 100
            }
        )
        
        hits = response['hits']['hits']
        if hits:
            print(f"\nFound {len(hits)} matches for {p_column_name}: {p_column_value}")
        else:
            print(f"\nNo matches found for {p_column_name}: {p_column_value}")
    except Exception as error:
        print("Error searching data:", str(error))

def del_emp_by_id(p_collection_name, employee_id):
    try:
        response = client.search(
            index=p_collection_name,
            body={
                "query": {
                    "match": {
                        "Employee ID": employee_id
                    }
                }
            }
        )
        
        hits = response['hits']['hits']
        if hits:
            for hit in hits:
                document_id = hit['_id']
                client.delete(
                    index=p_collection_name,
                    id=document_id
                )
            
            print(f"Employee ID: {employee_id} deleted")
        else:
            print(f"No documents found with Employee ID: {employee_id}")
            
    except Exception as error:
        print("Error deleting employee:", str(error))

def get_emp_count(p_collection_name):
    try:
        client.indices.refresh(index=p_collection_name)
        
        response = client.count(index=p_collection_name)
        count = response['count']
        print(f"\nTotal employees in {p_collection_name}: {count}")
        return count
    except Exception as error:
        print("Error getting employee count:", str(error))
        return 0

def get_dep_facet(p_collection_name):
    try:
        client.indices.refresh(index=p_collection_name)

        body = {
            "size": 0,
            "aggs": {
                "departments_keyword": {
                    "terms": {
                        "field": "Department.keyword",
                        "size": 100,
                        "min_doc_count": 1
                    }
                },
                "departments_raw": {
                    "terms": {
                        "field": "Department",
                        "size": 100,
                        "min_doc_count": 1
                    }
                }
            }
        }

        response = client.search(index=p_collection_name, body=body)

        keyword_buckets = response['aggregations']['departments_keyword']['buckets']
        raw_buckets = response['aggregations']['departments_raw']['buckets']

        if raw_buckets:
            print("\nDepartment facet:")
            for bucket in raw_buckets:
                print(f"{bucket['key']}: {bucket['doc_count']} employees")

        if not keyword_buckets and not raw_buckets:
            print("\nNo department data found")

    except Exception as e:
        print(f"\nDetailed error in get_dep_facet: {e}")

def main():
    v_name_collection = "hash_praveenkumar"
    v_phone_collection = "hash_0233"
    
    print("\nCreating collections:")
    create_collection(v_name_collection)
    create_collection(v_phone_collection)

    print("\nInitial counts:")
    get_emp_count(v_name_collection)
    get_emp_count(v_phone_collection)
    
    print("\nIndexing data:")
    index_data(v_name_collection, "Department")
    index_data(v_phone_collection, "Gender")

    print("\nUpdated counts:")
    get_emp_count(v_name_collection)
    get_emp_count(v_phone_collection)

    print("\nDeleting employee:")
    del_emp_by_id(v_name_collection, "E02004")
    
    print("\nSearching for IT department employees:")
    search_by_column(v_name_collection, "Department", "IT")
    print("\nSearching for Male employees:")
    search_by_column(v_name_collection, "Gender", "Male")
    print("\nSearching for IT department employees :")
    search_by_column(v_phone_collection, "Department", "IT")
    
    print("\nGetting department facets:")
    get_dep_facet(v_name_collection)
    get_dep_facet(v_phone_collection)

if __name__ == "__main__":
    main()
