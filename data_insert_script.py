import requests
import json
from enum import Enum
import math
import random
import string
from datetime import datetime, timedelta
import os
import time

class VtexHttpClient:
  def __init__(self, appKey, appToken):
    self.headers = {
    'X-VTEX-API-AppKey': appKey,
    'X-VTEX-API-AppToken': appToken,
    'x-vtex-api-appKey': appKey,
    'x-vtex-api-appToken': appToken,
    'Content-Type':'application/json'
  }

  def handleHttpResponse(self, response):
    try:
        response.raise_for_status()
        if (not response.text):
            return{
                'httpStatus': 200,
                'data':"Empty response"
            }
        return {
            'httpStatus': 200,
            'data': response.text if isinstance(response.text, int) else json.loads(response.text)
        }
    except requests.exceptions.RequestException as err:
        return {
            'httpStatus': response.status_code,
            'message': response.reason
        }
    except requests.exceptions.HTTPError as errh:
        return {
            'httpStatus': response.status_code,
            'message': response.reason
        }
    except requests.exceptions.ConnectionError as errc:
        return {
            'httpStatus': response.status_code,
            'message': response.reason
        }
    except requests.exceptions.Timeout as errt:
        return {
            'httpStatus': response.status_code,
            'message': response.reason
        }



  def get(self, url, headers=None):
      merged_headers = {**self.headers, **(headers or {})}
      res = requests.get(url, headers=merged_headers, allow_redirects=True)
      return self.handleHttpResponse(res)
  
  def getWithHeaders(self, url, headers=None):
      merged_headers = {**self.headers, **(headers or {})}
      return requests.get(url, headers=merged_headers, allow_redirects=True)

  def post(self, url, data):
      return self.handleHttpResponse(requests.post(url, data=json.dumps(data), headers = self.headers, allow_redirects=True))

  def put(self, url, data):
      return self.handleHttpResponse(requests.put(url, data=json.dumps(data), headers = self.headers, allow_redirects=True))

  def patch(self, url, data):
      return self.handleHttpResponse(requests.patch(url, data=json.dumps(data), headers = self.headers, allow_redirects=True))

  def delete(self, url):
      return self.handleHttpResponse(requests.delete(url, headers = self.headers, allow_redirects=True))

class VtexMasterDataClient:
  def __init__(self, accountName, appKey, appToken):
    self.baseCommerceStableUrl = f"https://{accountName}.vtexcommercestable.com.br/api/dataentities/"
    self.baseApiUrl = f"http://api.vtex.com/{accountName}/dataentities/"
    self.httpClient = VtexHttpClient(appKey, appToken)

  def buildParamsUrl(self,fields, schema, where, sort, size):
    urls = []
    if isinstance(fields, list) and bool(fields):
      urls.append(f"_fields={','.join(fields)}")
    else:
      urls.append(f"_fields=_all")

    if where is not None and bool(where):
      urls.append(f"_where={where}")

    if schema is not None and bool(schema):
      urls.append(f"_schema={schema}")

    if sort is not None and bool(sort):
      urls.append(f"_sort={sort}")

    if sort is not None and bool(sort):
      urls.append(f"_sort={sort}")

    if isinstance(size, (int, float, complex)) and size is not None:
      urls.append(f"_size={size}")
    else:
      urls.append(f"_size=200")

    return "&".join(urls)

  def scroll(self, entityName, schemaName, fields, where, sort, size):
    requestUrl = f"{self.baseCommerceStableUrl}{entityName}/scroll"
    paramsUrl = self.buildParamsUrl(fields, schemaName, where, sort, size)

    return self.httpClient.get(f"{requestUrl}?{paramsUrl}")

  def getDocument(self, entityName, documentId, fields, schemaName):
    requestUrl = f"{self.baseCommerceStableUrl}{entityName}/documents/{documentId}"
    paramsUrl = self.buildParamsUrl(fields, schemaName, None, None, None)

    return self.httpClient.get(f"{requestUrl}?{paramsUrl}")

  def createDocument(self, entityName, schemaName, data):
    requestUrl = f"{self.baseCommerceStableUrl}{entityName}/documents"
    paramsUrl = self.buildParamsUrl(None, schemaName, None, None, None)

    return self.httpClient.post(f"{requestUrl}?{paramsUrl}", data)

  def updateDocument(self, entityName, schemaName, documentId, where, data):
    requestUrl = f"{self.baseCommerceStableUrl}{entityName}/documents/{documentId}"
    paramsUrl = self.buildParamsUrl(None, schemaName, where, None, None)

    return self.httpClient.put(f"{requestUrl}?{paramsUrl}", data)

  def updatePartialDocument(self, entityName, schemaName, documentId, where, data):
    requestUrl = f"{self.baseCommerceStableUrl}{entityName}/documents/{documentId}"
    paramsUrl = self.buildParamsUrl(None, schemaName, where, None, None)

    return self.httpClient.patch(f"{requestUrl}?{paramsUrl}", data)

  def deleteDocument(self, entityName, documentId):
    requestUrl = f"{self.baseCommerceStableUrl}{entityName}/documents/{documentId}"

    return self.httpClient.delete(requestUrl)

  def searchDocuments(self, entityName, schemaName, fields, where, sort, page, pageSize):
    requestUrl = f"{self.baseCommerceStableUrl}{entityName}/search"
    paramsUrl = self.buildParamsUrl(fields, schemaName, where, sort, None)

    # Need to implement rest range
    return self.httpClient.getWithHeaders(f"{requestUrl}?{paramsUrl}", {"REST-Range": f"resources={((page - 1) * pageSize)}-{(page * pageSize) - 1}"})

  # def getContentPage(self):
  #   url = f"{self.baseCommerceStableUrl}vtex_admin_cms_graphql_content/scroll?_sort=lastInteractionIn ASC&_size=100&_fields=id,name,createdIn,updatedIn,lastInteractionIn,deletedIn,builderId,type,status&_schema=0.20.1&_where=(deletedIn is null) AND (builderId=faststore)"
  #   return self.httpClient.get(url)

  # def getPageContent(self, pageId):
  #   url = f"{self.baseApiUrl}vtex_admin_cms_graphql_contentVariant/search?_sort=lastInteractionIn ASC&_fields=status,blocks,extraBlocks&_schema=0.20.1&_where=parentId={pageId} AND deletedIn is null"
  #   return self.httpClient.get(url)

  # def createContentPage(self, data):
  #   url = f"{self.baseApiUrl}vtex_admin_cms_graphql_content/documents?_schema=0.20.1"
  #   return self.httpClient.post(url, data)

  # def createPageContent(self, data):
  #   url = f"{self.baseApiUrl}vtex_admin_cms_graphql_contentVariant/documents?_schema=0.20.1"
  #   return self.httpClient.post(url, data)

class VtexConnector(Enum):
  CONNECTOR = VtexMasterDataClient("eypartnerus", "vtexappkey-eypartnerus-LSNCFT", "YEKRBNEBCBIEISMWPWKYGCYWFZBSOOUUOVTDNMDJMOMWIBMOUWFTTRPNJYHWKOSIOSQHCEDBCEJYVKAMXXUMUANMONJVMPQGQXGSOYOQFRSSYNBXNDYBTEZPWTUKJEAJ")
  
def delete_existing_records():
    entityNam = "kohlerOrders"
    schema = "kohler-orders-schema-v1"
    fields = ["id", "sapOrderNumber", "poNumber"]
    where = ''
    sort = ''
    pageSize = 100
    
    all_records = []
    current_page = 1
    total_records = None
    
    while True:
        # Get response with headers
        response = VtexConnector.CONNECTOR.value.httpClient.getWithHeaders(
            f"{VtexConnector.CONNECTOR.value.baseCommerceStableUrl}{entityNam}/search?{VtexConnector.CONNECTOR.value.buildParamsUrl(fields, schema, where, sort, None)}",
            {"REST-Range": f"resources={((current_page - 1) * pageSize)}-{(current_page * pageSize) - 1}"}
        )
        
        try:
            response.raise_for_status()
            if response.text:
                records = json.loads(response.text)
                if isinstance(records, list):
                    all_records.extend(records)
                else:
                    all_records.append(records)
            
            # Extract total from REST-Content-Range header
            content_range = response.headers.get('REST-Content-Range', '')
            if content_range:
                # Format: "resources 0-4/4" where 4 is total
                parts = content_range.split('/')
                if len(parts) == 2:
                    total_records = int(parts[1])
                    print(f"Page {current_page}: Fetched records, Total: {total_records}")
                    # Check if we've fetched all records
                    if (current_page * pageSize) >= total_records:
                        break
                current_page += 1
            else:
                break
        except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError) as e:
            print(f"Error fetching records: {e}")
            break
    
    # Collect records to dictionary
    search_data = {
        'httpStatus': 200,
        'data': all_records,
        'total': total_records
    }
    
    print(f"Fetched Records for Deletion: Total: {search_data.get('total')}, Records: {len(search_data.get('data', []))}")
    
    for data in search_data.get('data', []):
        deleteRes = VtexConnector.CONNECTOR.value.deleteDocument(entityNam, data['id'])
        print(f" -- Deleted record with id: {data['id']}")

def create_sample_records_and_save_locally():
    """
    Create 1,000,000 sample records with realistic data for testing.
    2000 soldToIds with 500 records each.
    Saves to JSON files with 20,000 records each.
    """
    
    # Configuration
    NUM_SOLD_TO = 2000
    RECORDS_PER_SOLD_TO = 500
    TOTAL_RECORDS = NUM_SOLD_TO * RECORDS_PER_SOLD_TO  # 1,000,000
    RECORDS_PER_FILE = 100000
    NUM_USERS = 10000
    
    # Pools
    brands = ['Kohler', 'Moen', 'Delta', 'American Standard', 'Grohe', 'Pfister', 'Hansgrohe', 'Toto', 'Brizo', 'Rohl']
    
    streets = ['Main St', 'Oak Ave', 'Maple Dr', 'Pine Rd', 'Elm St', 'Cedar Ln', 'Birch Way', 'Willow Dr', 'Ash Ct', 'Spruce Rd']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 
              'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA', 'TX', 'FL', 'TX', 'OH', 'NC']
    
    # Create pools
    print("Creating data pools...")
    sold_to_pool = [f"SOLD-TO-{i:05d}" for i in range(1, NUM_SOLD_TO + 1)]
    user_pool = [f"USR-{i:05d}" for i in range(1, NUM_USERS + 1)]
    sku_pool = [f"SKU-{i:05d}" for i in range(1, 5001)]
    
    # Counters for unique values
    sap_counter = 1
    po_counter = 1
    
    print(f"Created pools: {len(sold_to_pool)} sold-to IDs, {len(user_pool)} user IDs, {len(sku_pool)} SKUs")
    print(f"Starting to generate {TOTAL_RECORDS:,} records...")
    
    # Generate records and save to files
    records_buffer = []
    file_counter = 1
    record_counter = 0
    
    # Create data-import directory if it doesn't exist
    output_dir = "sample_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Calculate date range (last 2 years)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    date_range = (end_date - start_date).days
    
    # Generate 500 records for each soldToId to ensure distribution
    for sold_to_idx, sold_to_id in enumerate(sold_to_pool):
        for record_in_batch in range(RECORDS_PER_SOLD_TO):
            # Select random values from pools
            user_id = random.choice(user_pool)
            brand = random.choice(brands)
            
            # Generate unique SAP order number
            sap_order_number = f"SAP-{sap_counter:08d}"
            sap_counter += 1
            
            # Generate unique PO number
            po_number = f"PO-{po_counter:08d}"
            po_counter += 1
            
            # Generate SKU numbers (1-20 unique SKUs from pool)
            sku_count = random.randint(1, 20)
            selected_skus = random.sample(sku_pool, min(sku_count, len(sku_pool)))
            sku_number = '|'.join(selected_skus)
            
            # Generate ship-to address
            street_num = random.randint(1, 9999)
            street = random.choice(streets)
            city_idx = random.randint(0, len(cities) - 1)
            city = cities[city_idx]
            state = states[city_idx]
            zip_code = random.randint(10000, 99999)
            ship_to_address = f"{street_num} {street}, {city}, {state} {zip_code}"
            
            # Generate created date (within last 2 years)
            random_days = random.randint(0, date_range)
            created_date = (start_date + timedelta(days=random_days)).strftime('%Y-%m-%d')
            
            # Create record
            record = {
                "userId": user_id,
                "soldToId": sold_to_id,
                "sapOrderNumber": sap_order_number,
                "shipToAddress": ship_to_address,
                "poNumber": po_number,
                "skuNumber": sku_number,
                "brand": brand,
                "createdDate": created_date
            }
            
            records_buffer.append(record)
            record_counter += 1
            
            # Save to file when buffer reaches RECORDS_PER_FILE
            if len(records_buffer) >= RECORDS_PER_FILE:
                filename = f"{output_dir}/sample_records_{file_counter:03d}.json"
                with open(filename, 'w') as f:
                    json.dump(records_buffer, f, indent=2)
                print(f"Saved {len(records_buffer):,} records to {filename} (Total: {record_counter:,})")
                records_buffer = []
                file_counter += 1
            
            # Progress indicator
            if record_counter % 50000 == 0:
                print(f"Generated {record_counter:,} records (Processing soldToId {sold_to_idx + 1}/{NUM_SOLD_TO})...")
    
    # Save remaining records
    if records_buffer:
        filename = f"{output_dir}/sample_records_{file_counter:03d}.json"
        with open(filename, 'w') as f:
            json.dump(records_buffer, f, indent=2)
        print(f"Saved {len(records_buffer):,} records to {filename} (Total: {record_counter:,})")
    
    print(f"\nData generation complete!")
    print(f"Total records created: {record_counter:,}")
    print(f"Total files created: {file_counter}")
    print(f"Output directory: {os.path.abspath(output_dir)}")
    
def import_records_to_maserdata():
    """
    Import records from sample_records_00001.json to masterdata.
    Processes records in batches of 1000 with 2s delay between batches.
    """
    entityNam = "kohlerOrders"
    schema = "kohler-orders-schema-v1"
    
    # Read the sample records file
    sample_file = "sample_data/sample_records_001.json"
    
    if not os.path.exists(sample_file):
        print(f"Error: File {sample_file} not found!")
        return
    
    print(f"Reading records from {sample_file}...")
    with open(sample_file, 'r') as f:
        records = json.load(f)
    
    print(f"Loaded {len(records):,} records")
    
    # Import records in batches of 1000
    batch_size = 1000
    batch_delay = 10  # seconds
    total_imported = 0
    failed_imports = 0
    
    for batch_num in range(0, len(records), batch_size):
        batch_records = records[batch_num:batch_num + batch_size]
        batch_num_display = (batch_num // batch_size) + 1
        
        print(f"\nProcessing batch {batch_num_display}: {len(batch_records)} records...")
        
        for idx, record in enumerate(batch_records):
            try:
                # Import record to masterdata
                result = VtexConnector.CONNECTOR.value.createDocument(
                    entityNam, 
                    schema, 
                    record
                )
                
                if result.get('httpStatus') == 200:
                    total_imported += 1
                    # if (idx + 1) % 100 == 0:
                    #     print(f"  ✓ Imported {idx + 1}/{len(batch_records)} records in this batch")
                else:
                    failed_imports += 1
                    print(f"  ✗ Failed to import record {idx + 1}: {result.get('message', 'Unknown error')}")
            except Exception as e:
                failed_imports += 1
                print(f"  ✗ Error importing record {idx + 1}: {str(e)}")
        
        print(f"Batch {batch_num_display} completed: {len(batch_records)} records imported")
        
        # Add delay between batches (except after the last batch)
        if batch_num + batch_size < len(records):
            print(f"Waiting {batch_delay}s before next batch...")
            time.sleep(batch_delay)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Import Summary:")
    print(f"  Total records imported: {total_imported:,}")
    print(f"  Failed imports: {failed_imports:,}")
    print(f"  Total records: {len(records):,}")
    print(f"{'='*50}")

if __name__ == "__main__":
    # delete_existing_records()
    # create_sample_records_and_save_locally()
    
    import_records_to_maserdata()
    