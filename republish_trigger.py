import os
import math
from datetime import datetime,timezone
import pandas as pd
import requests
from msal import ConfidentialClientApplication
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import argparse
import boto3
import csv


# Constants
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"
CSV_FILE_PATH = "path_to_your_csv_file.csv"  # Update with the actual path
RETRY_LIMIT = 3
MAX_WORKERS = 5  # Number of threads for parallel processing

# Configure logging
logging.basicConfig(filename="update_publish_field_by_client_bhr_test.log", level=logging.INFO, format="%(asctime)s - %(message)s")

# Function to fetch client ID from AWS Parameter Store
def get_client_id_from_aws(parameter_name):
    """Fetch client ID from AWS Parameter Store."""
    ssm = boto3.client('ssm')
    try:
        response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return response['Parameter']['Value']
    except Exception as e:
        logging.error(f"Failed to fetch parameter {parameter_name} from AWS Parameter Store: {e}")
        raise

# Function to fetch client secret and tenant ID from AWS Parameter Store
def get_parameter_from_aws(parameter_name):
    """Fetch a parameter from AWS Parameter Store with SSL verification disabled."""
    ssm = boto3.client('ssm', verify=False)  # Disable SSL verification
    try:
        response = ssm.get_parameter(Name=parameter_name, WithDecryption=True)
        return response['Parameter']['Value']
    except Exception as e:
        logging.error(f"Failed to fetch parameter {parameter_name} from AWS Parameter Store: {e}")
        raise

def get_access_token(client_id, client_secret, tenant_id):
    """Authenticate and get an access token using MSAL."""
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception("Failed to acquire access token")

def update_publish_field(access_token, document_id, publish_value):
    """Update the 'publish to bright' field for a document."""
    site_id = get_parameter_from_aws("/MicrosoftGraphApi/Prod/au/site-id")
    list_id = get_parameter_from_aws("/MicrosoftGraphApi/Prod/au/list-id")
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{GRAPH_API_ENDPOINT}/sites/{site_id}/lists/{list_id}/items/{document_id}/fields"
    payload = {"PublishtoBright": str(publish_value)}  # Set to True or False
    for attempt in range(RETRY_LIMIT):
        response = requests.patch(url, json=payload, headers=headers)
        if response.status_code == 200:
            logging.info(f"Updated 'publish to bright' for document ID: {document_id}")
            return True
        else:
            logging.warning(f"Failed to update 'publish to bright' for document ID: {document_id} - Attempt {attempt + 1} - status {response.status_code}")
            time.sleep(2)  # Retry delay
    logging.error(f"Failed to update 'publish to bright' for document ID: {document_id} after {RETRY_LIMIT} attempts")
    return False

# Function to write results to a CSV file
def write_results_to_csv(results, output_file, append=False):
    """Write the results of operations to a CSV file. append=True will append and create header if needed."""
    mode = "a" if append else "w"
    header = ["Document ID", "File Name", "ClientId", "Operation", "Status", "Message", "Timestamp"]
    file_exists = os.path.exists(output_file)
    with open(output_file, mode=mode, newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if not append or not file_exists:
            writer.writerow(header)
        for result in results:
            writer.writerow([
                result.get("document_id", ""),
                result.get("file_name", ""),
                result.get("client_id", ""),
                result.get("operation", ""),
                result.get("status", ""),
                result.get("message", ""),
                result.get("timestamp", "")
            ])

def process_client_folder(access_token, client_folder, documents, publish_value, results, file_info_map):
    """Process all documents in a client folder and collect results."""
    operation = "Unpublish" if publish_value == "false" else "Republish"
    logging.info(f"Processing client folder: {client_folder} for {operation}")
    for doc_id in documents:
        try:
            success = update_publish_field(access_token, doc_id, publish_value)
            status = "Success" if success else "Failed"
            message = "Operation completed" if success else "Operation failed"
        except Exception as e:
            status = "Error"
            message = str(e)
        file_name = file_info_map.get(doc_id, "")
        results.append({
            "document_id": doc_id,
            "file_name": file_name,
            "client_id": client_folder,
            "operation": operation,
            "status": status,
            "message": message
        })

def process_folders_in_parallel(access_token, grouped_documents, publish_value, results, file_info_map):
    """Process client folders in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for client_folder, documents in grouped_documents.items():
            executor.submit(process_client_folder, access_token, client_folder, documents, publish_value, results, file_info_map)
        logging.info("All client folders submitted for processing.")

def batch_client_ids(client_ids, batch_size):
    """Yield successive batches of client ids."""
    client_ids = list(client_ids)
    for i in range(0, len(client_ids), batch_size):
        yield client_ids[i:i + batch_size]

# Add CLI argument parsing
def parse_arguments():
    parser = argparse.ArgumentParser(description="Process documents by client folder.")
    parser.add_argument("--csv", default="test.csv", help="Path to the CSV file.")
    parser.add_argument("--workers", type=int, default=5, help="Number of parallel workers.")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing.")
    parser.add_argument("--batch-index", type=int, help="(1-based) Only process this batch of ClientIds and write results to a batch CSV.")
    parser.add_argument("--batch-range", type=str, help="(e.g., '1-10') Process a range of batches.")
    parser.add_argument("--test", action="store_true", help="Run in test mode (only process EMP555555 in ClientId field).")
    parser.add_argument("--emp", type=str, help="Process only the specified ClientId (e.g., EMP555555). Overrides --test if provided.")
    return parser.parse_args()

def main():
    args = parse_arguments()
    global MAX_WORKERS
    MAX_WORKERS = args.workers

    # Fetch client ID, client secret, and tenant ID from AWS Parameter Store
    CLIENT_ID = get_parameter_from_aws("/MicrosoftGraphApi/Prod/ClientId")
    CLIENT_SECRET = get_parameter_from_aws("/MicrosoftGraphApi/Prod/ClientSecret")
    TENANT_ID = get_parameter_from_aws("/MicrosoftGraphApi/Prod/TenantId")

    # Step 1: Authenticate and get access token
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID)

    print(f"Access token acquired: {access_token[:10]}...")  # Print only the first 10 characters for security

    # Step 2: Read document IDs and client folders from CSV
    df = pd.read_csv(args.csv, dtype=str, keep_default_na=False)
    if args.emp:
        filtered_df = df[df['ClientId'] == args.emp]
    elif args.test:
        filtered_df = df[df['ClientId'] == 'EMP555555']
    else:
        filtered_df = df
    grouped_documents = filtered_df.groupby("ClientId")["id"].apply(list).to_dict()  # Group by ClientId
    file_info_map = dict(zip(filtered_df['id'], filtered_df.get('FileLeafRef', "")))

    # Prepare batching
    all_client_ids = sorted(grouped_documents.keys())  # deterministic order
    batch_size = args.batch_size if args.batch_size and args.batch_size > 0 else len(all_client_ids)
    total_batches = math.ceil(len(all_client_ids) / batch_size) if batch_size > 0 else 1

    master_file = "operation_results_bhr_test.csv"

    # Disallow using both batch-index and batch-range together
    if args.batch_index is not None and args.batch_range:
        logging.error("Cannot use both --batch-index and --batch-range at the same time.")
        raise SystemExit("Cannot use both --batch-index and --batch-range at the same time.")

    # If a single batch index is provided, process only that batch and exit
    if args.batch_index is not None:
        batch_idx = args.batch_index
        if batch_idx < 1 or batch_idx > total_batches:
            logging.error(f"Invalid --batch-index {batch_idx}. Must be between 1 and {total_batches}.")
            raise SystemExit(f"Invalid --batch-index {batch_idx}. Must be between 1 and {total_batches}.")

        logging.info(f"Processing only batch {batch_idx}/{total_batches}")
        print(f"Processing only batch {batch_idx}/{total_batches}")

        start = (batch_idx - 1) * batch_size
        end = start + batch_size
        client_id_batch = all_client_ids[start:end]
        batch_grouped_documents = {cid: grouped_documents[cid] for cid in client_id_batch}

        batch_results = []
        logging.info(f"Processing batch {batch_idx}/{total_batches} with {len(client_id_batch)} ClientIds.")
        process_folders_in_parallel(access_token, batch_grouped_documents, publish_value="false", results=batch_results, file_info_map=file_info_map)
        process_folders_in_parallel(access_token, batch_grouped_documents, publish_value="true", results=batch_results, file_info_map=file_info_map)

        ts = datetime.now(timezone.utc).isoformat()
        for r in batch_results:
            r.setdefault("timestamp", ts)

        per_batch_file = f"operation_results_bhr_batch_{batch_idx}.csv"
        write_results_to_csv(batch_results, per_batch_file, append=False)
        logging.info(f"Wrote per-batch results to {per_batch_file}")

        # append to master so progress is preserved
        write_results_to_csv(batch_results, master_file, append=True)
        logging.info(f"Appended batch results to {master_file}")

        return

    # Process a range of batches if --batch-range is provided
    if args.batch_range:
        try:
            start_batch, end_batch = map(int, args.batch_range.split("-"))
        except ValueError:
            logging.error(f"Invalid --batch-range format: {args.batch_range}. Use 'start-end' (e.g., '1-10').")
            raise SystemExit(f"Invalid --batch-range format: {args.batch_range}. Use 'start-end' (e.g., '1-10').")

        if start_batch < 1 or end_batch > total_batches or start_batch > end_batch:
            logging.error(f"Invalid --batch-range {args.batch_range}. Must be between 1 and {total_batches}.")
            raise SystemExit(f"Invalid --batch-range {args.batch_range}. Must be between 1 and {total_batches}.")

        print(f"Processing batches {start_batch} to {end_batch} out of {total_batches} total batches.")

        for batch_idx in range(start_batch, end_batch + 1):
            start = (batch_idx - 1) * batch_size
            end = start + batch_size
            client_id_batch = all_client_ids[start:end]
            batch_grouped_documents = {cid: grouped_documents[cid] for cid in client_id_batch}

            logging.info(f"Processing batch {batch_idx}/{total_batches} with {len(client_id_batch)} ClientIds.")
            batch_results = []

            process_folders_in_parallel(access_token, batch_grouped_documents, publish_value="false", results=batch_results, file_info_map=file_info_map)
            process_folders_in_parallel(access_token, batch_grouped_documents, publish_value="true", results=batch_results, file_info_map=file_info_map)

            ts = datetime.utcnow().isoformat()
            for r in batch_results:
                r.setdefault("timestamp", ts)

            per_batch_file = f"operation_results_bhr_batch_{batch_idx}.csv"
            write_results_to_csv(batch_results, per_batch_file, append=False)
            logging.info(f"Wrote per-batch results to {per_batch_file}")

            # append to master so progress is preserved
            write_results_to_csv(batch_results, master_file, append=True)
            logging.info(f"Appended batch results to {master_file}")

        return

    # Otherwise process all batches sequentially and append each batch to master CSV
    if batch_size and batch_size > 0:
        for idx, client_id_batch in enumerate(batch_client_ids(all_client_ids, batch_size), start=1):
            batch_grouped_documents = {cid: grouped_documents[cid] for cid in client_id_batch}
            batch_results = []
            logging.info(f"Processing batch {idx}/{total_batches} with {len(client_id_batch)} ClientIds.")
            process_folders_in_parallel(access_token, batch_grouped_documents, publish_value="false", results=batch_results, file_info_map=file_info_map)
            process_folders_in_parallel(access_token, batch_grouped_documents, publish_value="true", results=batch_results, file_info_map=file_info_map)

            ts = datetime.utcnow().isoformat()
            for r in batch_results:
                r.setdefault("timestamp", ts)

            # append this batch to master file
            write_results_to_csv(batch_results, master_file, append=True)
            logging.info(f"Processed batch {idx}/{total_batches} and appended results to {master_file}.")
    else:
        # No batching, process all ClientIds at once and write single file
        results = []
        process_folders_in_parallel(access_token, grouped_documents, publish_value="false", results=results, file_info_map=file_info_map)
        process_folders_in_parallel(access_token, grouped_documents, publish_value="true", results=results, file_info_map=file_info_map)
        ts = datetime.now(timezone.utc).isoformat()
        for r in results:
            r.setdefault("timestamp", ts)
        write_results_to_csv(results, master_file, append=False)
        logging.info(f"Results written to {master_file}")

if __name__ == "__main__":
    main()