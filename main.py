import json
import time
import asyncio
import httpx
import subprocess
import os

# --- Settings ---
# Enter the API URL here (Updated to duck API)
API_URL = "https://ff-jwt-gen-api.lovable.app/api/public/token" 
# Number of retries
MAX_RETRIES = 2
# Delay between retries (in seconds)
RETRY_DELAY = 60
# NEW SETTING: How many accounts to process at once (Batch Size)
BATCH_SIZE = 100

# --- Token Generation Logic ---

async def generate_single_token(client, uid: str, password: str):
    """Generates a token from the API."""
    try:
        url = f"{API_URL}?uid={uid}&password={password}"
        resp = await client.get(url, timeout=30)
        
        # Return JSON data if the request is successful
        if resp.status_code == 200:
            return resp.json()
        # Return None if unsuccessful
        return None
    except Exception as e:
        # Print error in case of any exception
        print(f"Error for UID {uid}: {e}")
        return None

async def process_account_with_retry(client, account, index):
    """Processes an account with retry logic."""
    uid = account['uid']
    password = account['password']
    
    # Try up to the number specified in MAX_RETRIES
    for attempt in range(MAX_RETRIES):
        token_data = await generate_single_token(client, uid, password)
        
        # If token is found, return the data
        if token_data and "token" in token_data:
            return {
                "status": "success",
                "account": account,
                "token_data": token_data,
                "index": index
            }
        
        # If this is not the last attempt, wait and try again
        if attempt < MAX_RETRIES - 1:
            print(f"UID #{index + 1} {uid} - Failed. Retrying in {RETRY_DELAY} seconds...")
            await asyncio.sleep(RETRY_DELAY)
            
    # Return failure message if all attempts fail
    return {
        "status": "failed",
        "account": account,
        "index": index
    }

async def main():
    """Main function that runs the entire process."""
    input_file = "accounts.json"
    
    # If accounts.json doesn't exist, create it automatically (to prevent crashes)
    if not os.path.exists(input_file):
        print(f"⚠️ '{input_file}' not found! Creating an empty file...")
        with open(input_file, 'w') as f:
            json.dump([], f)
            
    try:
        with open(input_file) as f:
            accounts = json.load(f)
    except json.JSONDecodeError:
        print(f"Error: '{input_file}' is not a valid JSON or is empty.")
        return

    print(f"🚀 Starting token generation for {len(accounts)} accounts...")
    start_time = time.time()
    
    # Dictionary to store tokens based on region
    result = {'IND': [], 'BR': [], 'BD': []}
    failed_accounts = []

    # Send requests concurrently using httpx.AsyncClient but in BATCHES
    if accounts:
        async with httpx.AsyncClient() as client:
            all_responses = []
            
            # NEW LOGIC: Process accounts in chunks (batches) of 30
            for i in range(0, len(accounts), BATCH_SIZE):
                batch = accounts[i:i + BATCH_SIZE]
                print(f"\n🔄 Processing batch: {i + 1} to {i + len(batch)}...")
                
                # Create tasks for the current batch only
                tasks = [process_account_with_retry(client, acc, i + j) for j, acc in enumerate(batch)]
                
                # Wait for the current batch to complete
                batch_responses = await asyncio.gather(*tasks)
                all_responses.extend(batch_responses)
                
                # Short delay between batches to let the server breathe
                if i + BATCH_SIZE < len(accounts):
                    await asyncio.sleep(0.5)

            # Process all responses after all batches are done
            print("\n✅ All batches processed. Saving data...")
            for res in all_responses:
                if res['status'] == 'success':
                    account = res['account']
                    token_data = res['token_data']
                    
                    # Use 'region' key with 'notiRegion' as a fallback to support both old and new APIs
                    raw_region = token_data.get('region', token_data.get('notiRegion', ''))
                    region_code = (raw_region if raw_region else '').upper()
                    
                    if region_code == 'IND':
                        region = 'IND'
                    elif region_code in {'BR', 'US', 'SAC', 'NA'}:
                        region = 'BR'
                    else:
                        region = 'BD'
                    
                    result[region].append({
                        'uid': account['uid'],
                        'token': token_data['token']
                    })
                    print(f"✅ UID #{res['index'] + 1} {account['uid']} - Token generated ({region})")
                else:
                    # Add failed accounts to the list
                    failed_accounts.append(res['account']['uid'])
                    print(f"❌ UID #{res['index'] + 1} {res['account']['uid']} - Failed to generate token.")

    # Save resulting tokens to files (Files will always be created now)
    for region in ['IND', 'BR', 'BD']:
        tokens = result[region]
        filename = f'token_{region.lower()}.json'
        
        # Creates an empty file [] even if there is no data, to prevent Git crashes
        with open(filename, 'w') as f:
            json.dump(tokens, f, indent=2)
        print(f"💾 {len(tokens)} tokens saved in {filename}.")

    # --- Git Upload / Add Logic ---
    print("\n🚀 Preparing files for GitHub upload (Git Add)...")
    try:
        # This command will forcefully add files to Git, even if they are new
        subprocess.run(["git", "add", "token_ind.json", "token_br.json", "token_bd.json"], check=True)
        print("✅ Files successfully added to Git! Your Action will now upload them easily.")
    except Exception as e:
        print(f"⚠️ Error adding to Git (ignore this if running on a local PC): {e}")

    # --- Print detailed summary ---
    total_time = time.time() - start_time
    print("\n" + "="*40)
    print("✨ Process completed! ✨")
    print(f"⏱️ Total time: {total_time:.2f} seconds")
    print(f" Total accounts: {len(accounts)}")
    print(f"✔️ Successful tokens: {len(accounts) - len(failed_accounts)}")
    print(f"❌ Failed accounts: {len(failed_accounts)}")
    if failed_accounts:
        print(f"   -> Failed UIDs: {', '.join(failed_accounts)}")
    print("="*40)


# --- Run the script ---
if __name__ == "__main__":
    # You will need the `httpx` library to run this script.
    # Install it using the command `pip install httpx`.
    asyncio.run(main())