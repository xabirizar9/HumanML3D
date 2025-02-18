import requests
import time
import os
import tarfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define your login credentials
LOGIN_URL = "https://amass.is.tue.mpg.de/login.php"
USERNAME = "irizar.xabier@gmail.com"
PASSWORD = "aupareal7"

def login():
    """Logs into the AMASS website and stores the session cookies."""
    print("[*] Logging in...")
    session = requests.Session()
    session.cookies.clear()
    
    payload = {
        "username": USERNAME,
        "password": PASSWORD
    }

    response = session.post(LOGIN_URL, data=payload)
    
    if "incorrect" in response.text.lower():
        print("[!] Login failed. Check credentials.")
        return None
    
    if 'PHPSESSID' in session.cookies:
        print(f"[+] Session cookie: PHPSESSID={session.cookies['PHPSESSID']}")
    
    print("[+] Logged in successfully.")
    return session

def process_dataset(session, dataset_name, actual_dirname=None):
    """Downloads and extracts a dataset in one go."""
    actual_dirname = actual_dirname or dataset_name
    workspace_dir = f"/workspace/amass_data/{actual_dirname}"
    filename = f"{workspace_dir}/{dataset_name}.tar.bz2"
    
    # Check if directory exists and is not empty
    if (os.path.exists(workspace_dir) and os.listdir(workspace_dir)) and not os.path.exists(filename):
        print(f"[*] {actual_dirname} already exists and is not empty. Skipping.")
        return dataset_name, True
    
    # Check if tar file exists
    if os.path.exists(filename):
        print(f"[*] Found existing tar file for {dataset_name}. Extracting...")
        try:
            # Create workspace directory if it doesn't exist
            os.makedirs(workspace_dir, exist_ok=True)
            os.system(f"chmod 777 {workspace_dir}")
            
            # Extract the file
            with tarfile.open(filename, "r:bz2") as tar:
                tar.extractall(path=workspace_dir)
                
            # Clean up
            os.remove(filename)
            print(f"[+] Successfully extracted {dataset_name}")
            return dataset_name, True
            
        except Exception as e:
            print(f"[!] Error extracting {dataset_name}: {str(e)}")
            return dataset_name, False
    
    # If no tar file exists, proceed with download attempt
    download_url = f"https://download.is.tue.mpg.de/download.php?domain=amass&resume=1&sfile=amass_per_dataset/smplh/gender_specific/mosh_results/{dataset_name}.tar.bz2"
    
    print(f"[*] Processing {dataset_name} -> {actual_dirname}")
    
    try:
        # Create workspace directory with 
        os.system(f"mkdir -p {workspace_dir}")
        os.system(f"chmod 777 {workspace_dir}")
        
        # Download directly to workspace
        headers = {
            'Accept-Encoding': 'gzip, deflate',
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://amass.is.tue.mpg.de/download.php'
        }
        
        # Download the file
        print(f"[*] Downloading {dataset_name}")
        response = session.get(download_url, stream=True, headers=headers)
        
        if response.headers.get('content-type', '').startswith('text/html'):
            print(f"[!] Download failed for {dataset_name}")
            return dataset_name, False
            
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    
        # Extract the file
        print(f"[*] Extracting {dataset_name}")
        with tarfile.open(filename, "r:bz2") as tar:
            tar.extractall(path=workspace_dir)
            
        # Clean up
        os.remove(filename)
        print(f"[+] Successfully processed {dataset_name}")
        return dataset_name, True
        
    except Exception as e:
        print(f"[!] Error processing {dataset_name}: {str(e)}")
        return dataset_name, False

if __name__ == "__main__":
    # List of all datasets with their actual directory names
    DATASETS = [
        ("ACCAD", "ACCD"),  # Fixed: both URL and directory should be ACCAD
        ("BMLrub", "BioMotionLab_NTroje"),
        ("DFaust", "DFaust_67"),  # DFaust in URL
        ("EKUT", "EKUT"),
        ("EyesJapanDataset", "Eyes_Japan_Dataset"),
        ("HumanEva", "HumanEva"),
        ("BMLhandball", "BMLhandball"),
        ("HDM05", "MPI_HDM05"),  # HDM05 in URL
        ("PosePrior", "MPI_Limits"),  # PosePrior in URL
        ("MoSh", "MPI_mosh"),  # Fixed: MoSh is the correct URL name (not MoSH)
        ("BMLmovi", "BMLmovi"),
        ("SSM", "SSM_synced"),  # SSM in URL
        ("TCDHands", "TCD_handMocap"),  # TCDHands in URL
        ("SFU", "SFU"),
        ("Transitions", "Transitions_mocap"),  # Transitions in URL
        ("TotalCapture", "TotalCapture"),
        ("CMU", "CMU"),
        ("KIT", "KIT")
    ]
    
    # Create base workspace directory
    os.system("mkdir -p /workspace/amass_data")
    os.system("chmod 777 /workspace/amass_data")
    
    session = login()
    if session:
        # Process datasets in parallel
        max_workers = 4
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_dataset = {
                executor.submit(process_dataset, session, url_name, dir_name): url_name 
                for url_name, dir_name in DATASETS
            }
            
            for future in as_completed(future_to_dataset):
                dataset_name, success = future.result()
                results[dataset_name] = success
        
        # Print summary
        print("\nDownload Summary:")
        print("----------------")
        for dataset, success in results.items():
            status = "Success" if success else "Failed"
            print(f"{dataset}: {status}")
