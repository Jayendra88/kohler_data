import json
import os
from pathlib import Path

def extract_sample_data():
    """
    Read sample data JSON files and extract 3 distinct userId and soldToId from each file.
    Takes samples from beginning, center, and end of each file.
    Returns two arrays of distinct userIds and soldToIds.
    """
    
    sample_data_dir = "sample_data"
    user_ids_set = set()
    sold_to_ids_set = set()
    
    # Find all sample_records_*.json files
    sample_files = sorted([f for f in os.listdir(sample_data_dir) if f.startswith('sample_records_') and f.endswith('.json')])
    
    print(f"Found {len(sample_files)} sample data files\n")
    print("═" * 80)
    
    for file_idx, filename in enumerate(sample_files, 1):
        filepath = os.path.join(sample_data_dir, filename)
        
        print(f"\nFile {file_idx}: {filename}")
        print("-" * 80)
        
        # Read the JSON file
        with open(filepath, 'r') as f:
            records = json.load(f)
        
        print(f"Total records in file: {len(records):,}")
        
        # Extract samples from beginning, center, and end
        indices = []
        
        # Beginning (first record)
        if len(records) > 0:
            indices.append(0)
        
        # Center (middle record)
        if len(records) > 1:
            indices.append(len(records) // 2)
        
        # End (last record)
        if len(records) > 1:
            indices.append(len(records) - 1)
        
        # Extract data from these positions
        file_user_ids = []
        file_sold_to_ids = []
        
        print(f"\nSamples extracted:")
        for position_name, idx in zip(['Beginning', 'Center', 'End'], indices):
            record = records[idx]
            user_id = record.get('userId')
            sold_to_id = record.get('soldToId')
            
            print(f"  {position_name:10} (Index {idx:6}): userId={user_id:12} | soldToId={sold_to_id}")
            
            if user_id:
                user_ids_set.add(user_id)
                file_user_ids.append(user_id)
            if sold_to_id:
                sold_to_ids_set.add(sold_to_id)
                file_sold_to_ids.append(sold_to_id)
    
    print("\n" + "═" * 80)
    print(f"\nSUMMARY:")
    print(f"Total distinct userIds collected: {len(user_ids_set)}")
    print(f"Total distinct soldToIds collected: {len(sold_to_ids_set)}")
    
    # Convert to sorted lists
    user_ids_list = sorted(list(user_ids_set))
    sold_to_ids_list = sorted(list(sold_to_ids_set))
    
    # Print arrays
    print(f"\n{'═' * 80}")
    print(f"\nUSER IDS ARRAY ({len(user_ids_list)} items):")
    print(json.dumps(user_ids_list, indent=2))
    
    print(f"\n{'═' * 80}")
    print(f"\nSOLD TO IDS ARRAY ({len(sold_to_ids_list)} items):")
    print(json.dumps(sold_to_ids_list, indent=2))
    
    # Save to output file
    output_data = {
        "userIds": user_ids_list,
        "soldToIds": sold_to_ids_list,
        "timestamp": str(os.stat(filepath).st_mtime)
    }
    
    output_file = "extracted_sample_data.json"
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n{'═' * 80}")
    print(f"\nResults saved to: {output_file}")
    print(f"{'═' * 80}\n")
    
    return user_ids_list, sold_to_ids_list

if __name__ == "__main__":
    user_ids, sold_to_ids = extract_sample_data()
