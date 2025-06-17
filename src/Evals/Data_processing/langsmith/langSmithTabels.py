import pandas as pd
from langsmith import Client
from Evals.model_caller.model_caller import DynamicBeefExtractor
from pathlib import Path
import sys
import os

# Add project root to path for imports
project_root = Path(__file__).resolve().parents[4]  # Go up 4 levels to get to project root
sys.path.append(str(project_root))

extractor = DynamicBeefExtractor()

client = Client()


def load_ground_truth_data():
    """Load the ground truth data"""
    df = pd.read_parquet("src/Evals/data/ground_truth/ground_truth.parquet")
    return df

def create_langsmith_dataset(dataset_name: str = "Beef Extraction Evaluation"):
    """Create LangSmith dataset from ground truth data"""
    # Load the ground truth data
    df = load_ground_truth_data()
    
    #add a column for the primal - use category_description instead of product_description
    df["primal"] = df["category_description"].apply(lambda x: extractor.infer_primal_from_category(x))

    # Create the examples
    examples = []
    for _, row in df.iterrows():
        examples.append({
            "inputs": {"product_description": row["product_description"], "primal": row["primal"]},
            "outputs": {"subprimal": row["subprimal"], "grade": row["grade"]}
        })
    
    # Create the langsmith dataset
    dataset = client.create_dataset(
        dataset_name=dataset_name, 
        description="Ground truth dataset for sub-primal and grade extraction"
    )
    
    return dataset, examples

def upload_reviewed_files_as_datasets():
    """Upload all reviewed Excel files as separate LangSmith datasets"""
    outputs_dir = project_root / "outputs"
    
    # Find all files with "reviewed" in the name
    reviewed_files = list(outputs_dir.glob("*reviewed*.xlsx"))
    
    if not reviewed_files:
        print("No reviewed files found in outputs directory")
        return
    
    print(f"Found {len(reviewed_files)} reviewed files:")
    for file in reviewed_files:
        print(f"  - {file.name}")
    
    datasets_created = []
    
    for file_path in reviewed_files:
        try:
            # Read the Excel file
            df = pd.read_excel(file_path)
            
            # Skip if no data
            if df.empty:
                print(f"Skipping empty file: {file_path.name}")
                continue
            
            # Extract dataset name from filename
            # Convert filenames like "meat_inventory_master_20250616_104047_rib_reviewed.xlsx" 
            # to "test_rib_reviewed"
            filename = file_path.stem
            if "rib_reviewed" in filename:
                dataset_name = "test_rib_reviewed"
            elif "loin_reviewed" in filename:
                dataset_name = "test_loin_reviewed"  
            elif "round_reviewed" in filename:
                dataset_name = "test_round_reviewed"
            elif "Flank_Cat_Reviewed" in filename:
                dataset_name = "test_flank_reviewed"
            elif "Ground_Cat_Reviewed" in filename:
                dataset_name = "test_ground_reviewed"
            elif "variety_reviewed" in filename:
                dataset_name = "test_variety_reviewed"
            elif "other_reviewed" in filename:
                dataset_name = "test_other_reviewed"
            else:
                # Generic fallback
                dataset_name = f"test_{filename.split('_')[-2]}_reviewed"
            
            print(f"\nProcessing {file_path.name} -> {dataset_name}")
            
            # Check required columns
            required_cols = ['product_description', 'subprimal', 'grade']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"  Missing required columns: {missing_cols}")
                print(f"  Available columns: {list(df.columns)}")
                continue
            
            # Add primal column if not present
            if 'primal' not in df.columns:
                if 'category' in df.columns:
                    df['primal'] = df['category'].apply(lambda x: extractor.infer_primal_from_category(str(x)) if pd.notna(x) else None)
                elif 'category_description' in df.columns:
                    df['primal'] = df['category_description'].apply(lambda x: extractor.infer_primal_from_category(str(x)) if pd.notna(x) else None)
                else:
                    # Infer primal from dataset name
                    if "rib" in dataset_name.lower():
                        df['primal'] = 'Rib'
                    elif "loin" in dataset_name.lower():
                        df['primal'] = 'Loin'
                    elif "round" in dataset_name.lower():
                        df['primal'] = 'Round'
                    elif "flank" in dataset_name.lower():
                        df['primal'] = 'Flank'
                    elif "chuck" in dataset_name.lower():
                        df['primal'] = 'Chuck'
                    else:
                        df['primal'] = 'Other'
            
            # Filter out rows with missing essential data
            df_clean = df.dropna(subset=['product_description'])
            
            print(f"  Records: {len(df)} total, {len(df_clean)} with valid descriptions")
            
            if df_clean.empty:
                print(f"  No valid records found, skipping")
                continue
            
            # Create examples for LangSmith
            examples = []
            for _, row in df_clean.iterrows():
                examples.append({
                    "inputs": {
                        "product_description": str(row["product_description"]),
                        "primal": str(row.get("primal", "Unknown"))
                    },
                    "outputs": {
                        "subprimal": str(row.get("subprimal", "")) if pd.notna(row.get("subprimal")) else None,
                        "grade": str(row.get("grade", "")) if pd.notna(row.get("grade")) else None
                    }
                })
            
            # Check if dataset already exists
            try:
                existing_datasets = list(client.list_datasets(dataset_name=dataset_name))
                if existing_datasets:
                    print(f"  Dataset '{dataset_name}' already exists, skipping creation")
                    continue
            except Exception as e:
                print(f"  Error checking existing datasets: {e}")
            
            # Create the dataset
            try:
                dataset = client.create_dataset(
                    dataset_name=dataset_name,
                    description=f"Reviewed test dataset from {file_path.name} with {len(examples)} examples"
                )
                
                # Add examples to the dataset
                client.create_examples(dataset_id=dataset.id, examples=examples)
                
                print(f"  ✅ Created dataset '{dataset_name}' with {len(examples)} examples")
                datasets_created.append(dataset_name)
                
            except Exception as e:
                print(f"  ❌ Error creating dataset '{dataset_name}': {e}")
                continue
                
        except Exception as e:
            print(f"❌ Error processing {file_path.name}: {e}")
            continue
    
    print(f"\n🎉 Successfully created {len(datasets_created)} datasets:")
    for name in datasets_created:
        print(f"  - {name}")
    
    return datasets_created

def upload_combined_reviewed_files_as_single_dataset():
    """Combine all reviewed Excel files into one larger test dataset with sample from all primals"""
    outputs_dir = project_root / "outputs"
    
    # Find all reviewed Excel files
    reviewed_files = list(outputs_dir.glob("*reviewed*.xlsx"))
    
    if not reviewed_files:
        print("No reviewed files found in outputs directory")
        return None
    
    print(f"Found {len(reviewed_files)} reviewed files:")
    for file in reviewed_files:
        print(f"  - {file.name}")
    
    # Combine all data from reviewed files
    all_data = []
    primal_counts = {}
    
    for file_path in reviewed_files:
        try:
            # Read the Excel file
            df = pd.read_excel(file_path)
            
            # Skip if no data
            if df.empty:
                print(f"Skipping empty file: {file_path.name}")
                continue
            
            print(f"\nProcessing {file_path.name}")
            
            # Check required columns
            required_cols = ['product_description']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                print(f"  Missing required columns: {missing_cols}")
                print(f"  Available columns: {list(df.columns)}")
                continue
            
            # Add primal column if not present - infer from filename
            if 'primal' not in df.columns:
                filename = file_path.name.lower()
                if "rib_reviewed" in filename:
                    df['primal'] = 'Rib'
                elif "loin_reviewed" in filename:
                    df['primal'] = 'Loin'
                elif "round_reviewed" in filename:
                    df['primal'] = 'Round'
                elif "flank_reviewed" in filename:
                    df['primal'] = 'Flank'
                elif "chuck_reviewed" in filename:
                    df['primal'] = 'Chuck'
                elif "brisket_reviewed" in filename:
                    df['primal'] = 'Brisket'
                elif "plate_reviewed" in filename:
                    df['primal'] = 'Plate'
                elif "variety_reviewed" in filename:
                    df['primal'] = 'Variety'
                elif "other_reviewed" in filename:
                    df['primal'] = 'Other'
                else:
                    df['primal'] = 'Unknown'
            
            # Filter out rows with missing essential data
            df_clean = df.dropna(subset=['product_description'])
            
            print(f"  Records: {len(df)} total, {len(df_clean)} with valid descriptions")
            
            if df_clean.empty:
                print(f"  No valid records found, skipping")
                continue
            
            # Add this file's data to the combined dataset
            for _, row in df_clean.iterrows():
                primal = str(row.get("primal", "Unknown"))
                
                # Track count per primal
                if primal not in primal_counts:
                    primal_counts[primal] = 0
                
                # Only add if we haven't reached 10 samples for this primal
                if primal_counts[primal] < 10:
                    all_data.append({
                        "product_description": str(row["product_description"]),
                        "primal": primal,
                        "subprimal": str(row.get("subprimal", "")) if pd.notna(row.get("subprimal")) else None,
                        "grade": str(row.get("grade", "")) if pd.notna(row.get("grade")) else None,
                        "source_file": file_path.name
                    })
                    primal_counts[primal] += 1
                
        except Exception as e:
            print(f"❌ Error processing {file_path.name}: {e}")
            continue
    
    if not all_data:
        print("No valid data found across all files")
        return None
    
    print(f"\n📊 Combined dataset statistics:")
    print(f"  Total records: {len(all_data)}")
    for primal, count in primal_counts.items():
        print(f"  {primal}: {count} records")
    
    # Create examples for LangSmith
    examples = []
    for data in all_data:
        examples.append({
            "inputs": {
                "product_description": data["product_description"],
                "primal": data["primal"]
            },
            "outputs": {
                "subprimal": data["subprimal"],
                "grade": data["grade"]
            }
        })
    
    dataset_name = "sample 10 from all primals"
    
    # Check if dataset already exists and delete it
    try:
        existing_datasets = list(client.list_datasets(dataset_name=dataset_name))
        if existing_datasets:
            print(f"  Dataset '{dataset_name}' already exists, deleting and recreating...")
            for dataset in existing_datasets:
                client.delete_dataset(dataset_id=dataset.id)
    except Exception as e:
        print(f"  Error checking/deleting existing datasets: {e}")
    
    # Create the combined dataset
    try:
        dataset = client.create_dataset(
            dataset_name=dataset_name,
            description=f"Combined reviewed test dataset with samples from all primals. Total: {len(examples)} examples from {len(reviewed_files)} source files."
        )
        
        # Add examples to the dataset
        client.create_examples(dataset_id=dataset.id, examples=examples)
        
        print(f"  ✅ Created combined dataset '{dataset_name}' with {len(examples)} examples")
        return dataset_name
        
    except Exception as e:
        print(f"  ❌ Error creating combined dataset '{dataset_name}': {e}")
        return None