import pandas as pd
from langsmith import Client
from Evals.model_caller.model_caller import DynamicBeefExtractor

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