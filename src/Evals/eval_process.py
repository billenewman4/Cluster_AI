# Implements the evaluation process for the beef extraction model using LangSmith

from pathlib import Path
import pandas as pd
import sys

# Add src to path for imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from Evals.model_caller.model_caller import call_model_on_list
from langsmith import Client
from Evals.Data_processing.langsmith.langSmithTabels import create_langsmith_dataset

client = Client()

def eval_process(dataset_name: str = "Beef Extraction Evaluation"):
    """Main evaluation process"""
    
    # Check if dataset exists
    # Try to get the dataset by name
    datasets = list(client.list_datasets(dataset_name=dataset_name))
    
    if datasets:
        # Dataset exists, use it
        print(f"Using existing dataset: {dataset_name}")
        dataset = datasets[0]
        # Don't need to create examples again, they're already in the dataset
        examples = None
    else:
        # Dataset doesn't exist, create it
        print(f"Dataset '{dataset_name}' not found, creating new one")
        dataset, examples = create_langsmith_dataset(dataset_name)
        # Add examples to the new dataset
        client.create_examples(dataset_id=dataset.id, examples=examples)
            
    
    # Run the evaluation using the dataset name
    experiment_results = client.evaluate(
        target_function,
        data=dataset_name,  # Use the actual dataset name
        evaluators=[
            subprimal_evaluator,
            grade_evaluator,
            usda_code_evaluator,  # Add USDA code evaluator
        ],
        experiment_prefix="beef-extraction-eval",
        max_concurrency=2,
    )
    
    print(f"Evaluation completed! Results: {experiment_results}")

def target_function(inputs: dict) -> dict:
    """Target function that calls our beef extraction workflow for a single input"""
    # call_model_on_list expects a list, so wrap single input in list
    result_list = call_model_on_list(
        product_descriptions=[inputs["product_description"]], 
        primal=inputs["primal"]
    )
    
    # Extract the first (and only) result
    result = result_list[0]
    
    # Return the full unified output format
    return {
        "subprimal": result["subprimal_pred"],
        "grade": result["grade_pred"],
        "usda_code": result["usda_code_pred"],
        "needs_review": result["needs_review"]
    }

def subprimal_evaluator(inputs: dict, outputs: dict, reference_outputs: dict):
    """Evaluate subprimal extraction accuracy"""
    predicted = outputs.get("subprimal", "")
    expected = reference_outputs.get("subprimal", "")
    
    # Simple exact match for now
    score = 1.0 if predicted == expected else 0.0
    
    return {
        "key": "subprimal_accuracy",
        "score": score,
        "comment": f"Predicted: {predicted}, Expected: {expected}"
    }

def grade_evaluator(inputs: dict, outputs: dict, reference_outputs: dict):
    """Evaluate grade extraction accuracy"""
    predicted = outputs.get("grade", "")
    expected = reference_outputs.get("grade", "")
    
    # Simple exact match for now
    score = 1.0 if predicted == expected else 0.0
    
    return {
        "key": "grade_accuracy",
        "score": score,
        "comment": f"Predicted: {predicted}, Expected: {expected}"
    }

def usda_code_evaluator(inputs: dict, outputs: dict, reference_outputs: dict):
    """Evaluate USDA code extraction accuracy"""
    predicted = outputs.get("usda_code", "")
    expected = reference_outputs.get("usda_code", "")
    
    # Simple exact match for now - USDA codes should be exact
    score = 1.0 if predicted == expected else 0.0
    
    return {
        "key": "usda_code_accuracy",
        "score": score,
        "comment": f"Predicted: {predicted}, Expected: {expected}"
    }


if __name__ == "__main__":
    eval_process()