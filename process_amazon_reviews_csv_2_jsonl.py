import pandas as pd

def process_amazon_reviews(input_file, output_file):
    """
    Processes an Amazon Product Reviews CSV file to create a Vespa-compatible JSON format.
    
    Args:
        input_file (str): Path to the input CSV file containing Amazon Product Reviews data.
        output_file (str): Path to the output JSON file for Vespa indexing.
    
    Workflow:
        1. Reads the CSV file into a Pandas DataFrame.
        2. Cleans and renames the columns to match Vespa format.
        3. Creates a "text" column that combines `Summary` and `Text`.
        4. Selects relevant columns: `doc_id`, `title`, and `text`.
        5. Outputs a JSON file for Vespa indexing.
    """
    # Read the dataset
    reviews = pd.read_csv(input_file)
    
    # Check column names and print if 'Summary' or 'Text' is missing
    print(f"Columns in dataset: {reviews.columns}")
    
    # Handle missing values for 'Summary' and 'Text' columns
    if 'Summary' in reviews.columns:
        reviews['Summary'] = reviews['Summary'].fillna('')
    else:
        print("Column 'Summary' is missing.")
    
    if 'Text' in reviews.columns:
        reviews['Text'] = reviews['Text'].fillna('')
    else:
        print("Column 'Text' is missing.")
    
    # Combine 'Summary' and 'Text' to create the 'text' column
    reviews["text"] = reviews["Summary"] + " " + reviews["Text"]
    
    # Select relevant columns: 'doc_id', 'title' (Summary), and 'text'
    reviews = reviews[['Id', 'Summary', 'text']]
    reviews.rename(columns={'Summary': 'title', 'Id': 'doc_id'}, inplace=True)
    
    # Create 'fields' column with JSON-like structure of each record
    reviews['fields'] = reviews.apply(lambda row: row.to_dict(), axis=1)
    
    # Create 'put' column based on 'doc_id' (Vespa expects 'put' with doc_id)
    reviews['put'] = reviews['doc_id'].apply(lambda x: f"id:reviews:doc::{x}")
    
    # Prepare final DataFrame for Vespa (put and fields)
    df_result = reviews[['put', 'fields']]
    
    # Save to JSON file
    df_result.to_json(output_file, orient='records', lines=True)
    print(f"Processed data saved to {output_file}")

# Example usage
process_amazon_reviews("amazon_reviews.csv", "amazon_reviews_for_vespa.jsonl")
