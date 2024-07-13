import modin.pandas as pd
import ray
import time
# pip install "modin[all]" # (Recommended) Install Modin with Ray and Dask engines.
# Initialize Ray (required for Modin to work with Ray engine)
ray.shutdown()
ray.init()

def split_url(url):
    url=url.replace('https://','').replace('http://','')
    parts = url.split(' ')
    return parts[1] if len(parts) > 1 else url

def deduplicate_csv(input_file, output_file):
    print("Reading CSV file...")
    df = pd.read_csv(input_file, usecols=['url'])
    
    print("Splitting URLs and creating deduplication key...")
    df['dedup_key'] = df['url'].apply(split_url)
    
    print("Removing duplicates...")
    df_deduplicated = df.drop_duplicates(subset='dedup_key')
    
    # Remove the 'dedup_key' column before saving
    df_deduplicated = df_deduplicated.drop(columns=['dedup_key'])
    
    print("Saving deduplicated data...")
    df_deduplicated.to_csv(output_file, index=False)

    # Get some stats
    total_rows = len(df)
    unique_rows = len(df_deduplicated)
    print(f"Total rows: {total_rows:,}")
    print(f"Unique URLs (based on second part): {unique_rows:,}")
    print(f"Removed {total_rows - unique_rows:,} duplicate rows")

if __name__ == "__main__":
    input_file=f'waybackmachines-myshopify.com.csv'
    import time
    start_time = time.time()

    output_file=input_file.replace('.csv','-modin.csv')
    deduplicate_csv(input_file, output_file)
    print(f"Time taken for asynchronous execution with concurrency limited by semaphore: {time.time() - start_time} seconds")
