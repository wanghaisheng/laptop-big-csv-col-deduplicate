import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import time
def split_url(url):
    url=url.replace('https://','').replace('http://','')

    parts = url.split(' ')
    return parts[1] if len(parts) > 1 else url

def deduplicate_csv(input_file, output_file):
    # Read the CSV file
    df = dd.read_csv(input_file, usecols=['url'])
    df['dedup_key'] = df['url'].apply(split_url, meta=('dedup_key', 'object'))
    
    # Remove duplicates based on the 'url' column
    df_deduplicated = df.drop_duplicates(subset='dedup_key')
    df_deduplicated = df_deduplicated.drop(columns=['dedup_key'])
    
    # Compute and write to a new CSV file
    with ProgressBar():
        df_deduplicated.to_csv(output_file, single_file=True, index=False)

    # Get some stats
    total_rows = len(df)
    unique_rows = len(df_deduplicated)
    print(f"Total rows: {total_rows:,}")
    print(f"Unique URLs: {unique_rows:,}")
    print(f"Removed {total_rows - unique_rows:,} duplicate rows")

if __name__ == "__main__":
    input_file=f'waybackmachines-myshopify.com.csv'
    start_time = time.time()

    output_file=input_file.replace('.csv','-dask.csv')
    deduplicate_csv(input_file, output_file)
    print(f"Time taken for asynchronous execution with concurrency limited by semaphore: {time.time() - start_time} seconds")
