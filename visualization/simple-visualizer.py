import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from azure.storage.blob import BlobServiceClient
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_blob_storage(connection_string):
    """Connect to Azure Blob Storage"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        return blob_service_client
    except Exception as e:
        logging.error(f"Error connecting to Blob Storage: {e}")
        return None

def get_all_json_files(blob_service_client, container_name):
    """Get all JSON files from the container"""
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs()
        
        json_files = []
        for blob in blobs:
            if blob.name.endswith('.json'):
                json_files.append(blob)
                
        logging.info(f"Found {len(json_files)} JSON files in container '{container_name}'")
        return json_files
    except Exception as e:
        logging.error(f"Error listing blobs: {e}")
        return []

def process_json_files(blob_service_client, container_name, json_files, max_files=None):
    """Process JSON files and return a DataFrame"""
    if max_files:
        json_files = json_files[:max_files]
        
    all_records = []
    container_client = blob_service_client.get_container_client(container_name)
    
    for blob in json_files:
        try:
            blob_client = container_client.get_blob_client(blob.name)
            download_stream = blob_client.download_blob()
            content = download_stream.readall()
            content_str = content.decode('utf-8')
            
            # Process each line in the file
            for line in content_str.split('\n'):
                if line.strip():
                    try:
                        record = json.loads(line)
                        all_records.append(record)
                    except json.JSONDecodeError:
                        logging.warning(f"Could not parse JSON line in {blob.name}")
        except Exception as e:
            logging.error(f"Error processing {blob.name}: {e}")
    
    if not all_records:
        logging.error("No valid records found")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(all_records)
    
    # Convert timestamp to datetime
    if 'windowEndTime' in df.columns:
        df['windowEndTime'] = pd.to_datetime(df['windowEndTime'])
    
    return df

def create_visualizations(df, output_folder):
    """Create visualizations and save them"""
    if df is None or df.empty:
        logging.error("No data to visualize")
        return
    
    # Create output folder if it doesn't exist
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    # Set the style
    sns.set_style("whitegrid")
    plt.rcParams.update({'font.size': 12})
    
    # 1. Ice Thickness Over Time
    plt.figure(figsize=(12, 6))
    for location in df['location'].unique():
        location_data = df[df['location'] == location]
        location_data = location_data.sort_values('windowEndTime')
        plt.plot(
            location_data['windowEndTime'], 
            location_data['avgIceThickness'],
            marker='o', 
            linestyle='-', 
            label=location
        )
    
    plt.title('Average Ice Thickness Over Time by Location', fontsize=16)
    plt.xlabel('Date and Time')
    plt.ylabel('Ice Thickness (cm)')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'ice_thickness_over_time.png'), dpi=300)
    plt.close()
    
    # 2. Bar chart of max snow accumulation by location
    plt.figure(figsize=(10, 6))
    max_snow_by_location = df.groupby('location')['maxSnowAccumulation'].max().reset_index()
    
    # Create bar chart without using palette parameter
    ax = plt.bar(max_snow_by_location['location'], max_snow_by_location['maxSnowAccumulation'])
    
    plt.title('Maximum Snow Accumulation by Location', fontsize=16)
    plt.xlabel('Location')
    plt.ylabel('Snow Accumulation (cm)')
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'max_snow_by_location.png'), dpi=300)
    plt.close()
    
    # 3. Temperature vs Ice Thickness Scatter Plot
    plt.figure(figsize=(10, 6))
    for location in df['location'].unique():
        location_data = df[df['location'] == location]
        plt.scatter(
            location_data['avgSurfaceTemperature'], 
            location_data['avgIceThickness'],
            label=location,
            alpha=0.7
        )
    
    plt.title('Relationship Between Surface Temperature and Ice Thickness', fontsize=16)
    plt.xlabel('Surface Temperature (°C)')
    plt.ylabel('Ice Thickness (cm)')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'temperature_vs_thickness.png'), dpi=300)
    plt.close()
    
    # 4. Summary statistics table (as CSV instead of HTML)
    summary = df.groupby('location').agg({
        'avgIceThickness': ['mean', 'min', 'max'],
        'maxSnowAccumulation': ['mean', 'max'],
        'avgSurfaceTemperature': ['mean', 'min', 'max'],
        'avgExternalTemperature': ['mean', 'min', 'max']
    }).round(2)
    
    # Save summary to CSV
    summary.to_csv(os.path.join(output_folder, 'summary_statistics.csv'))
    
    logging.info(f"Visualizations and summary statistics saved to {output_folder}")
    
    return summary

def generate_markdown_results(df, summary, output_folder):
    """Generate markdown for the Results section"""
    if df is None or df.empty:
        return
    
    # Get sample records for display
    sample_records = df.drop_duplicates('location').sort_values('location')[
        ['location', 'windowEndTime', 'avgIceThickness', 'maxSnowAccumulation', 'avgSurfaceTemperature']
    ].head(3)
    
    # Create markdown content
    markdown = f"""## Results

The real-time monitoring system successfully collected and processed sensor data from three key locations along the Rideau Canal Skateway. The system processed a total of {len(df)} aggregated data points over {df['windowEndTime'].nunique()} time windows.

### Aggregated Data Analysis

The Stream Analytics job aggregated data into 5-minute windows, calculating average values for key metrics. Sample aggregated output is shown below:

| Location | Time Window | Avg Ice Thickness (cm) | Max Snow Accumulation (cm) | Avg Surface Temp (°C) |
|----------|-------------|------------------------|----------------------------|------------------------|
"""

    # Add sample data rows
    for _, row in sample_records.iterrows():
        markdown += f"| {row['location']} | {row['windowEndTime']} | {row['avgIceThickness']:.2f} | {row['maxSnowAccumulation']:.1f} | {row['avgSurfaceTemperature']:.1f} |\n"
    
    # Add key findings
    markdown += """
### Key Findings

1. **Ice Thickness Patterns:**
"""
    # Get location with max ice thickness
    max_thickness_loc = df.groupby('location')['avgIceThickness'].mean().idxmax()
    max_thickness = df.groupby('location')['avgIceThickness'].mean().max()
    min_thickness_loc = df.groupby('location')['avgIceThickness'].mean().idxmin()
    min_thickness = df.groupby('location')['avgIceThickness'].mean().min()
    
    thickness_diff = ((max_thickness - min_thickness) / min_thickness * 100).round(1)
    
    markdown += f"   - {max_thickness_loc} maintained the thickest ice (average of {max_thickness:.1f} cm), approximately {thickness_diff}% thicker than {min_thickness_loc}.\n"
    
    # Find most variable location
    ice_std = df.groupby('location')['avgIceThickness'].std()
    most_variable_loc = ice_std.idxmax()
    markdown += f"   - {most_variable_loc} showed the most variability in ice thickness over time.\n\n"
    
    markdown += "2. **Temperature and Snow Relationships:**\n"
    
    # Snow accumulation info
    max_snow_loc = df.groupby('location')['maxSnowAccumulation'].max().idxmax()
    max_snow = df.groupby('location')['maxSnowAccumulation'].max().max()
    markdown += f"   - Snow accumulation reached a maximum of {max_snow:.1f} cm at {max_snow_loc}, which may require more frequent maintenance.\n"
    
    # Temperature relation
    coldest_loc = df.groupby('location')['avgSurfaceTemperature'].mean().idxmin()
    coldest_temp = df.groupby('location')['avgSurfaceTemperature'].mean().min()
    markdown += f"   - {coldest_loc} had the lowest average surface temperature ({coldest_temp:.1f}°C).\n\n"
    
    markdown += """3. **Temporal Patterns:**
   - Ice thickness measurements showed variations throughout the monitoring period.
   - The data enables NCC officials to make informed decisions about skateway safety.

![Ice Thickness Chart](screenshots/ice_thickness_over_time.png)

The aggregated data enables NCC officials to make informed decisions about skateway safety and maintenance requirements at specific locations, enhancing both safety and visitor experience.
"""

    # Write markdown to file
    with open(os.path.join(output_folder, 'results_section.md'), 'w') as f:
        f.write(markdown)
    
    logging.info(f"Results markdown saved to {output_folder}/results_section.md")
    
    return markdown

def main():
    # Get connection string from command line argument or input
    import sys
    if len(sys.argv) > 1:
        connection_string = sys.argv[1]
    else:
        connection_string = input("Enter your Azure Storage connection string: ")
    
    # Container name
    container_name = "rideau-canal-data"
    
    # Output folder
    output_folder = "results"
    
    # Connect to blob storage
    blob_service_client = connect_to_blob_storage(connection_string)
    if not blob_service_client:
        return
    
    # Get all JSON files
    json_files = get_all_json_files(blob_service_client, container_name)
    if not json_files:
        return
    
    # Process JSON files (limit to 100 files for performance)
    df = process_json_files(blob_service_client, container_name, json_files, max_files=100)
    
    # Create visualizations
    summary = create_visualizations(df, output_folder)
    
    # Generate markdown for Results section
    generate_markdown_results(df, summary, output_folder)
    
    logging.info("Done! Check the output folder for visualizations and summary statistics.")
    
    # Copy files to screenshots folder for README
    screenshots_folder = "screenshots"
    Path(screenshots_folder).mkdir(parents=True, exist_ok=True)
    
    for file in ['ice_thickness_over_time.png', 'max_snow_by_location.png']:
        source = os.path.join(output_folder, file)
        target = os.path.join(screenshots_folder, file)
        if os.path.exists(source):
            import shutil
            shutil.copy2(source, target)
            logging.info(f"Copied {file} to screenshots folder")

if __name__ == "__main__":
    main()
