#TASK1:
import pandas as pd

df = pd.read_csv('census.csv')
df.rename(columns={
    'State name': 'State/UT',
    'District name': 'District',
    'Male_Literate': 'Literate_Male',
    'Female_Literate': 'Literate_Female',
    'Rural_Households': 'Households_Rural',
    'Urban_ Households': 'Households_Urban',
    'Age_Group_0_29': 'Young_and_Adult',
    'Age_Group_30_49': 'Middle_Aged',
    'Age_Group_50': 'Senior_Citizen',
    'Age not stated': 'Age_Not_Stated'
}, inplace=True)
#TASK2:
df.to_csv('upcensus.csv',index=False)
print(df.columns)
name=['Andaman and  Nicobar Islands','Arunachal Pradesh','Bihar']
# Define a function to rename the State/UT names
def rename_state_ut(name):
    words = name.split()  # Split the name into words
    renamed_words = []
    
    for word in words:
        # Special condition for the word 'and'
        if word.lower() == 'and':
            renamed_words.append('and')
        else:
            # Capitalize the first letter of other words, lower the rest
            renamed_words.append(word.capitalize())
    
    # Join the processed words back into a string
    renamed_name = " ".join(renamed_words)
    return renamed_name

# Apply the renaming function to the 'State/UT' column in the DataFrame
# Assuming the column containing State/UT names is named 'State/UT' in the CSV file
df['State/UT'] = df['State/UT'].apply(rename_state_ut)

# Save the updated DataFrame back to a new CSV file
df.to_csv('Census_renamed.csv', index=False)

print("State/UT names have been renamed and saved to 'Census_renamed.csv'")
#TASK3:
# Open the Telangana.txt file for reading and writing
input_file_path = "Telangana.txt"

# Read the content from the file
with open(input_file_path, "r") as file:
    lines = file.readlines()

# Create a list to store updated lines
updated_lines = []

# Iterate over each line and replace state names where applicable
for line in lines:
    # Strip any trailing newline characters
    line = line.strip()

    # Check conditions and replace state/UT names
    if "Andhra Pradesh" in line:
        updated_line = line.replace("Andhra Pradesh", "Telangana")
    elif "Jammu and Kashmir" in line:
        updated_line = line.replace("Jammu and Kashmir", "Ladakh")
    else:
        updated_line = line  # Keep the line unchanged

    # Add the updated line to the list
    updated_lines.append(updated_line)

# Write the updated content back to the file (or a new file if preferred)
with open(input_file_path, "w") as file:
    for line in updated_lines:
        file.write(line + "\n")

print("State/UT names have been updated successfully!")
#TASK4:
import pandas as pd

# Load the data from a CSV file
df = pd.read_csv("Census_renamed.csv")

# Check column names to ensure they are loaded correctly
print("Columns in DataFrame:", df.columns)

# Strip any leading/trailing spaces from column names
df.columns = df.columns.str.strip()

# Function to calculate the percentage of missing data in each column
def calculate_missing_percentage(dataframe):
    return (dataframe.isna().sum() / len(dataframe)) * 100

# Step 1: Calculate the percentage of missing data before filling
missing_before = calculate_missing_percentage(df)
print("Missing Data Percentage (Before Filling):")
print(missing_before)

# Step 2: Fill missing values based on the provided logic
if "Male" in df.columns and "Female" in df.columns:
    df["Population"] = df["Population"].fillna(df["Male"] + df["Female"])

if "Literate_Male" in df.columns and "Literate_Female" in df.columns:
    df["Literate"] = df["Literate"].fillna(df["Literate_Male"] + df["Literate_Female"])

if all(col in df.columns for col in ["Young_and_Adult", "Middle_Aged", "Senior_Citizen", "Age_Not_Stated"]):
    df["Population"] = df["Population"].fillna(
        df["Young_and_Adult"] + df["Middle_Aged"] + df["Senior_Citizen"] + df["Age_Not_Stated"]
    )

if "Households_Rural" in df.columns and "Households_Urban" in df.columns:
    df["Households"] = df["Households"].fillna(df["Households_Rural"] + df["Households_Urban"])

# Step 3: Calculate the percentage of missing data after filling
missing_after = calculate_missing_percentage(df)
print("\nMissing Data Percentage (After Filling):")
print(missing_after)

# Step 4: Compare and show the difference in missing data percentages
difference = missing_before - missing_after
print("\nDifference in Missing Data Percentage:")
print(difference)

# Optional: Save the cleaned data to a new CSV file
df.to_csv("cleaned_census.csv", index=False)
print("\nData processing complete! Cleaned data saved as 'cleaned_census.csv'.")
#TASK5:
import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus  # Import URL encoding utility

# Replace your username and password here
username = "assassinken2"
password = "reshsat1918"

# Encode the username and password
encoded_username = quote_plus(username)
encoded_password = quote_plus(password)

# Build the MongoDB connection string with encoded credentials
connection_string = f"mongodb+srv://{encoded_username}:{encoded_password}@mydatabase1.g4l8k7x.mongodb.net/"


# Connect to MongoDB Atlas
client = MongoClient(connection_string)

# Step 1: Load the cleaned data from the CSV
df = pd.read_csv("cleaned_census.csv")

# Step 2: Convert the DataFrame to a list of dictionaries
data_records = df.to_dict(orient="records")

# Step 3: Access the database and collection
db = client["census_data"]
collection = db["census"]

# Step 4: Insert the data into the collection
try:
    result = collection.insert_many(data_records)
    print(f"Inserted {len(result.inserted_ids)} records into the 'census' collection!")
except Exception as e:
    print(f"Error inserting data: {e}")

# Step 5: Verify by counting documents
try:
    count = collection.count_documents({})
    print(f"The 'census' collection now has {count} documents.")
except Exception as e:
    print(f"Error counting documents: {e}")
#TASK6:
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text

# Step 1: Connect to MongoDB
mongo_client = MongoClient(f"mongodb+srv://{encoded_username}:{encoded_password}@mydatabase1.g4l8k7x.mongodb.net/")
mongo_db = mongo_client["census_data"]
mongo_collection = mongo_db["census"]

# Step 2: Fetch Data from MongoDB and Load it into a Pandas DataFrame
data = list(mongo_collection.find())
df = pd.DataFrame(data)

# Remove the MongoDB '_id' column (not needed in SQL)
if '_id' in df.columns:
    df.drop('_id', axis=1, inplace=True)

# Step 3: Connect to the Relational Database (SQLite as an example)
engine = create_engine('sqlite:///census_data.db')

# Step 4: Upload Data to the Relational Database
table_name = "census_data"

try:
    # Use 'replace' to avoid duplicate issues during the initial load
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data uploaded successfully to the '{table_name}' table.")
except Exception as e:
    print(f"Error uploading data: {e}")

# Step 5: Verify the Data Upload (Use 'text()' for SQL queries
with engine.connect() as conn:
    query = text(f"SELECT COUNT(*) FROM {table_name}")
    result = conn.execute(query)
    count = result.fetchone()[0]
    print(f"The '{table_name}' table now has {count} records.")


#TASK7:
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Connect to the relational database (SQLite example)
engine = create_engine('sqlite:///census_data.db')

# Streamlit App Title
st.title("Census Data Standardization And Analysis")
# Define Queries in a Dictionary

queries = {
    "Total population of each district": 
        "SELECT District, `State/UT`, Population FROM census_data ORDER BY Population DESC",

    "Literate males and females in each district": 
        "SELECT District, Literate_Male, Literate_Female FROM census_data",

    "Percentage of workers (both male and female) in each district": 
        """
        SELECT District, 
               ROUND((CAST(Male_Workers + Female_Workers AS FLOAT) * 100.0 / Population), 2) AS Worker_Percentage 
        FROM census_data
        """,

    "Households with LPG/PNG in each district": 
        "SELECT District, LPG_or_PNG_Households FROM census_data",

    "Religious composition of each district": 
        """
        SELECT District, Hindus, Muslims, Christians, Sikhs, Buddhists, Jains, 
               Others_Religions, Religion_Not_Stated  
        FROM census_data
        """,

    "Households with internet access in each district": 
        "SELECT District, Households_with_Internet FROM census_data",

    "Educational attainment distribution in each district": 
        """
        SELECT District, Below_Primary_Education, Primary_Education, Middle_Education, 
               Secondary_Education, Higher_Education, Graduate_Education, Other_Education 
        FROM census_data
        """,

    "Households with transportation access in each district": 
        """
        SELECT District, Households_with_Bicycle, Households_with_Car_Jeep_Van, 
               Households_with_Radio_Transistor, Households_with_Scooter_Motorcycle_Moped 
        FROM census_data
        """,

    "Condition of census houses in each district": 
        """
        SELECT District, 
               Condition_of_occupied_census_houses_Dilapidated_Households, 
               Households_with_separate_kitchen_Cooking_inside_house, 
               Having_bathing_facility_Total_Households, 
               Having_latrine_facility_within_the_premises_Total_Households 
        FROM census_data
        """,

    "Household size distribution in each district": 
        """
        SELECT District, 
               Household_size_1_person_Households, 
               Household_size_2_persons_Households, 
               Household_size_3_to_5_persons_Households, 
               Household_size_6_8_persons_Households, 
               Household_size_9_persons_and_above_Households 
        FROM census_data
        """,

    "Total number of households in each state": 
        """
        SELECT `State/UT`, SUM(Households) AS Total_Households 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Households with latrine within premises (statewise)": 
        """
        SELECT `State/UT`, 
               SUM(Having_latrine_facility_within_the_premises_Total_Households) AS Latrine_Within 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Average household size per state": 
        """
        SELECT `State/UT`, 
               ROUND(SUM(Population) * 1.0 / SUM(Households), 2) AS Avg_Household_Size 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Owned vs Rented households (statewise)": 
        """
        SELECT `State/UT`, 
               SUM(Ownership_Owned_Households) AS Owned, 
               SUM(Ownership_Rented_Households) AS Rented 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Latrine facility type distribution (statewise)": 
        """
        SELECT `State/UT`, 
               SUM(Type_of_latrine_facility_Pit_latrine_Households) AS Pit_Latrine, 
               SUM(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households) AS Flush_Latrine 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Households with drinking water near premises (statewise)": 
        """
        SELECT `State/UT`, 
               SUM(Location_of_drinking_water_source_Near_the_premises_Households) AS Near_Premises 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Power parity (income) distribution per state": 
        """
        SELECT `State/UT`, 
               SUM(Power_Parity_Less_than_Rs_45000) AS '<45K', 
               SUM(Power_Parity_Rs_45000_90000) AS '45K-90K', 
               SUM(Power_Parity_Rs_90000_150000) AS '90K-150K', 
               SUM(Power_Parity_Rs_150000_330000) AS '150K-330K', 
               SUM(Power_Parity_Above_Rs_545000) AS '>545K' 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Married couples by household size (statewise)": 
        """
        SELECT `State/UT`, 
               SUM(Married_couples_1_Households) AS Couple1, 
               SUM(Married_couples_2_Households) AS Couple2, 
               SUM(Married_couples_3_Households) AS Couple3 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Households below poverty line (approx - less than Rs 45000)": 
        """
        SELECT `State/UT`, 
               SUM(Power_Parity_Less_than_Rs_45000) AS Below_Poverty 
        FROM census_data 
        GROUP BY `State/UT`
        """,

    "Literacy rate per state": 
        """
        SELECT `State/UT`, 
               ROUND(SUM(Literate) * 100.0 / SUM(Population), 2) AS Literacy_Rate 
        FROM census_data 
        GROUP BY `State/UT`
        """
}

# Dropdown to Select Query
query_name = st.selectbox("Select Query", list(queries.keys()))

# Run Selected Query and Display Results
if st.button("Run Query"):
    query = queries[query_name]
    try:
         df = pd.read_sql_query(query, engine)
         st.dataframe(df)
    except Exception as e:
        st.error(f"Error executing query: {e}")
#To run        
#python -m streamlit run Census_data.py
