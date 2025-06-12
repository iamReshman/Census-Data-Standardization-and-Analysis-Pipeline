# Census-Data-Standardization-and-Analysis-Pipeline
This project is a complete data engineering pipeline that cleans, processes, stores, and analyzes Indian census data. It integrates tools such as **Pandas**, **MongoDB**, **SQLite**, and **Streamlit** to provide a comprehensive workflow from raw CSV data to a user-friendly web interface.

---

## 📁 Project Structure

census_pipeline_project/
│
├── census.csv # Original raw census dataset
├── cleaned_census.csv # Final cleaned dataset after processing
├── Census_data.py # Streamlit application for data analysis
├── Telangana.txt # Text file for state renaming operation
├── README.md # Project documentation

yaml
Copy
Edit

---

## ✅ Features

- Rename inconsistent columns and state/UT names
- Handle state formation (e.g., Telangana from Andhra Pradesh)
- Fill missing values using logical rules
- Store data in MongoDB and then transfer it to a relational database (SQLite)
- Provide a Streamlit dashboard to query and analyze the census data

---

## 🛠️ Technologies Used

- **Python**
- **Pandas**
- **MongoDB Atlas** (`pymongo`)
- **SQLite** (`SQLAlchemy`)
- **Streamlit** (for dashboard)
- **VSCode or any Python IDE**

---

## 🔧 Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/census-pipeline-project.git
cd census-pipeline-project
2. Create Virtual Environment (Optional)
bash
Copy
Edit
python -m venv venv
venv\Scripts\activate  # On Windows
3. Install Dependencies
bash
Copy
Edit
pip install -r requirements.txt
If no requirements.txt, manually install:

bash
Copy
Edit
pip install pandas pymongo sqlalchemy streamlit
4. Set up MongoDB Atlas
Create an account on MongoDB Atlas

Create a cluster and a database named census_data and collection census

Update the username and password in your code:

python
Copy
Edit
username = "your_username"
password = "your_password"
🚀 Run the Pipeline
Step-by-Step:
Run the ETL code (Tasks 1–6 are in a single Python script or Jupyter cells)

Launch the Streamlit App:

bash
Copy
Edit
streamlit run Census_data.py
📌 Streamlit App Features
View total population by district

Analyze literacy by gender

Study worker distribution

View LPG/PNG usage

Analyze religious demographics

Educational attainment summaries

Internet accessibility by district
