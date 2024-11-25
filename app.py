import streamlit as st
import pandas as pd
import sqlite3
import io
import base64
import os
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="Database Operations Tool",
    page_icon="🗃️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Modern UI styling
st.markdown("""
    <style>
    .main {
        padding: 1rem;
    }
    .stButton>button {
        width: 100%;
        background-color: #4CAF50;
        color: white;
        padding: 0.5rem;
        border-radius: 5px;
        border: none;
        margin: 0.5rem 0;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
    .reportview-container {
        background: #fafafa;
    }
    .css-1d391kg {
        padding: 1rem;
    }
    .stSelectbox {
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)

def init_db():
    """Initialize SQLite database"""
    if not os.path.exists('databases'):
        os.makedirs('databases')
    conn = sqlite3.connect('databases/main.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS database_list
                 (name TEXT PRIMARY KEY, created_date TEXT)''')
    conn.commit()
    return conn

def get_database_names():
    """Get list of databases"""
    conn = init_db()
    c = conn.cursor()
    c.execute("SELECT name FROM database_list")
    databases = [row[0] for row in c.fetchall()]
    conn.close()
    return databases

def create_table(db_name, columns, column_types):
    """Create a new table with specified column types"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    c = conn.cursor()
    columns_with_types = [f"{col} {type_}" for col, type_ in zip(columns, column_types)]
    query = f'''CREATE TABLE IF NOT EXISTS data
               ({', '.join(columns_with_types)})'''
    c.execute(query)
    conn.commit()
    conn.close()

def get_table_data(db_name):
    """Get table data"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    return pd.read_sql_query("SELECT * FROM data", conn)

def get_columns(db_name):
    """Get column names and types"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    c = conn.cursor()
    c.execute("PRAGMA table_info(data)")
    columns = [(row[1], row[2]) for row in c.fetchall()]
    conn.close()
    return columns

def delete_rows(db_name, condition_col, condition_val):
    """Delete rows based on condition"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    c = conn.cursor()
    c.execute(f"DELETE FROM data WHERE {condition_col} = ?", (condition_val,))
    deleted_count = c.rowcount
    conn.commit()
    conn.close()
    return deleted_count

def update_row(db_name, condition_col, condition_val, update_col, update_val):
    """Update row based on condition"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    c = conn.cursor()
    c.execute(f"UPDATE data SET {update_col} = ? WHERE {condition_col} = ?", 
              (update_val, condition_val))
    updated_count = c.rowcount
    conn.commit()
    conn.close()
    return updated_count

def bulk_import_data(db_name, df):
    """Bulk import data from DataFrame"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    df.to_sql('data', conn, if_exists='append', index=False)
    conn.close()

def main():
    st.title("🗃️ Database Operations Tool")
    
    # Sidebar for database operations
    with st.sidebar:
        st.header("Database Operations")
        
        # Create New Database
        with st.expander("Create New Database", expanded=True):
            new_db_name = st.text_input("Database Name")
            columns_input = st.text_input("Column Names (comma-separated)")
            column_types = st.multiselect("Column Types", 
                                        ["TEXT", "INTEGER", "REAL", "BLOB", "DATE"],
                                        ["TEXT"])
            
            if st.button("Create Database"):
                if new_db_name and columns_input and column_types:
                    try:
                        columns = [col.strip() for col in columns_input.split(",")]
                        if len(columns) != len(column_types):
                            st.error("❌ Number of columns and types must match!")
                            return
                        
                        conn = init_db()
                        c = conn.cursor()
                        c.execute("INSERT INTO database_list VALUES (?, ?)",
                                (new_db_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        conn.commit()
                        conn.close()
                        create_table(new_db_name, columns, column_types)
                        st.success("✅ Database created successfully!")
                    except sqlite3.IntegrityError:
                        st.error("❌ Database name already exists!")
                else:
                    st.warning("⚠️ Please fill all fields")

    # Main content area
    databases = get_database_names()
    if databases:
        selected_db = st.selectbox("📁 Select Database", databases)
        
        if selected_db:
            df = get_table_data(selected_db)
            columns = get_columns(selected_db)
            
            tabs = st.tabs(["📝 Manage Data", "🔄 Update Data", "🗑️ Delete Data", "📥 Import/Export"])

            # Data Management Tab
            with tabs[0]:
                st.subheader("Data Preview")
                st.dataframe(df, use_container_width=True)
                
                st.subheader("Add New Row")
                new_row_data = {}
                for col, type_ in columns:
                    if type_ == 'DATE':
                        new_row_data[col] = st.date_input(f"Enter {col}")
                    elif type_ == 'INTEGER':
                        new_row_data[col] = st.number_input(f"Enter {col}", step=1)
                    elif type_ == 'REAL':
                        new_row_data[col] = st.number_input(f"Enter {col}", step=0.1)
                    else:
                        new_row_data[col] = st.text_input(f"Enter {col}")
                
                if st.button("Add Row"):
                    if all(str(val) != "" for val in new_row_data.values()):
                        conn = sqlite3.connect(f'databases/{selected_db}.db')
                        c = conn.cursor()
                        placeholders = ','.join(['?' for _ in columns])
                        query = f"INSERT INTO data VALUES ({placeholders})"
                        c.execute(query, list(new_row_data.values()))
                        conn.commit()
                        conn.close()
                        st.success("✅ Row added!")
                        st.rerun()

            # Update Data Tab
            with tabs[1]:
                st.subheader("Update Records")
                col1, col2 = st.columns(2)
                
                with col1:
                    condition_col = st.selectbox("Select Column for Condition", 
                                               [col for col, _ in columns])
                    condition_val = st.text_input("Enter Value to Match")
                
                with col2:
                    update_col = st.selectbox("Select Column to Update", 
                                            [col for col, _ in columns])
                    update_val = st.text_input("Enter New Value")
                
                if st.button("Update Records"):
                    if condition_val and update_val:
                        updated = update_row(selected_db, condition_col, 
                                          condition_val, update_col, update_val)
                        st.success(f"✅ Updated {updated} records!")
                        st.rerun()

            # Delete Data Tab
            with tabs[2]:
                st.subheader("Delete Records")
                del_col = st.selectbox("Select Column for Deletion Condition", 
                                     [col for col, _ in columns])
                del_val = st.text_input("Enter Value to Delete")
                
                if st.button("Delete Records"):
                    if del_val:
                        deleted = delete_rows(selected_db, del_col, del_val)
                        st.success(f"✅ Deleted {deleted} records!")
                        st.rerun()

            # Import/Export Tab
            with tabs[3]:
                st.subheader("Import Data")
                uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
                if uploaded_file is not None:
                    import_df = pd.read_csv(uploaded_file)
                    if st.button("Import Data"):
                        bulk_import_data(selected_db, import_df)
                        st.success("✅ Data imported successfully!")
                        st.rerun()
                
                st.subheader("Export Data")
                export_format = st.selectbox("Select Format", 
                                           ["CSV", "Excel", "JSON"])
                
                if export_format == "CSV":
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv,
                        file_name=f"{selected_db}.csv",
                        mime="text/csv"
                    )
                elif export_format == "Excel":
                    buffer = io.BytesIO()
                    df.to_excel(buffer, index=False)
                    st.download_button(
                        label="📥 Download Excel",
                        data=buffer.getvalue(),
                        file_name=f"{selected_db}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                else:  # JSON
                    json_str = df.to_json(orient='records')
                    st.download_button(
                        label="📥 Download JSON",
                        data=json_str,
                        file_name=f"{selected_db}.json",
                        mime="application/json"
                    )
    else:
        st.info("👋 Welcome! Start by creating a new database using the sidebar.")

if __name__ == "__main__":
    main()