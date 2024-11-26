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

def create_table(db_name, columns, column_types, primary_key=None, unique_columns=None):
    """Create a new table with specified column types, optional primary key, and unique columns"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    c = conn.cursor()
    
    # Prepare column definitions
    column_defs = []
    for col, type_ in zip(columns, column_types):
        col_def = f"{col} {type_}"
        
        # Set primary key
        if primary_key and col == primary_key:
            col_def += " PRIMARY KEY"
        
        # Set unique columns
        if unique_columns and col in unique_columns:
            col_def += " UNIQUE"
        
        column_defs.append(col_def)
    
    query = f'''CREATE TABLE IF NOT EXISTS data
               ({', '.join(column_defs)})'''
    
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
    columns = [(row[1], row[2], row[5] == 1) for row in c.fetchall()]  # name, type, is_primary_key
    
    # Check for unique columns
    c.execute("PRAGMA index_list(data)")
    unique_columns = [row[1].replace('data_', '').replace('_unique', '') 
                      for row in c.fetchall() if 'unique' in row[1]]
    
    conn.close()
    return columns, unique_columns

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
    """Update row based on condition with more flexible value setting"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    c = conn.cursor()
    
    try:
        # Update with exact value assignment
        c.execute(f"UPDATE data SET {update_col} = ? WHERE {condition_col} = ?", 
                  (update_val, condition_val))
        updated_count = c.rowcount
        conn.commit()
        return updated_count
    except sqlite3.IntegrityError as e:
        st.error(f"Update failed: {str(e)}")
        return 0
    except sqlite3.OperationalError as e:
        st.error(f"SQL Error: {str(e)}")
        return 0
    finally:
        conn.close()

def bulk_import_data(db_name, df):
    """Bulk import data from DataFrame"""
    conn = sqlite3.connect(f'databases/{db_name}.db')
    try:
        df.to_sql('data', conn, if_exists='append', index=False)
        return True
    except sqlite3.IntegrityError as e:
        st.error(f"Import failed: {str(e)}")
        return False
    finally:
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
            
            # Modified column type and primary key selection
            if columns_input:
                columns = [col.strip() for col in columns_input.split(",")]
                column_types = []
                
                # Create a select box for each column
                st.write("Select type and configure each column:")
                for col in columns:
                    col_type = st.selectbox(
                        f"Type for {col}",
                        options=["TEXT", "INTEGER", "REAL", "BLOB", "DATE"],
                        key=f"type_{col}"  # Unique key for each selectbox
                    )
                    column_types.append(col_type)
                
                # Primary key selection
                primary_key = st.selectbox(
                    "Select Primary Key Column (optional)",
                    ["None"] + columns,
                    index=0
                )
                primary_key = None if primary_key == "None" else primary_key
                
                # Unique columns selection
                unique_columns = st.multiselect(
                    "Select Unique Columns (optional)",
                    columns
                )
            
            if st.button("Create Database"):
                if new_db_name and columns_input and len(columns) > 0:
                    try:
                        conn = init_db()
                        c = conn.cursor()
                        c.execute("INSERT INTO database_list VALUES (?, ?)",
                                (new_db_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        conn.commit()
                        conn.close()
                        
                        # Create table with primary key and unique columns
                        create_table(
                            new_db_name, 
                            columns, 
                            column_types, 
                            primary_key, 
                            unique_columns
                        )
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
            columns, unique_columns = get_columns(selected_db)
            
            # Display current table constraints
            st.sidebar.subheader("Table Constraints")
            columns_info = [col for col, _, _ in columns]
            
            # Highlight primary key and unique columns
            primary_key = next((col for col, _, is_primary in columns if is_primary), None)
            if primary_key:
                st.sidebar.info(f"🔑 Primary Key: {primary_key}")
            
            if unique_columns:
                st.sidebar.warning(f"🚫 Unique Columns: {', '.join(unique_columns)}")
            
            tabs = st.tabs(["📝 Manage Data", "🔄 Update Data", "🗑️ Delete Data", "📥 Import/Export"])

            # Data Management Tab
            with tabs[0]:
                st.subheader("Data Preview")
                st.dataframe(df, use_container_width=True)
                
                st.subheader("Add New Row")
                new_row_data = {}
                for col, type_, is_primary in columns:
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
                        try:
                            placeholders = ','.join(['?' for _ in columns])
                            query = f"INSERT INTO data VALUES ({placeholders})"
                            c.execute(query, list(new_row_data.values()))
                            conn.commit()
                            st.success("✅ Row added!")
                            st.rerun()
                        except sqlite3.IntegrityError as e:
                            st.error(f"❌ Insert failed: {str(e)}")
                        finally:
                            conn.close()

            # Update Data Tab
            with tabs[1]:
                st.subheader("Update Records")
                col1, col2 = st.columns(2)
    
                with col1:
                    condition_col = st.selectbox("Select Column for Condition", 
                                   [col for col, _, _ in columns])
                    condition_val = st.text_input("Enter Value to Match")
    
                with col2:
                    update_col = st.selectbox("Select Column to Update", 
                                [col for col, _, _ in columns])
                    # Different input method based on column type
                    column_types_dict = {col: type_ for col, type_, _ in columns}
                    update_val_input = None
        
                    if column_types_dict[update_col] == 'INTEGER':
                        update_val_input = st.number_input(f"Enter New Value for {update_col}", step=1)
                    elif column_types_dict[update_col] == 'REAL':
                        update_val_input = st.number_input(f"Enter New Value for {update_col}", step=0.1)
                    elif column_types_dict[update_col] == 'DATE':
                        update_val_input = st.date_input(f"Enter New Value for {update_col}")
                    else:
                        update_val_input = st.text_input(f"Enter New Value for {update_col}")
                        
                if st.button("Update Records"):
                    if condition_val is not None and update_val_input is not None:
                        # Convert update_val_input to string to work with SQLite
                        update_val = str(update_val_input)
            
                        updated = update_row(selected_db, condition_col, 
                              condition_val, update_col, update_val)
                        if updated > 0:
                            st.success(f"✅ Updated {updated} records!")
                            st.rerun()
                        else:
                            st.warning("No records were updated. Check your conditions.")

            # Delete Data Tab
            with tabs[2]:
                st.subheader("Delete Records")
                del_col = st.selectbox("Select Column for Deletion Condition", 
                                     [col for col, _, _ in columns])
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
                        success = bulk_import_data(selected_db, import_df)
                        if success:
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
