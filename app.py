import streamlit as st
import pandas as pd
from timesheet_reconciliation import TimesheetReconciliation
import tempfile
import os
from datetime import datetime

st.set_page_config(
    page_title="Timesheet Reconciliation Tool",
    page_icon="📊",
    layout="wide"
)

st.title("Timesheet Reconciliation Tool")

# File upload section
st.header("Upload Files")

# Create three columns for file uploads
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("HSBC Files")
    hsbc_files = st.file_uploader("Upload HSBC Files (Excel/CSV)", type=['xlsx', 'xls', 'csv'], accept_multiple_files=True, key='hsbc')

with col2:
    st.subheader("Mapping File")
    mapping_file = st.file_uploader("Upload Mapping File (XLSB)", type=['xlsb'], key='mapping')

with col3:
    st.subheader("CG Files")
    cg_files = st.file_uploader("Upload CG Files (Excel/CSV)", type=['xlsx', 'xls', 'csv'], accept_multiple_files=True, key='cg')

# Create a temporary directory to store uploaded files
@st.cache_resource
def create_temp_dir():
    return tempfile.mkdtemp()

temp_dir = create_temp_dir()

def save_uploaded_file(uploaded_file, temp_dir):
    if uploaded_file is not None:
        try:
            file_path = os.path.join(temp_dir, uploaded_file.name)
            with open(file_path, 'wb') as f:
                f.write(uploaded_file.getbuffer())
            return file_path
        except Exception as e:
            st.error(f"Error saving {uploaded_file.name}: {str(e)}")
            return None
    return None

def save_uploaded_files(files, temp_dir):
    saved_paths = []
    for file in files:
        path = save_uploaded_file(file, temp_dir)
        if path:
            saved_paths.append(path)
    return saved_paths

# Initialize session state for storing the generated report
if 'report_data' not in st.session_state:
    st.session_state.report_data = None
if 'report_filename' not in st.session_state:
    st.session_state.report_filename = None

# Process button
if st.button("Generate Report", type="primary"):
    if hsbc_files and mapping_file and cg_files:
        with st.spinner("Processing files..."):
            try:
                # Save uploaded files to temporary directory
                hsbc_paths = save_uploaded_files(hsbc_files, temp_dir)
                mapping_path = save_uploaded_file(mapping_file, temp_dir)
                cg_paths = save_uploaded_files(cg_files, temp_dir)

                # Validate that all files were saved successfully
                if not all([hsbc_paths, mapping_path, cg_paths]):
                    st.error("Failed to save one or more files. Please try uploading the files again.")
                    st.stop()

                # Create output directory
                output_dir = os.path.join(temp_dir, "output")
                os.makedirs(output_dir, exist_ok=True)

                # Initialize and run reconciliation
                reconciliation = TimesheetReconciliation(
                    hsbc_files=hsbc_paths,
                    mapping_file=mapping_path,
                    cg_files=cg_paths,
                    output_dir=output_dir
                )
                
                # Run the reconciliation
                excel_data = reconciliation.run()

                if not excel_data:
                    st.error("Failed to generate the report. Please check the input files and try again.")
                    st.stop()

                # Store the report data and filename in session state
                st.session_state.report_data = excel_data
                st.session_state.report_filename = f"Timesheet_Reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

                st.success("Report generated successfully! Click the 'Download Report' button below to download.")

            except Exception as e:
                st.error(f"Error generating report: {str(e)}")
                st.error("Please check that all files are in the correct format and try again.")
    else:
        st.warning("Please upload all required files.")

# Download button (only shown if report is generated)
if st.session_state.report_data is not None:
    st.download_button(
        label="Download Report",
        data=st.session_state.report_data,
        file_name=st.session_state.report_filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Add some helpful information
st.markdown("---")
st.markdown("""
### Instructions
1. Upload one or more HSBC Files (Excel or CSV format)
2. Upload the Mapping File (XLSB format)
3. Upload one or more CG Files (Excel or CSV format)
4. Click 'Generate Report' to process the files
5. Click 'Download Report' to save the report to your computer

### File Requirements
- **HSBC Files**: Excel (.xlsx, .xls) or CSV files containing timesheet entries
- **Mapping File**: XLSB file containing resource mapping information
- **CG Files**: Excel (.xlsx, .xls) or CSV files containing CG timesheet data

### Troubleshooting
If you encounter any errors:
1. Make sure all files are in the correct format
2. Check that the files are not corrupted
3. Try uploading the files again
4. If the error persists, please contact support
""") 