#!/usr/bin/env python3

import os
import pandas as pd
from datetime import datetime, timedelta
import logging
import numpy as np
from io import BytesIO

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define input file paths
HSBC_FILE = 'Project Time Actuals Report - DAILY 2025-05-02.xlsx'
MAPPING_FILE = 'GRI-2-May-2025.xlsb'
CG_FILE = 'IN_combinedCSV.xlsx'
OUTPUT_DIR = 'output'

class TimesheetReconciliation:
    def __init__(self, hsbc_files, mapping_file, cg_files, output_dir='output'):
        self.hsbc_files = hsbc_files if isinstance(hsbc_files, list) else [hsbc_files]
        self.mapping_file = mapping_file
        self.cg_files = cg_files if isinstance(cg_files, list) else [cg_files]
        self.output_dir = output_dir
        
    def read_file(self, file_path):
        """Read Excel or CSV file and return DataFrame"""
        try:
            file_extension = os.path.splitext(file_path)[1].lower()
            
            if file_extension == '.xlsb':
                # For xlsb files, read specific sheets
                return pd.read_excel(file_path, sheet_name=['Offshore Active', 'Offshore Inactive'], engine='pyxlsb')
            elif file_extension == '.csv':
                # For CSV files
                return pd.read_csv(file_path)
            else:
                # For other Excel files
                return pd.read_excel(file_path)
                
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    def read_multiple_files(self, file_paths):
        """Read and combine multiple Excel/CSV files"""
        dfs = []
        for file_path in file_paths:
            df = self.read_file(file_path)
            if isinstance(df, dict):  # If it's a mapping file with multiple sheets
                return df
            dfs.append(df)
        return pd.concat(dfs, ignore_index=True)

    def process_timesheet(self, hsbc_df, mapping_df, cg_df):
        """Process timesheet data"""
        try:
            # Step 1: Filter HSBC data by TSSTATUS
            hsbc_filtered = hsbc_df[
                hsbc_df['TSSTATUS'].isin(['Approved', 'Posted','Submitted'])
            ].copy()
            
            # Ensure UNITS_CONSUMED is numeric and fill NaN with 0
            hsbc_filtered['UNITS_CONSUMED'] = pd.to_numeric(hsbc_filtered['UNITS_CONSUMED'], errors='coerce').fillna(0)
            
            # Set UNITS_CONSUMED to 0 for non-productive entries
            hsbc_filtered.loc[hsbc_filtered['PROJECT_PRODUCTIVE_FLAG'] != 'Yes', 'UNITS_CONSUMED'] = 0
            
            # Step 2: Sum UNITS_CONSUMED for same RESOURCEID and TIMEPERIOD and get the latest PRICING_MODEL
            hsbc_grouped = hsbc_filtered.groupby(['RESOURCEID', 'RESOURCE_NAME', 'TIMEPERIOD']).agg({
                'UNITS_CONSUMED': 'sum',
                'PRICING_MODEL': 'last'  # Get the latest PRICING_MODEL for each group
            }).reset_index()
            
            # Step 3: Map CG Email from mapping file
            # Handle different column names in mapping sheets
            active_df = mapping_df['Offshore Active'].copy()
            inactive_df = mapping_df['Offshore Inactive'].copy()
            
            # Rename the P&L Owner column in Inactive sheet to match Active sheet
            inactive_df = inactive_df.rename(columns={'New P&L Owner': 'P&L Owner new'})
            
            # Combine mapping data from both sheets
            mapping_combined = pd.concat([
                active_df,
                inactive_df
            ], ignore_index=True)
            
            # Remove duplicates from mapping data
            mapping_combined = mapping_combined.drop_duplicates(subset=['PS ID'])
            
            # Merge HSBC data with mapping data
            merged_data = pd.merge(
                hsbc_grouped,
                mapping_combined[['PS ID', 'CG Email Id', 'P&L Owner new']],
                left_on='RESOURCEID',
                right_on='PS ID',
                how='left'
            )
            
            # Step 4: Map CG hours for each record
            # Convert Entry Date to datetime if it's not already
            cg_df['Entry Date'] = pd.to_datetime(cg_df['Entry Date'])
            cg_df['User Email'] = cg_df['User Email'].str.lower().str.strip()
            
            results = []
            for _, row in merged_data.iterrows():
                # Calculate date range for CG hours
                timeperiod = pd.to_datetime(row['TIMEPERIOD'])
                # Calculate end date (timeperiod + 6 days) to get 7-day window
                end_date = (timeperiod + timedelta(days=6)).replace(hour=23, minute=59, second=59)
                
                # Get CG Email Id for matching
                cg_email = row['CG Email Id'].lower().strip() if pd.notna(row['CG Email Id']) else None
                
                # Filter CG data for the date range and email
                cg_filtered = cg_df[
                    (cg_df['User Email'] == cg_email) &
                    ((cg_df['Entry Date'] >= timeperiod) & (cg_df['Entry Date'] <= end_date))
                ]
                
                # Calculate CG hours (use 0 if no matching records found)
                cg_hours = cg_filtered['Actual Billable Hours (Selected Dates)'].sum() if not cg_filtered.empty else 0
                
                # Create result row
                result_row = {
                    'Name': row['RESOURCE_NAME'],
                    'HSBC Staff ID': row['RESOURCEID'],
                    'CG Email': row['CG Email Id'],
                    'P&L Owner': row['P&L Owner new'],
                    'Timesheet Period': pd.to_datetime(row['TIMEPERIOD']).strftime('%Y-%m-%d'),
                    'Pricing Model': row['PRICING_MODEL'],
                    'HSBC Hrs': row['UNITS_CONSUMED'],
                    'CG Hrs': cg_hours,
                    'Discrepancy': row['UNITS_CONSUMED'] - cg_hours
                }
                results.append(result_row)

            final_df = pd.DataFrame(results)
            logger.info(f"Final result rows: {len(final_df)}")
            return final_df

        except Exception as e:
            logger.error(f"Error processing timesheet data: {str(e)}")
            raise

    def process_flagged_timesheets(self, hsbc_df, mapping_df):
        """Process flagged timesheet entries"""
        try:
            # Step 1: Filter HSBC data for flagged entries by TSSTATUS
            flagged_entries = hsbc_df[
                hsbc_df['TSSTATUS'].isin(['Open', 'Returned', 'Submitted'])
            ].copy()
            
            # Ensure UNITS_CONSUMED is numeric and fill NaN with 0
            flagged_entries['UNITS_CONSUMED'] = pd.to_numeric(flagged_entries['UNITS_CONSUMED'], errors='coerce').fillna(0)
            
            # Set UNITS_CONSUMED to 0 for non-productive entries
            flagged_entries.loc[flagged_entries['PROJECT_PRODUCTIVE_FLAG'] != 'Yes', 'UNITS_CONSUMED'] = 0

            # Step 2: Handle different column names in mapping sheets
            active_df = mapping_df['Offshore Active'].copy()
            inactive_df = mapping_df['Offshore Inactive'].copy()
            
            # Rename the P&L Owner column in Inactive sheet to match Active sheet
            inactive_df = inactive_df.rename(columns={'New P&L Owner': 'P&L Owner new'})
            
            # Combine mapping data from both sheets
            mapping_combined = pd.concat([
                active_df,
                inactive_df
            ], ignore_index=True)
            
            # Remove duplicates from mapping data
            mapping_combined = mapping_combined.drop_duplicates(subset=['PS ID'])

            # Step 3: Merge HSBC data with mapping data
            merged_data = pd.merge(
                flagged_entries,
                mapping_combined[['PS ID', 'CG Email Id', 'P&L Owner new']],
                left_on='RESOURCEID',
                right_on='PS ID',
                how='left'
            )

            # Create result DataFrame with required columns
            result_df = pd.DataFrame({
                'Name': merged_data['RESOURCE_NAME'],
                'HSBC Staff ID': merged_data['RESOURCEID'],
                'CG Email': merged_data['CG Email Id'],
                'P&L Owner': merged_data['P&L Owner new'],
                'Timesheet Period': merged_data['TIMEPERIOD'],
                'Pricing Model': merged_data['PRICING_MODEL'],
                'HSBC Hrs': merged_data['UNITS_CONSUMED'],
                'Status': merged_data['TSSTATUS']
            })

            return result_df

        except Exception as e:
            logger.error(f"Error processing flagged timesheet entries: {str(e)}")
            raise

    def generate_report(self, processed_data, flagged_data):
        """Generate reconciliation report"""
        try:
            # Create a BytesIO object to store the Excel data
            output = BytesIO()
            
            # Create Excel writer
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Write main reconciliation worksheet
                processed_data.to_excel(
                    writer,
                    sheet_name='HSBC_CG TS Recon',
                    index=False
                )
                
                # Auto-adjust column widths for main worksheet
                worksheet = writer.sheets['HSBC_CG TS Recon']
                for idx, col in enumerate(processed_data.columns):
                    max_length = max(
                        processed_data[col].astype(str).apply(len).max(),
                        len(col)
                    )
                    worksheet.column_dimensions[chr(65 + idx)].width = max_length + 2

                # Write flagged entries worksheet
                flagged_data.to_excel(
                    writer,
                    sheet_name='HSBC Flagged TS Entry',
                    index=False
                )
                
                # Auto-adjust column widths for flagged entries worksheet
                worksheet = writer.sheets['HSBC Flagged TS Entry']
                for idx, col in enumerate(flagged_data.columns):
                    max_length = max(
                        flagged_data[col].astype(str).apply(len).max(),
                        len(col)
                    )
                    worksheet.column_dimensions[chr(65 + idx)].width = max_length + 2

            return output.getvalue()

        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise

    def run(self):
        """Run the reconciliation process"""
        try:
            logger.info("Reading input files...")
            # Read and combine multiple HSBC files
            hsbc_df = self.read_multiple_files(self.hsbc_files)
            
            # Read mapping file
            mapping_df = self.read_file(self.mapping_file)
            
            # Read and combine multiple CG files
            cg_df = self.read_multiple_files(self.cg_files)
            
            logger.info("Processing timesheet data...")
            processed_data = self.process_timesheet(hsbc_df, mapping_df, cg_df)
            
            # Process flagged timesheet entries
            flagged_data = self.process_flagged_timesheets(hsbc_df, mapping_df)
            
            logger.info("Generating report...")
            excel_data = self.generate_report(processed_data, flagged_data)
            
            logger.info("Report generated successfully")
            logger.info("Reconciliation completed successfully")
            
            return excel_data

        except Exception as e:
            logger.error(f"Error in reconciliation process: {str(e)}")
            raise

if __name__ == "__main__":
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    reconciliation = TimesheetReconciliation(
        hsbc_files=HSBC_FILE,
        mapping_file=MAPPING_FILE,
        cg_files=CG_FILE,
        output_dir=OUTPUT_DIR
    )
    reconciliation.run() 