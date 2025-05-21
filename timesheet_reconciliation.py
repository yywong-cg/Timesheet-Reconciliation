#!/usr/bin/env python3

import os
import pandas as pd
from datetime import datetime, timedelta
import logging
import numpy as np

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define file paths
HSBC_FILE = "/Users/eunicewong/Desktop/Input/IN_CombinedCSV.xlsx"  # File 1
MAPPING_FILE = "/Users/eunicewong/Desktop/Input/GRI-2-May-2025.xlsb"  # File 2
CG_FILE = "/Users/eunicewong/Desktop/Input/Project Time Actuals Report - DAILY 2025-05-02.xlsx"  # File 3
OUTPUT_DIR = "output"  # Changed to local output directory

class TimesheetReconciliation:
    def __init__(self, hsbc_file, mapping_file, cg_file, output_dir='output'):
        self.hsbc_file = hsbc_file
        self.mapping_file = mapping_file
        self.cg_file = cg_file
        self.output_dir = output_dir
        
    def read_excel_file(self, file_path):
        """Read Excel file and return DataFrame"""
        try:
            if file_path.endswith('.xlsb'):
                # For xlsb files, read specific sheets
                return pd.read_excel(file_path, sheet_name=['Offshore Active', 'Offshore Inactive'], engine='pyxlsb')
            else:
                return pd.read_excel(file_path)
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    def process_timesheet(self, hsbc_df, mapping_df, cg_df):
        """Process timesheet data"""
        try:
            # Log initial row count
            logger.info(f"Initial HSBC data rows: {len(hsbc_df)}")
            
            # Step 1: Filter HSBC data
            hsbc_filtered = hsbc_df[
                (hsbc_df['PROJECT_PRODUCTIVE_FLAG'] == 'Yes') &
                (hsbc_df['TSSTATUS'].isin(['Approved', 'Posted']))
            ].copy()
            
            # Log filtered rows
            logger.info(f"Rows after filtering: {len(hsbc_filtered)}")

            # Step 2: Combine mapping data from both sheets
            mapping_combined = pd.concat([
                mapping_df['Offshore Active'],
                mapping_df['Offshore Inactive']
            ], ignore_index=True)
            
            # Remove duplicates from mapping data
            mapping_combined = mapping_combined.drop_duplicates(subset=['PS ID'])
            logger.info(f"Unique PS IDs in mapping data: {len(mapping_combined)}")

            # Step 3: Merge HSBC data with mapping data
            merged_data = pd.merge(
                hsbc_filtered,
                mapping_combined[['PS ID', 'CG Email Id', 'P&L Owner new']],
                left_on='RESOURCEID',
                right_on='PS ID',
                how='left'
            )
            
            # Check for duplicates after merge
            if len(merged_data) != len(hsbc_filtered):
                logger.warning(f"Merge created duplicates! Before: {len(hsbc_filtered)}, After: {len(merged_data)}")
                # Remove duplicates if any
                merged_data = merged_data.drop_duplicates()
                logger.info(f"Rows after removing duplicates: {len(merged_data)}")

            # Step 4: Process CG data
            # Convert Entry Date to datetime if it's not already
            cg_df['Entry Date'] = pd.to_datetime(cg_df['Entry Date'])
            
            # Create a list to store results
            results = []

            # Process each row in merged data
            for _, row in merged_data.iterrows():
                # Calculate date range for CG hours
                timeperiod = pd.to_datetime(row['TIMEPERIOD'])
                end_date = timeperiod + timedelta(days=5)
                
                # Filter CG data for the date range
                cg_hours = cg_df[
                    (cg_df['User Email'] == row['CG Email Id']) &
                    (cg_df['Entry Date'] >= timeperiod) &
                    (cg_df['Entry Date'] <= end_date)
                ]['Actual Billable Hours (Selected Dates)'].sum()

                # Create result row
                result_row = {
                    'Name': row['RESOURCE_NAME'],
                    'HSBC Staff ID': row['RESOURCEID'],
                    'CG Email': row['CG Email Id'],
                    'P&L Owner': row['P&L Owner new'],
                    'Timesheet Period': row['TIMEPERIOD'],
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

    def generate_report(self, processed_data):
        """Generate reconciliation report"""
        try:
            # Create output filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(self.output_dir, f'Timesheet_Reconciliation_{timestamp}.xlsx')
            
            # Create Excel writer
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Write to worksheet
                processed_data.to_excel(
                    writer,
                    sheet_name='HSBC_CG TS Recon',
                    index=False
                )
                
                # Auto-adjust column widths
                worksheet = writer.sheets['HSBC_CG TS Recon']
                for idx, col in enumerate(processed_data.columns):
                    max_length = max(
                        processed_data[col].astype(str).apply(len).max(),
                        len(col)
                    )
                    worksheet.column_dimensions[chr(65 + idx)].width = max_length + 2

            logger.info(f"Report generated successfully: {output_file}")
            return output_file

        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise

    def run(self):
        """Main execution method"""
        try:
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Read all files
            logger.info("Reading input files...")
            hsbc_df = self.read_excel_file(self.hsbc_file)
            mapping_df = self.read_excel_file(self.mapping_file)
            cg_df = self.read_excel_file(self.cg_file)

            # Process data
            logger.info("Processing timesheet data...")
            processed_data = self.process_timesheet(hsbc_df, mapping_df, cg_df)

            # Generate report
            logger.info("Generating report...")
            output_file = self.generate_report(processed_data)
            
            logger.info(f"Reconciliation completed successfully. Report saved to: {output_file}")
                    
        except Exception as e:
            logger.error(f"Error in main execution: {str(e)}")
            raise

if __name__ == "__main__":
    reconciliation = TimesheetReconciliation(
        hsbc_file=HSBC_FILE,
        mapping_file=MAPPING_FILE,
        cg_file=CG_FILE,
        output_dir=OUTPUT_DIR
    )
    reconciliation.run() 