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

class TimesheetReconciliation:
    def __init__(self):
        self.input_dir = 'input'
        self.output_dir = 'output'
        
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
            # Step 1: Filter HSBC data
            hsbc_filtered = hsbc_df[
                (hsbc_df['PROJECT_PRODUCTIVE_FLAG'] == 'Yes') &
                (hsbc_df['TSSTATUS'].isin(['Approved', 'Posted']))
            ].copy()

            # Step 2: Combine mapping data from both sheets
            mapping_combined = pd.concat([
                mapping_df['Offshore Active'],
                mapping_df['Offshore Inactive']
            ], ignore_index=True)

            # Step 3: Merge HSBC data with mapping data
            merged_data = pd.merge(
                hsbc_filtered,
                mapping_combined[['PS ID', 'CG Email Id', 'P&L Owner new']],
                left_on='RESOURCEID',
                right_on='PS ID',
                how='left'
            )

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

            return pd.DataFrame(results)

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
            
            # Read input files
            hsbc_file = None
            mapping_file = None
            cg_file = None
            
            for filename in os.listdir(self.input_dir):
                file_path = os.path.join(self.input_dir, filename)
                if filename.endswith('.xlsx'):
                    if 'Project Time Actuals Report' in pd.read_excel(file_path).columns:
                        cg_file = file_path
                    else:
                        hsbc_file = file_path
                elif filename.endswith('.xlsb'):
                    mapping_file = file_path

            if not all([hsbc_file, mapping_file, cg_file]):
                raise ValueError("Missing required input files")

            # Read all files
            logger.info("Reading input files...")
            hsbc_df = self.read_excel_file(hsbc_file)
            mapping_df = self.read_excel_file(mapping_file)
            cg_df = self.read_excel_file(cg_file)

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
    reconciliation = TimesheetReconciliation()
    reconciliation.run() 