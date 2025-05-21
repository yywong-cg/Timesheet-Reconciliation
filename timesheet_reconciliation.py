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
HSBC_FILE = "/Users/yuenyingwong/Desktop/Input/IN_CombinedCSV.xlsx"  # File 1
MAPPING_FILE = "/Users/yuenyingwong/Desktop/Input/GRI-2-May-2025.xlsb"  # File 2
CG_FILE = "/Users/yuenyingwong/Desktop/Input/Project Time Actuals Report - DAILY 2025-05-02.xlsx"  # File 3
OUTPUT_DIR = "/Users/yuenyingwong/Desktop/Input/Report" 

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
            # logger.info(f"Initial HSBC data rows: {len(hsbc_df)}")
            
            # Step 1: Filter HSBC data
            hsbc_filtered = hsbc_df[
                (hsbc_df['PROJECT_PRODUCTIVE_FLAG'] == 'Yes') &
                (hsbc_df['TSSTATUS'].isin(['Approved', 'Posted'])) &
                (hsbc_df['UNITS_CONSUMED'] > 0)  # Remove rows with zero hours
            ].copy()
            
            # Log filtered rows
            # logger.info(f"Rows after filtering: {len(hsbc_filtered)}")

            # Step 2: Combine mapping data from both sheets
            mapping_combined = pd.concat([
                mapping_df['Offshore Active'],
                mapping_df['Offshore Inactive']
            ], ignore_index=True)
            
            # Remove duplicates from mapping data
            mapping_combined = mapping_combined.drop_duplicates(subset=['PS ID'])
            # logger.info(f"Unique PS IDs in mapping data: {len(mapping_combined)}")

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
                # logger.info(f"Rows after removing duplicates: {len(merged_data)}")

            # Step 4: Process CG data
            # Convert Entry Date to datetime if it's not already
            cg_df['Entry Date'] = pd.to_datetime(cg_df['Entry Date'])
            cg_df['User Email'] = cg_df['User Email'].str.lower().str.strip()  # Convert emails to lowercase and strip whitespace
            
            # Parse Timesheet Period into start and end dates
            def parse_timesheet_period(period):
                try:
                    start_str, end_str = period.split(' - ')
                    start_date = pd.to_datetime(start_str)
                    end_date = pd.to_datetime(end_str)
                    return start_date, end_date
                except:
                    return None, None
            
            cg_df['Timesheet Start'], cg_df['Timesheet End'] = zip(*cg_df['Timesheet Period'].apply(parse_timesheet_period))
            
            # logger.info(f"Total CG entries: {len(cg_df)}")
            # logger.info(f"Unique CG emails: {cg_df['User Email'].nunique()}")
            # logger.info(f"Date range in CG data: {cg_df['Entry Date'].min()} to {cg_df['Entry Date'].max()}")
            
            # Create a list to store results
            results = []

            # Process each row in merged data
            for _, row in merged_data.iterrows():
                # Calculate date range for CG hours
                timeperiod = pd.to_datetime(row['TIMEPERIOD'])
                # Calculate end date (TIMEPERIOD + 6 days) and set it to end of day
                end_date = (timeperiod + timedelta(days=6)).replace(hour=23, minute=59, second=59)
                
                # Get CG Email Id for matching
                cg_email = row['CG Email Id'].lower().strip() if pd.notna(row['CG Email Id']) else None
                
                # Remove detailed logger.info for each row
                # logger.info(f"\nProcessing {row['RESOURCE_NAME']}")
                # logger.info(f"CG Email: {cg_email}")
                # logger.info(f"Date range calculation:")
                # logger.info(f"  TIMEPERIOD (Start Date): {timeperiod.date()}")
                # logger.info(f"  End Date (TIMEPERIOD + 6 days): {end_date.date()} (inclusive, until 23:59:59)")
                # logger.info(f"  Total days in range: 7 days (including both start and end dates)")
                
                # Filter CG data for the date range and email
                cg_filtered = cg_df[
                    (cg_df['User Email'] == cg_email) &
                    (
                        # Match entries where the timesheet period overlaps with our target period
                        ((cg_df['Timesheet Start'] <= end_date) & (cg_df['Timesheet End'] >= timeperiod)) |
                        # Or match entries where the entry date falls within our target period
                        ((cg_df['Entry Date'] >= timeperiod) & (cg_df['Entry Date'] <= end_date))
                    )
                ]
                
                # Remove matching info logs
                # logger.info(f"Found {len(cg_filtered)} matching CG entries")
                # if len(cg_filtered) > 0:
                #     logger.info("CG entries found:")
                #     for _, cg_row in cg_filtered.iterrows():
                #         logger.info(f"  Date: {cg_row['Entry Date'].date()}, Hours: {cg_row['Actual Billable Hours (Selected Dates)']}")
                
                # Calculate CG hours
                cg_hours = cg_filtered['Actual Billable Hours (Selected Dates)'].sum()
                
                # Remove per-row hours log
                # logger.info(f"HSBC hours: {row['UNITS_CONSUMED']}, CG hours: {cg_hours}")
                if abs(row['UNITS_CONSUMED'] - cg_hours) > 0.01:  # Using small threshold for float comparison
                    logger.warning(f"DISCREPANCY FOUND: {row['UNITS_CONSUMED'] - cg_hours} hours difference")

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