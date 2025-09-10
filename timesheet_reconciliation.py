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
            # Get current date and calculate start month with Monday adjustment
            today = pd.Timestamp.today()
            one_month_back = today - pd.DateOffset(months=1)
            
            # Always go back to Monday of the previous week from the 1-month back date
            start_date = one_month_back - pd.Timedelta(days=one_month_back.weekday() + 7)
                
            start_month = start_date.strftime('%Y-%m')
            current_month = today.strftime('%Y-%m')
            
            # Convert TIMEPERIOD to datetime for comparison
            hsbc_df['TIMEPERIOD'] = pd.to_datetime(hsbc_df['TIMEPERIOD'])
            
            # Step 1: Filter HSBC data by TSSTATUS and timeperiod date
            hsbc_filtered = hsbc_df[
                (hsbc_df['TSSTATUS'].isin(['Approved', 'Posted','Submitted'])) &
                (hsbc_df['TIMEPERIOD'] >= start_date) &
                (hsbc_df['TIMEPERIOD'] <= today)
            ].copy()
            
            logger.info(f"Filtering records between {start_date.strftime('%Y-%m-%d')} and {today.strftime('%Y-%m-%d')}")
            logger.info(f"Found {len(hsbc_filtered)} records in the date range")
            
            # Ensure UNITS_CONSUMED is numeric and fill NaN with 0
            hsbc_filtered['UNITS_CONSUMED'] = pd.to_numeric(hsbc_filtered['UNITS_CONSUMED'], errors='coerce').fillna(0)
            
            # Set UNITS_CONSUMED to 0 for non-productive entries
            hsbc_filtered.loc[hsbc_filtered['PROJECT_PRODUCTIVE_FLAG'] != 'Yes', 'UNITS_CONSUMED'] = 0
            
            # Step 2: Sum UNITS_CONSUMED for same RESOURCEID and TIMEPERIOD and get the latest PRICING_MODEL
            hsbc_grouped = hsbc_filtered.groupby([
                'RESOURCEID', 
                'RESOURCE_NAME', 
                'TIMEPERIOD'
            ]).agg({
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
                mapping_combined[['PS ID', 'CG Email Id', 'P&L Owner new', 'Local Grade']],
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
                    'Local Grade': row['Local Grade'],
                    'Timesheet Period': pd.to_datetime(row['TIMEPERIOD']).strftime('%Y-%m-%d'),
                    'Pricing Model': row['PRICING_MODEL'],
                    'HSBC Hrs': row['UNITS_CONSUMED'],
                    'CG Hrs': cg_hours,
                    'Discrepancy': row['UNITS_CONSUMED'] - cg_hours
                }
                results.append(result_row)

            final_df = pd.DataFrame(results)
            unfiltered_df = final_df.copy()
            
            # Debug: Check what Local Grade values exist
            logger.info(f"Available Local Grade values: {final_df['Local Grade'].unique()}")
            logger.info(f"Total records before filtering: {len(final_df)}")
            
            # Separate Sub-Con and U records (exact match, case-insensitive)
            sub_con_mask = final_df['Local Grade'].str.lower().isin(['sub-con', 'u'])
            sub_con_records = final_df[sub_con_mask].copy()
            other_records = final_df[~sub_con_mask].copy()
            
            # Remove Local Grade column from Sub-Con TS Entry only
            sub_con_records = sub_con_records.drop(columns=['Local Grade'])
            # Remove Local Grade from other_records (HSBC_CG TS Recon)
            other_records = other_records.drop(columns=['Local Grade'])
            unfiltered_df = unfiltered_df.drop(columns=['Local Grade'])
            
            # Apply filters to other records (not Sub-Con/U)
            # Remove rows where Discrepancy is 0
            other_records = other_records[other_records['Discrepancy'] != 0]
            # Remove rows where HSBC Hrs/8 == CG Hrs/9 (with tolerance for float comparison)
            tolerance = 1e-6
            mask = ~((other_records['HSBC Hrs'] / 8 - other_records['CG Hrs'] / 9).abs() < tolerance)
            other_records = other_records[mask]
            
            logger.info(f"Final result rows: {len(other_records)}")
            logger.info(f"Sub-Con/U records: {len(sub_con_records)}")

            # Pivot Sub-Con records to match HSBC ExitDate Recon format (without Exit Date)
            if not sub_con_records.empty:
                # Prepare resource info
                resource_info = sub_con_records[['Name', 'HSBC Staff ID', 'CG Email', 'P&L Owner', 'Pricing Model']].drop_duplicates()
                # Create pivot table
                pivot_df = sub_con_records.pivot_table(
                    index=['Name', 'HSBC Staff ID'],
                    columns='Timesheet Period',
                    values='HSBC Hrs',
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
                # Get unique periods and sort
                unique_periods = sorted(sub_con_records['Timesheet Period'].unique())
                # Merge resource info with pivot data
                sub_con_final = pd.merge(
                    resource_info,
                    pivot_df,
                    on=['Name', 'HSBC Staff ID'],
                    how='outer'
                )
                # Fill 0 for any missing values in timesheet period columns
                period_columns = [col for col in sub_con_final.columns if col not in ['Name', 'HSBC Staff ID', 'CG Email', 'P&L Owner', 'Pricing Model']]
                sub_con_final[period_columns] = sub_con_final[period_columns].fillna(0)
                # Sort by Name
                sub_con_final = sub_con_final.sort_values('Name')
                sub_con_records = sub_con_final

            return other_records, unfiltered_df, sub_con_records

        except Exception as e:
            logger.error(f"Error processing timesheet data: {str(e)}")
            raise

    def process_flagged_timesheets(self, hsbc_df, mapping_df):
        """Process flagged timesheet entries"""
        try:
            # Get current date and calculate start month with Monday adjustment
            today = pd.Timestamp.today()
            one_month_back = today - pd.DateOffset(months=1)
            
            # Always go back to Monday of the previous week from the 1-month back date
            start_date = one_month_back - pd.Timedelta(days=one_month_back.weekday() + 7)
                
            start_month = start_date.strftime('%Y-%m')
            current_month = today.strftime('%Y-%m')
            
            # Convert TIMEPERIOD to datetime for comparison
            hsbc_df['TIMEPERIOD'] = pd.to_datetime(hsbc_df['TIMEPERIOD'])
            
            # Step 1: Filter HSBC data for flagged entries by TSSTATUS and timeperiod date
            flagged_entries = hsbc_df[
                (hsbc_df['TSSTATUS'].isin(['Open', 'Returned', 'Submitted'])) &
                (hsbc_df['TIMEPERIOD'] >= start_date) &
                (hsbc_df['TIMEPERIOD'] <= today)
            ].copy()
            
            logger.info(f"Filtering flagged entries between {start_date.strftime('%Y-%m-%d')} and {today.strftime('%Y-%m-%d')}")
            logger.info(f"Found {len(flagged_entries)} flagged entries in the date range")
            
            # Ensure UNITS_CONSUMED is numeric and fill NaN with 0
            flagged_entries['UNITS_CONSUMED'] = pd.to_numeric(flagged_entries['UNITS_CONSUMED'], errors='coerce').fillna(0)
            
            # Set UNITS_CONSUMED to 0 for non-productive entries
            flagged_entries.loc[flagged_entries['PROJECT_PRODUCTIVE_FLAG'] != 'Yes', 'UNITS_CONSUMED'] = 0

            # Step 2: Sum UNITS_CONSUMED for same RESOURCEID and TIMEPERIOD
            flagged_entries = flagged_entries.groupby([
                'RESOURCEID',
                'RESOURCE_NAME',
                'TIMEPERIOD',
                'TSSTATUS',
                'PRICING_MODEL'
            ]).agg({
                'UNITS_CONSUMED': 'sum'
            }).reset_index()

            # Step 3: Handle different column names in mapping sheets
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

            # Step 4: Merge HSBC data with mapping data
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

            # Remove completely duplicate rows (where all columns have the same values)
            result_df = result_df.drop_duplicates()

            # Log the number of duplicates removed
            logger.info(f"Removed {len(merged_data) - len(result_df)} duplicate entries from flagged timesheets")

            # Separate into two DataFrames based on status
            open_entries = result_df[result_df['Status'] == 'Open'].copy()
            other_entries = result_df[result_df['Status'] != 'Open'].copy()

            logger.info(f"Open entries: {len(open_entries)}")
            logger.info(f"Other flagged entries: {len(other_entries)}")

            return open_entries, other_entries

        except Exception as e:
            logger.error(f"Error processing flagged timesheet entries: {str(e)}")
            raise

    def process_exit_date_recon(self, hsbc_df, mapping_df, processed_data):
        """Process HSBC Exit Date Reconciliation report"""
        try:
            # Step 1: Get resource information from main reconciliation report
            resource_info = processed_data[['Name', 'HSBC Staff ID', 'CG Email', 'P&L Owner', 'Pricing Model']].drop_duplicates()
            
            # Step 2: Get exit dates from mapping file - ensure we get the actual data
            inactive_sheet = mapping_df['Offshore Inactive']
            if hasattr(inactive_sheet, 'values'):
                # If it's a pandas DataFrame, get the values
                inactive_df = pd.DataFrame(inactive_sheet.values, columns=inactive_sheet.columns)
            else:
                # If it's already the values, create DataFrame
                inactive_df = pd.DataFrame(inactive_sheet[1:], columns=inactive_sheet[0])
            
            # Get Exit Date column
            if 'Exit Date' in inactive_df.columns:
                exit_date_col = 'Exit Date'
            elif 'ExitDate' in inactive_df.columns:
                exit_date_col = 'ExitDate'
            else:
                raise ValueError("Exit Date column not found in Inactive sheet")
            
            # Just get PS ID and Exit Date
            exit_dates = inactive_df[['PS ID', exit_date_col]].copy()
            exit_dates['PS ID'] = exit_dates['PS ID'].astype(str)
            
            # Convert Excel date number to datetime
            # Excel dates are number of days since 1899-12-30
            # We need to handle both float and int types
            def convert_excel_date(x):
                if pd.isna(x):
                    return None
                try:
                    # Convert to int first to handle float format
                    excel_date = int(float(x))
                    return pd.Timestamp('1899-12-30') + pd.Timedelta(days=excel_date)
                except:
                    return None
            
            exit_dates['Exit Date'] = exit_dates[exit_date_col].apply(convert_excel_date)
            
            # Calculate 1-month window with Friday adjustment
            today = pd.Timestamp.today()
            one_month_back = today - pd.DateOffset(months=1)
            
            # Always go back to Monday of the previous week from the 1-month back date
            start_date = one_month_back - pd.Timedelta(days=one_month_back.weekday() + 7)
            
            # Step 3: Get onboard dates from HSBC data within the date range
            onboard_dates = hsbc_df[['RESOURCEID', 'CONTRACT_STARTDATE']].copy()
            onboard_dates['RESOURCEID'] = onboard_dates['RESOURCEID'].astype(str)
            
            # Convert CONTRACT_STARTDATE to datetime
            onboard_dates['Onboard Date'] = pd.to_datetime(onboard_dates['CONTRACT_STARTDATE'], errors='coerce')
            
            # Filter onboard dates to only include those within the current month range
            onboard_dates = onboard_dates.dropna(subset=['Onboard Date'])
            onboard_dates = onboard_dates[
                (onboard_dates['Onboard Date'] >= start_date) & 
                (onboard_dates['Onboard Date'] <= today)
            ]
            
            # Remove duplicates and keep the latest onboard date for each resource
            onboard_dates = onboard_dates.groupby('RESOURCEID')['Onboard Date'].max().reset_index()
            
            # Filter for exits within the last 1 month (with Friday adjustment)
            exit_dates = exit_dates[
                (exit_dates['Exit Date'] >= start_date) & 
                (exit_dates['Exit Date'] <= today)
            ].copy()
            
            # Step 3: Create pivot table from main reconciliation report
            pivot_df = processed_data.pivot_table(
                index=['Name', 'HSBC Staff ID'],
                columns='Timesheet Period',
                values='HSBC Hrs',
                aggfunc='sum',
                fill_value=0
            ).reset_index()
            
            # Get unique periods and sort them
            unique_periods = sorted(processed_data['Timesheet Period'].unique())
            
            # Step 4: Merge resource info with pivot data
            result_df = pd.merge(
                resource_info,
                pivot_df,
                on=['Name', 'HSBC Staff ID'],
                how='outer'
            )
            
            # Ensure HSBC Staff ID is string for merge
            result_df['HSBC Staff ID'] = result_df['HSBC Staff ID'].astype(str)
            
            # Step 5: Add exit dates
            result_df = pd.merge(
                result_df,
                exit_dates[['PS ID', 'Exit Date']],
                left_on='HSBC Staff ID',
                right_on='PS ID',
                how='left'
            )
            
            # Step 6: Add onboard dates
            result_df = pd.merge(
                result_df,
                onboard_dates[['RESOURCEID', 'Onboard Date']],
                left_on='HSBC Staff ID',
                right_on='RESOURCEID',
                how='left'
            )
            
            # Fill 0 for any missing values in timesheet period columns
            period_columns = [col for col in result_df.columns if col not in 
                            ['Name', 'HSBC Staff ID', 'CG Email', 'P&L Owner', 'PS ID', 'Exit Date', 'Onboard Date', 'Pricing Model']]
            result_df[period_columns] = result_df[period_columns].fillna(0)
            
            # Create final DataFrame with desired columns
            final_df = pd.DataFrame({
                'Name': result_df['Name'],
                'HSBC Staff ID': result_df['HSBC Staff ID'],
                'CG Email': result_df['CG Email'],
                'P&L Owner': result_df['P&L Owner'],
                'Pricing Model': result_df['Pricing Model'],
                'Onboard Date': result_df['Onboard Date'].apply(
                    lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else ''
                ),
                'Exit Date': result_df['Exit Date'].apply(
                    lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else ''
                )
            })
            
            # Add timesheet period columns
            for period in unique_periods:
                if period in result_df.columns:
                    final_df[period] = result_df[period]
                else:
                    final_df[period] = 0
            
            # Sort by Name
            final_df = final_df.sort_values('Name')

            # Debug: Log the counts before filtering
            logger.info(f"Total rows before date filtering: {len(final_df)}")
            has_onboard_debug = (final_df['Onboard Date'] != '') & (final_df['Onboard Date'].notna())
            has_exit_debug = (final_df['Exit Date'] != '') & (final_df['Exit Date'].notna())
            logger.info(f"Rows with onboard date: {has_onboard_debug.sum()}")
            logger.info(f"Rows with exit date: {has_exit_debug.sum()}")
            logger.info(f"Rows with both dates: {(has_onboard_debug & has_exit_debug).sum()}")
            logger.info(f"Rows with no dates: {((~has_onboard_debug) & (~has_exit_debug)).sum()}")
            
            # Remove rows where resource has no onboard date AND no exit date
            # Keep rows that have either onboard date OR exit date (or both)
            # Handle both empty strings and NaN values
            has_onboard_date = (final_df['Onboard Date'] != '') & (final_df['Onboard Date'].notna())
            has_exit_date = (final_df['Exit Date'] != '') & (final_df['Exit Date'].notna())
            mask = has_onboard_date | has_exit_date
            final_df = final_df[mask]
            
            logger.info(f"Rows after date filtering: {len(final_df)}")
            
            # Also remove rows where all period columns are non-zero and Exit Date is empty (legacy filter)
            period_columns = [col for col in final_df.columns if col not in ['Name', 'HSBC Staff ID', 'CG Email', 'P&L Owner', 'Pricing Model', 'Onboard Date', 'Exit Date']]
            mask2 = ~((final_df[period_columns] != 0).all(axis=1) & (final_df['Exit Date'] == ''))
            final_df = final_df[mask2]
            
            logger.info(f"Final rows after all filtering: {len(final_df)}")

            return final_df

        except Exception as e:
            logger.error(f"Error processing exit date reconciliation: {str(e)}")
            raise

    def generate_report(self, processed_data, open_flagged_data, other_flagged_data, exit_date_data, output_dir, combined_hsbc_df=None, sub_con_data=None):
        """Generate reconciliation report with all worksheets and the combined HSBC data"""
        try:
            # Create file path
            report_path = os.path.join(output_dir, 'HSBC_Timesheet_Reconciliation.xlsx')
            
            # Write all reports to the same Excel file
            with pd.ExcelWriter(report_path, engine='openpyxl', datetime_format='yyyy-mm-dd') as writer:
                # Write main reconciliation worksheet
                processed_data.to_excel(
                    writer,
                    sheet_name='HSBC_CG TS Recon',
                    index=False
                )
                
                # Auto-adjust column widths and format dates for main worksheet
                worksheet = writer.sheets['HSBC_CG TS Recon']
                for idx, col in enumerate(processed_data.columns):
                    max_length = max(
                        processed_data[col].astype(str).apply(len).max(),
                        len(col)
                    )
                    column_letter = chr(65 + idx)
                    worksheet.column_dimensions[column_letter].width = max_length + 2
                    
                    # Format date columns
                    if col == 'Timesheet Period':
                        for cell in worksheet[f'{column_letter}2:{column_letter}{len(processed_data)+1}']:
                            cell[0].number_format = 'yyyy-mm-dd'

                # Write Open flagged entries worksheet
                if not open_flagged_data.empty:
                    open_flagged_data.to_excel(
                        writer,
                        sheet_name='HSBC Open TS Entries',
                        index=False
                    )
                    
                    # Auto-adjust column widths and format dates for Open flagged entries worksheet
                    worksheet = writer.sheets['HSBC Open TS Entries']
                    for idx, col in enumerate(open_flagged_data.columns):
                        max_length = max(
                            open_flagged_data[col].astype(str).apply(len).max(),
                            len(col)
                        )
                        column_letter = chr(65 + idx)
                        worksheet.column_dimensions[column_letter].width = max_length + 2
                        
                        # Format date columns
                        if col == 'Timesheet Period':
                            for cell in worksheet[f'{column_letter}2:{column_letter}{len(open_flagged_data)+1}']:
                                cell[0].number_format = 'yyyy-mm-dd'

                # Write Other flagged entries worksheet
                if not other_flagged_data.empty:
                    other_flagged_data.to_excel(
                        writer,
                        sheet_name='HSBC Other Flagged TS',
                        index=False
                    )
                    
                    # Auto-adjust column widths and format dates for Other flagged entries worksheet
                    worksheet = writer.sheets['HSBC Other Flagged TS']
                    for idx, col in enumerate(other_flagged_data.columns):
                        max_length = max(
                            other_flagged_data[col].astype(str).apply(len).max(),
                            len(col)
                        )
                        column_letter = chr(65 + idx)
                        worksheet.column_dimensions[column_letter].width = max_length + 2
                        
                        # Format date columns
                        if col == 'Timesheet Period':
                            for cell in worksheet[f'{column_letter}2:{column_letter}{len(other_flagged_data)+1}']:
                                cell[0].number_format = 'yyyy-mm-dd'

                # Write exit date reconciliation worksheet
                exit_date_data.to_excel(
                    writer,
                    sheet_name='HSBC ExitDate Recon',
                    index=False
                )
                
                # Auto-adjust column widths and format dates for exit date worksheet
                worksheet = writer.sheets['HSBC ExitDate Recon']
                for idx, col in enumerate(exit_date_data.columns):
                    max_length = max(
                        exit_date_data[col].astype(str).apply(len).max(),
                        len(col)
                    )
                    column_letter = chr(65 + idx)
                    worksheet.column_dimensions[column_letter].width = max_length + 2
                    
                    # Format date columns
                    if col in ['Onboard Date', 'Exit Date']:
                        for cell in worksheet[f'{column_letter}2:{column_letter}{len(exit_date_data)+1}']:
                            cell[0].number_format = 'yyyy-mm-dd'

                # Write Sub-Con/U TS Entry worksheet if data exists
                if sub_con_data is not None and len(sub_con_data) > 0:
                    sub_con_data.to_excel(
                        writer,
                        sheet_name='Sub-Con TS Entry',
                        index=False
                    )
                    
                    # Auto-adjust column widths and format dates for Sub-Con/U worksheet
                    worksheet = writer.sheets['Sub-Con TS Entry']
                    for idx, col in enumerate(sub_con_data.columns):
                        max_length = max(
                            sub_con_data[col].astype(str).apply(len).max(),
                            len(col)
                        )
                        column_letter = chr(65 + idx)
                        worksheet.column_dimensions[column_letter].width = max_length + 2
                        
                        # Format date columns
                        if col == 'Timesheet Period':
                            for cell in worksheet[f'{column_letter}2:{column_letter}{len(sub_con_data)+1}']:
                                cell[0].number_format = 'yyyy-mm-dd'

                # Write combined HSBC data as a new worksheet if provided
                if combined_hsbc_df is not None:
                    combined_hsbc_df.to_excel(
                        writer,
                        sheet_name='IN_CombinedCSV',
                        index=False
                    )

            logger.info(f"Saved combined report to {report_path}")
            return report_path

        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise

    def process_and_save_reports(self, hsbc_files, cg_files, mapping_file, output_dir):
        """Process and save all reports"""
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Read and combine input files
            combined_hsbc_df = self.read_multiple_files(hsbc_files)
            mapping_df = self.read_file(mapping_file)
            combined_cg_df = self.read_multiple_files(cg_files)
            
            # Process main reconciliation
            processed_data, unfiltered_data, sub_con_data = self.process_timesheet(combined_hsbc_df, mapping_df, combined_cg_df)
            
            # Process flagged entries
            open_flagged_data, other_flagged_data = self.process_flagged_timesheets(combined_hsbc_df, mapping_df)
            
            # Process exit date reconciliation using unfiltered data
            exit_date_data = self.process_exit_date_recon(combined_hsbc_df, mapping_df, unfiltered_data)
            
            # Generate combined Excel report with all worksheets and the combined HSBC data
            report_path = self.generate_report(processed_data, open_flagged_data, other_flagged_data, exit_date_data, output_dir, combined_hsbc_df=combined_hsbc_df, sub_con_data=sub_con_data)
            
            return report_path
            
        except Exception as e:
            logger.error(f"Error in process_and_save_reports: {str(e)}")
            raise

    def run(self):
        """Run the reconciliation process"""
        try:
            logger.info("Starting timesheet reconciliation process...")
            
            # Process and save reports
            report_path = self.process_and_save_reports(
                self.hsbc_files,
                self.cg_files,
                self.mapping_file,
                self.output_dir
            )
            
            logger.info("Timesheet reconciliation completed successfully")
            logger.info(f"Combined report saved to: {report_path}")
            
            return report_path
            
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