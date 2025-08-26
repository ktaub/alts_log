import re
import pandas as pd
import os
from addepar_utils import fetch_addepar_data, set_up_environment
import warnings
import glob
from datetime import datetime
import openpyxl

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

# --------- Configuration ---------
DEBUG_OUTPUTS_ENABLED = False
TRANSACTIONS_CACHE_FILE = 'transactions_cache.xlsx'

def write_debug_excel(df, path):
    """Write debug DataFrame to Excel if debug is enabled."""
    try:
        if DEBUG_OUTPUTS_ENABLED and df is not None:
            df.to_excel(path, index=False)
    except Exception:
        pass

# ---------------------------------

def coerce_id_columns_to_int64(df):
    """Ensure ID columns are numeric (nullable Int64) for consistent joins."""
    for col in ['Direct Owner Entity ID', 'Entity ID']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    return df

def get_cached_transactions(cache_file=TRANSACTIONS_CACHE_FILE):
    """Load cached transactions and return DataFrame and last Trade Date."""
    if os.path.exists(cache_file):
        try:
            if os.path.getsize(cache_file) == 0:
                return None, None
            cached_df = pd.read_excel(cache_file, engine="openpyxl")
            cached_df = coerce_id_columns_to_int64(cached_df)
        except Exception:
            # Treat unreadable/invalid cache as missing
            return None, None
        if 'Posted Date' in cached_df.columns:
            cached_df['Posted Date'] = pd.to_datetime(cached_df['Posted Date'], errors='coerce')
            last_date = cached_df['Posted Date'].max()
            if pd.isna(last_date):
                last_date = None
        else:
            last_date = None
        return cached_df, last_date
    else:
        return None, None

def save_cached_transactions(df, cache_file=TRANSACTIONS_CACHE_FILE):
    """Save transactions DataFrame to cache file."""
    df.to_excel(cache_file, index=False)

def fetch_and_process_addepar_data(api_key, api_secret, firm_id, base_url, start_date, end_date):
    """Fetch and process transaction data from Addepar, using cache if available."""
    cache_file = TRANSACTIONS_CACHE_FILE
    cached_df, last_cached_date = get_cached_transactions(cache_file)
    today_dt = pd.to_datetime(end_date)
    
    # If cache exists, determine fetch_start_date
    if last_cached_date is not None:
        if last_cached_date.date() >= today_dt.date() - pd.Timedelta(days=1):
            # If last cached date is today, fetch from yesterday to today
            fetch_start_date = (today_dt - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            fetch_start_date = (last_cached_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        fetch_start_date = start_date
    
    # Only fetch if fetch_start_date <= end_date
    if pd.to_datetime(fetch_start_date) > today_dt:
        transactions_df = cached_df
    else:
        transactions_df, success, message = fetch_addepar_data(
            "transactions", "105390", api_key, api_secret, firm_id, base_url, fetch_start_date, end_date
        )
        if not success:
            print(message)
            # If cache exists, use cached data only
            if cached_df is not None:
                transactions_df = cached_df
            else:
                return None, None
        else:
            # Combine cached and new data
            if cached_df is not None:
                combined_df = pd.concat([cached_df, transactions_df], ignore_index=True)
                combined_df = combined_df.sort_values('Posted Date').drop_duplicates('ID', keep='last')
                transactions_df = combined_df
            # Save updated cache
            save_cached_transactions(transactions_df, cache_file)
    
    # Standardize ID columns as numeric for joins
    transactions_df = coerce_id_columns_to_int64(transactions_df)

    # Only include Distributions where Return of Capital is not zero
    mask = ~((transactions_df["Type"] == "Distribution") & (transactions_df["Return of Capital"] == 0))
    transactions_df = transactions_df[mask]


    # Group by Direct Owner and Security (names, not IDs)
    tx_grouped = transactions_df.groupby(["Direct Owner Entity ID", "Entity ID", "Type"]).agg({"Value": "sum", "Trade Date": "max"}).reset_index()
    tx_grouped = tx_grouped.rename(columns={"Value": "Total"})

    # Create a pivot table: one row per position, columns for each transaction type
    pivot = tx_grouped.pivot_table(
        index=["Direct Owner Entity ID", "Entity ID"],
        columns="Type",
        values="Total",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    # Convert all values to absolute
    for col in pivot.columns:
        if col in tx_grouped["Type"].unique():
            pivot[col] = pivot[col].abs()

    # Subtract cancellation columns from original transaction columns
    def adjust_for_cancellation(df, txn_type):
        orig_col = txn_type
        cancel_col = f'(Cancellation) {txn_type}'
        if orig_col in df.columns:
            if cancel_col in df.columns:
                df[orig_col] = df[orig_col] - df[cancel_col]
                df.drop(columns=[cancel_col], inplace=True)
            # Ensure no negative values after subtraction
            df[orig_col] = df[orig_col].clip(lower=0)
        return df

    for txn_type in ['Buy', 'Contribution', 'Distribution', 'Sell']:
        pivot = adjust_for_cancellation(pivot, txn_type)

    # Find last buy/contribution date
    buy_contrib = tx_grouped[tx_grouped["Type"].isin(["Buy", "Contribution"])]
    last_buy_contrib = buy_contrib.groupby(["Direct Owner Entity ID", "Entity ID"])["Trade Date"].max().reset_index()
    last_buy_contrib = last_buy_contrib.rename(columns={"Trade Date": "Last Buy/Contribution"})

    # Find last sell/distribution date
    sell_dist = tx_grouped[tx_grouped["Type"].isin(["Sell", "Distribution"])]
    last_sell_dist = sell_dist.groupby(["Direct Owner Entity ID", "Entity ID"])["Trade Date"].max().reset_index()
    last_sell_dist = last_sell_dist.rename(columns={"Trade Date": "Last Sell/Distribution"})

    # Merge into the pivot table
    result = pivot.merge(last_buy_contrib, on=["Direct Owner Entity ID", "Entity ID"], how="left")
    result = result.merge(last_sell_dist, on=["Direct Owner Entity ID", "Entity ID"], how="left")
    write_debug_excel(result, 'result.xlsx')

    # Standardize ID columns for the result as well
    result = coerce_id_columns_to_int64(result)

    return transactions_df, result

def process_alts_info_data():
    """Load and process the Alts Info Excel file."""
    # Read Alts Info file
    investment_status_df = pd.read_excel("Alts Info.xlsx", engine="openpyxl")
    investment_status_df = coerce_id_columns_to_int64(investment_status_df)
    
    # Coerce Account, Instrument, Client, Investment, Received in Addepar to string, strip whitespace, drop rows where Account/Instrument is empty or null
    investment_status_df['Account'] = investment_status_df['Account'].astype(str).str.strip()
    investment_status_df['Instrument'] = investment_status_df['Instrument'].astype(str).str.strip()

    if 'Client' in investment_status_df.columns:
        investment_status_df['Client'] = investment_status_df['Client'].astype(str).str.strip()
    else:
        investment_status_df['Client'] = ''
    if 'Investment' in investment_status_df.columns:
        investment_status_df['Investment'] = investment_status_df['Investment'].astype(str).str.strip()
    else:
        investment_status_df['Investment'] = ''
    if 'Received in Addepar' in investment_status_df.columns:
        investment_status_df['Received in Addepar'] = investment_status_df['Received in Addepar'].astype(str).str.strip()
    else:
        investment_status_df['Received in Addepar'] = ''
    # Keep rows without IDs to allow pending Buys (not yet in Addepar) to appear

    # Create a normalized instrument column for matching (use Investment when not yet in Addepar)
    instrument_match = investment_status_df['Instrument']
    mask_not_in_addepar = investment_status_df['Received in Addepar'].str.lower() == 'false'
    if 'Investment' in investment_status_df.columns:
        instrument_match = instrument_match.mask(mask_not_in_addepar, investment_status_df['Investment'])
    investment_status_df['Instrument_Match'] = instrument_match.astype(str).str.strip()
    
    # Convert Documentation Approval Date to datetime for calculations
    investment_status_df['Documentation Approval Date'] = pd.to_datetime(investment_status_df['Documentation Approval Date'], errors='coerce')
    
    # Normalize transaction type for robust matching
    if 'Transaction Type' in investment_status_df.columns:
        investment_status_df['Txn_Type'] = investment_status_df['Transaction Type'].astype(str).str.strip().str.lower()
    else:
        investment_status_df['Txn_Type'] = ''
    
    # Create Subscription Approval Date from Buy transactions
    buy_transactions = investment_status_df[investment_status_df['Transaction Type'] == 'Buy'].copy()
    subscription_dates = buy_transactions.groupby(["Direct Owner Entity ID", "Entity ID"])['Documentation Approval Date'].first().reset_index()
    subscription_dates.rename(columns={'Documentation Approval Date': 'Subscription Approval Date'}, inplace=True)

    # Create open/close summary: for each (Account, Instrument), is there any Completed? == False?
    complete_summary = investment_status_df.groupby(["Direct Owner Entity ID", "Entity ID"], dropna=False)['Completed?'].apply(lambda x: (x.astype(str).str.lower() == 'false').any()).reset_index()
    complete_summary = complete_summary.rename(columns={'Completed?': 'is_open'})
    # Ensure IDs remain numeric for downstream joins
    complete_summary = coerce_id_columns_to_int64(complete_summary)
    # is_open == True means at least one incomplete row, so should go to 'open'
    
    # save dfs to excel
    write_debug_excel(investment_status_df, 'investment_status_df.xlsx')
    write_debug_excel(subscription_dates, 'subscription_dates.xlsx')
    write_debug_excel(complete_summary, 'complete_summary.xlsx')
    
    return investment_status_df, subscription_dates, complete_summary

def consolidate_alts_info_data(investment_status_df, transactions_df):
    """Consolidate Alts Info data to one row per position with calculated remaining to sell.
    For Received in Addepar == False, group by Client + Instrument; otherwise, by Account + Instrument."""
    consolidated_positions = []

    # Split DataFrame
    received_false = investment_status_df[investment_status_df['Received in Addepar'].str.lower() == 'false'].copy()
    received_true = investment_status_df[investment_status_df['Received in Addepar'].str.lower() != 'false'].copy()

    # For items not yet in Addepar, they may lack IDs. Keep these for Open tab if Buy incomplete.
    # Already handled by keeping rows without IDs in process_alts_info_data.

    # Use Investment as Instrument for the 'false' group
    if not received_false.empty:
        received_false['Instrument'] = received_false['Investment']

    def build_positions(df, group_keys):
        if df.empty:
            return

        # Filter out blank identifiers per grouping style
        if 'Client' in group_keys:
            df = df[
                (df['Client'].astype(str).str.strip() != '') &
                (df['Instrument'].astype(str).str.strip() != '')
            ]
        else:
            df = df[
                df['Direct Owner Entity ID'].notna() &
                df['Entity ID'].notna()
            ]
        if df.empty:
            return

        for _, grp in df.groupby(group_keys, dropna=False):
            grp = grp.copy()

            account = str(grp['Account'].iloc[0]) if 'Account' in grp.columns else ''
            instrument = str(grp['Instrument'].iloc[0])
            direct_owner_entity_id = grp['Direct Owner Entity ID'].iloc[0] if 'Direct Owner Entity ID' in grp.columns else pd.NA
            entity_id = grp['Entity ID'].iloc[0] if 'Entity ID' in grp.columns else pd.NA

            client_series = grp['Client'].astype(str).str.strip().replace('', pd.NA).dropna() if 'Client' in grp.columns else pd.Series([], dtype=str)
            client_value = client_series.iloc[0] if not client_series.empty else ''

            complete_val = None
            if 'Completed?' in grp.columns and not grp['Completed?'].isnull().all():
                complete_val = grp['Completed?'].iloc[0]

            row = {
                'Direct Owner Entity ID': direct_owner_entity_id,
                'Entity ID': entity_id,
                'Account': account,
                'Client': client_value,
                'Instrument': instrument,
                'Original Commitment': 0,
                'Remaining to Sell': 0,
                'Instruction Date': None,
                'Documentation Approval Date': None,
                'Last Trade Date': None,
                'Completed?': complete_val
            }

            # Buys
            buy = grp[grp['Transaction Type'] == 'Buy']
            if not buy.empty:
                buy = buy.sort_values('Documentation Approval Date', ascending=False)
                latest_buy = buy.iloc[0]
                row['Original Commitment'] = pd.to_numeric(buy['Original Commitment'], errors='coerce').fillna(0).sum()
                row['Instruction Date'] = latest_buy.get('Instruction Date')
                row['Documentation Approval Date'] = latest_buy.get('Documentation Approval Date')
                row['Last Trade Date'] = latest_buy.get('Last Trade Date')

            # Sells and Liquidates (Remaining to Sell and Instruction Date)
            sell = grp[grp['Transaction Type'].isin(['Sell', 'Liquidate'])]
            total_remaining_to_sell = 0
            # If no Instruction Date from Buy, get it from the latest Sell/Liquidate
            if pd.isna(row['Instruction Date']) and not sell.empty:
                sell = sell.sort_values('Documentation Approval Date', ascending=False)
                latest_sell = sell.iloc[0]
                row['Instruction Date'] = latest_sell.get('Instruction Date')
                if pd.isna(row['Last Trade Date']):
                    row['Last Trade Date'] = latest_sell.get('Last Trade Date')
            if not sell.empty and 'Sell Amount' in investment_status_df.columns:
                for _, s in sell.iterrows():
                    original_sell_amount = s.get('Sell Amount', 0)
                    if pd.notna(original_sell_amount) and pd.notna(s.get('Documentation Approval Date')):
                        matching = transactions_df[
                            (transactions_df['Direct Owner Entity ID'] == direct_owner_entity_id) &
                            (transactions_df['Entity ID'] == entity_id) &
                            (transactions_df['Type'] == 'Sell') &
                            (pd.to_datetime(transactions_df['Last Trade Date'], errors='coerce') >= s['Documentation Approval Date'])
                        ]
                        addepar_amt = matching['Value'].abs().sum() if not matching.empty else 0
                        total_remaining_to_sell += max(0, original_sell_amount - addepar_amt)
                    else:
                        if pd.notna(original_sell_amount):
                            total_remaining_to_sell += original_sell_amount
            row['Remaining to Sell'] = total_remaining_to_sell

            consolidated_positions.append(row)

    # Group 1: Received in Addepar == False (Client + Instrument), Original Commitment = sum of buys
    build_positions(received_false, ['Client', 'Instrument'])

    build_positions(received_true, ['Direct Owner Entity ID', 'Entity ID'])

    consolidated_df = pd.DataFrame(consolidated_positions)
    write_debug_excel(consolidated_df, 'consolidated_df.xlsx')
    return consolidated_df

def merge_and_calculate_final_metrics(consolidated_investment_df, addepar_result, subscription_dates):
    """Merge data sources and calculate final financial metrics."""

    # Ensure Direct Owner Entity ID and Entity ID are numeric for consistent joins
    consolidated_investment_df = coerce_id_columns_to_int64(consolidated_investment_df)
    addepar_result = coerce_id_columns_to_int64(addepar_result)
    subscription_dates = coerce_id_columns_to_int64(subscription_dates)
    
    # Merge on normalized columns for case-insensitive join
    merged = consolidated_investment_df.merge(
        addepar_result,
        left_on=["Direct Owner Entity ID", "Entity ID"],
        right_on=["Direct Owner Entity ID", "Entity ID"],
        how="left"
    )
    
    # Add Subscription Approval Date
    merged = merged.merge(subscription_dates, on=['Direct Owner Entity ID', 'Entity ID'], how='left')

    #SAVE
    write_debug_excel(merged, 'merged_test.xlsx')
    
    # Fill missing Addepar columns with 0
    addepar_columns = ['Buy', 'Contribution', 'Contribution (Recalled)', 'Distribution', 'Sell']
    for col in addepar_columns:
        if col not in merged.columns:
            merged[col] = 0
        else:
            merged[col] = merged[col].fillna(0)

    merged["Contributed Capital"] = merged['Buy'] + merged["Contribution"] - merged["Contribution (Recalled)"]

    merged["Unfunded Capital"] = (
        merged["Original Commitment"]
        - merged["Contribution"]
        + merged["Contribution (Recalled)"]
        - merged["Buy"]
    )

    # Returned Capital
    merged["Returned Capital"] = (
        merged["Contribution (Recalled)"]
        + merged["Distribution"]
    )

    write_debug_excel(merged, 'merged.xlsx')

    return merged

def format_and_save_excel(merged_data, investment_status_df, output_filename):
    """Format dates and save the final Excel file with proper formatting."""
    # Select and reorder columns (removed Transaction Type since we now have one row per position)
    final_df = merged_data[["Client", "Account", "Instrument", "Instruction Date", "Last Trade Date", "Subscription Approval Date", "Original Commitment", "Contributed Capital", "Unfunded Capital", "Returned Capital", "Remaining to Sell", "Last Buy/Contribution", "Last Sell/Distribution", "Completed?", "is_open"]]
    
    # Prepare Open Reason column (populated below for Open rows only)
    final_df['Open Reason'] = ''

    # Ensure Account column has empty string instead of NaN, 'nan', 'None', or None
    final_df['Account'] = final_df['Account'].astype(str).replace(['nan', 'None'], '').replace({pd.NA: '', None: ''}).fillna('').str.strip()

    # Sort by Instruction Date (most recent last) and compute last dates per Account+Instrument
    final_df['Instruction Date'] = pd.to_datetime(final_df['Instruction Date'], errors='coerce')
    final_df['Last Trade Date'] = pd.to_datetime(final_df['Last Trade Date'], errors='coerce')
    # Compute last dates for each position
    final_df['Last Instruction Date'] = final_df.groupby(['Account', 'Instrument'])['Instruction Date'].transform('max')
    final_df['Last Trade Date'] = final_df.groupby(['Account', 'Instrument'])['Last Trade Date'].transform('max')
    # Replace 'Instruction Date' column with 'Last Instruction Date' at the same position in the output
    cols_order = list(final_df.columns)
    if 'Instruction Date' in cols_order and 'Last Instruction Date' in cols_order:
        insert_idx = cols_order.index('Instruction Date')
        # Remove any existing occurrence to avoid duplicates
        cols_order = [c for c in cols_order if c not in ['Instruction Date', 'Last Instruction Date']]
        cols_order.insert(insert_idx, 'Last Instruction Date')
        final_df = final_df[cols_order]
    # Sort by the computed Last Instruction Date
    sort_col = 'Last Instruction Date' if 'Last Instruction Date' in final_df.columns else 'Instruction Date'
    final_df = final_df.sort_values(sort_col, ascending=True, na_position='first')

    date_columns = ["Instruction Date", "Last Instruction Date", "Last Trade Date", "Subscription Approval Date", "Last Buy/Contribution", "Last Sell/Distribution"]
    for col in date_columns:
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors='coerce').dt.strftime('%m/%d/%Y')
            final_df[col] = final_df[col].fillna("")

    # Delete any previous 'Alts Log *.xlsx' files in the root directory
    for old_file in glob.glob('Alts Log *.xlsx'):
        try:
            os.remove(old_file)
        except Exception as e:
            print(f"Could not delete {old_file}: {e}")

    # Propagate is_open to all rows for each (Account, Instrument)
    final_df['is_open'] = final_df.groupby(['Account', 'Instrument'])['is_open'].transform('max')

    # Convert Unfunded Capital to numeric for comparison
    final_df['Unfunded Capital'] = pd.to_numeric(final_df['Unfunded Capital'], errors='coerce').fillna(0)
    
    # A position is considered "open" if it has unfunded capital OR if is_open is True
    final_df['is_open'] = (final_df['is_open'] != False) | (final_df['Unfunded Capital'] > 0)
    
    # Check for incomplete transactions in investment_status_df first (higher priority than Unfunded Capital)
    for idx, row in final_df.iterrows():
        if row['is_open'] != False:
            # Find matching rows in investment_status_df
            if row['Account'] and row['Instrument']:
                # For positions with Account and Instrument, compare using Instrument_Match for consistency
                matching_rows = investment_status_df[
                    (investment_status_df['Account'].astype(str).str.strip().str.lower() == str(row['Account']).strip().lower()) &
                    (investment_status_df['Instrument_Match'].astype(str).str.strip().str.lower() == str(row['Instrument']).strip().lower())
                ]
            else:
                # For positions without Account or without Addepar IDs, use Client and Instrument
                matching_rows = investment_status_df[
                    (investment_status_df['Client'].astype(str).str.strip().str.lower() == str(row['Client']).strip().lower()) &
                    (investment_status_df['Instrument_Match'].astype(str).str.strip().str.lower() == str(row['Instrument']).strip().lower())
                ]
            
            # Check for incomplete transactions (normalized types)
            incomplete_buys = matching_rows[
                (matching_rows['Txn_Type'] == 'buy') & 
                (matching_rows['Completed?'].astype(str).str.lower() == 'false')
            ]
            
            incomplete_sells = matching_rows[
                (matching_rows['Txn_Type'].isin(['sell'])) & 
                (matching_rows['Completed?'].astype(str).str.lower() == 'false')
            ]
            
            incomplete_liquidates = matching_rows[
                (matching_rows['Txn_Type'].isin(['liquidate'])) & 
                (matching_rows['Completed?'].astype(str).str.lower() == 'false')
            ]
            
            # Determine open reason based on incomplete transactions
            if not incomplete_buys.empty:
                final_df.loc[idx, 'Open Reason'] = 'Buy'
            elif not incomplete_sells.empty:
                final_df.loc[idx, 'Open Reason'] = 'Sell'
            elif not incomplete_liquidates.empty:
                final_df.loc[idx, 'Open Reason'] = 'Liquidate'

    # If still empty, then mark as Unfunded Capital
    final_df.loc[(final_df['Open Reason'] == '') & (final_df['Unfunded Capital'] > 0), 'Open Reason'] = 'Unfunded Capital'

    # Split into open and close sheets based on is_open
    open_df = final_df[final_df['is_open'] == True].copy()
    close_df = final_df[final_df['is_open'] == False].copy()

    # Drop Completed? and is_open from output (keep Account in both sheets)
    cols_to_drop = [col for col in ['Completed?', 'is_open'] if col in open_df.columns]
    open_df = open_df.drop(columns=cols_to_drop)
    close_df = close_df.drop(columns=cols_to_drop)
    
    # Do not include Open Reason in the Closed sheet
    if 'Open Reason' in close_df.columns:
        close_df = close_df.drop(columns=['Open Reason'])

    # Remove "Remaining to Sell" column from closed sheet since closed positions shouldn't have remaining amounts
    if 'Remaining to Sell' in close_df.columns:
        close_df = close_df.drop(columns=['Remaining to Sell'])

    # Reorder columns in Open sheet: place Open Reason after Client, Account, Instrument
    if 'Open Reason' in open_df.columns:
        prefix = [col for col in ['Client', 'Account', 'Instrument', 'Open Reason'] if col in open_df.columns]
        others = [c for c in open_df.columns if c not in prefix]
        open_df = open_df[prefix + others]

    def _format_sheet(writer, sheet_name, sheet_df):
        worksheet = writer.sheets[sheet_name]
        worksheet.sheet_view.zoomScale = 90
        all_currency_cols = ["Original Commitment", "Contributed Capital", "Unfunded Capital", "Returned Capital", "Remaining to Sell"]
        currency_cols = [col for col in all_currency_cols if col in sheet_df.columns]
        # Format currency columns
        for col_idx, col in enumerate(sheet_df.columns, 1):
            if col in currency_cols:
                for cell in worksheet[chr(64+col_idx)][1:]:  # skip header
                    cell.number_format = '"$"#,##0'
        # Set column widths based on formatted values
        for col_idx, col in enumerate(sheet_df.columns, 1):
            if col in currency_cols:
                sheet_df[col] = sheet_df[col].fillna(0)
                max_length = max(
                    len("${:,.0f}".format(cell.value if cell.value is not None and cell.value != '' else 0))
                    for cell in worksheet[chr(64+col_idx)][1:]
                )
                max_length = max(max_length, len(str(col)))
            else:
                max_length = max(
                    len(str(worksheet.cell(row=row, column=col_idx).value))
                    for row in range(1, worksheet.max_row + 1)
                )
            worksheet.column_dimensions[chr(64+col_idx)].width = max_length + 2
        # Conditional formatting for Open sheet
        if sheet_name == "Open" and 'Open Reason' in sheet_df.columns:
            open_reason_col_idx = list(sheet_df.columns).index('Open Reason') + 1
            open_reason_col_letter = chr(64 + open_reason_col_idx)
            for row in range(2, worksheet.max_row + 1):
                cell = worksheet[f"{open_reason_col_letter}{row}"]
                reason = cell.value
                if reason == 'Unfunded Capital':
                    cell.fill = openpyxl.styles.PatternFill(start_color='FFE6CC', end_color='FFE6CC', fill_type='solid')
                elif reason == 'Buy':
                    cell.fill = openpyxl.styles.PatternFill(start_color='E6FFE6', end_color='E6FFE6', fill_type='solid')
                elif reason == 'Sell' or reason == 'Liquidate':
                    cell.fill = openpyxl.styles.PatternFill(start_color='FFE6E6', end_color='FFE6E6', fill_type='solid')
                elif reason == 'Pending Transaction':
                    cell.fill = openpyxl.styles.PatternFill(start_color='F0F0F0', end_color='F0F0F0', fill_type='solid')

    # Save to Excel with adjusted column widths and currency formatting
    with pd.ExcelWriter(f"{output_filename}", engine="openpyxl") as writer:
        open_df.to_excel(writer, index=False, sheet_name="Open")
        close_df.to_excel(writer, index=False, sheet_name="Closed")
        _format_sheet(writer, "Open", open_df)
        _format_sheet(writer, "Closed", close_df)
    
    os.startfile(f"{output_filename}")

def main():
    """Main application function."""
    # Determine today's date for filename
    output_filename = f'Alts Log {datetime.now().strftime("%m-%d-%Y")}.xlsx'

    # If today's log already exists, open and exit
    if os.path.exists(output_filename):
        os.startfile(output_filename)
        exit()

    # Set up environment and API credentials
    api_key, api_secret, firm_id, base_url, start_date, end_date = set_up_environment()
    transactions_df, addepar_result = fetch_and_process_addepar_data(
        api_key, api_secret, firm_id, base_url, start_date, end_date
    )
    if transactions_df is None:
        return

    investment_status_df, subscription_dates, complete_summary = process_alts_info_data()
    consolidated_investment_df = consolidate_alts_info_data(investment_status_df, transactions_df)
    merged_data = merge_and_calculate_final_metrics(consolidated_investment_df, addepar_result, subscription_dates)
    merged_data = merged_data.merge(complete_summary, on=["Direct Owner Entity ID", "Entity ID"], how="left")
    format_and_save_excel(merged_data, investment_status_df, output_filename)


if __name__ == "__main__":
    main() 