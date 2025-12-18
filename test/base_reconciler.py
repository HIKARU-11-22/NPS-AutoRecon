import pandas as pd
import datetime
from io import BytesIO
from itertools import zip_longest  # add at top of file


class BaseReconciler:
    def __init__(self, bank_file, soa_file):
        self.bank_df = pd.read_excel(bank_file)
        self.soa_df = pd.read_excel(soa_file)
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    def extract_id(self, row):
        """
        Inline logic for extracting the ID from the row.
        This checks if 'Transaction Id' exists, if not, tries to extract from 'Remarks' or falls back to None.
        """
        # Check if 'Transaction Id' exists
        if pd.notna(row['Transaction Id']):
            return row['Transaction Id']
        
        # If 'Transaction Id' is missing, check 'Remarks'
        if pd.notna(row['Remarks']):
            remarks = str(row['Remarks']).upper()
            if 'ID' in remarks:
                return remarks.split('ID')[1].strip()  # Extract ID after "ID"
        
        # If neither 'Transaction Id' nor a valid 'Remarks' is found, return None
        return None

    def fill_missing_ids_with_reference(self, df):
        """
        Fill missing extracted_id in bank_df based on Reference No grouping.
        """
        df['extracted_id'] = df.apply(self.extract_id, axis=1)
        for ref_no, group in df.groupby('Reference No'):
            valid_extracted_id = group['extracted_id'].dropna().iloc[0] if not group['extracted_id'].dropna().empty else None
            if valid_extracted_id:
                df.loc[df['Reference No'] == ref_no, 'extracted_id'] = df.loc[df['Reference No'] == ref_no, 'extracted_id'].fillna(valid_extracted_id)
        return df

    def preprocess(self):
        
        # Step 1: Bank ID Extraction + Fill Missing
        self.bank_df = self.fill_missing_ids_with_reference(self.bank_df)
        self.bank_df['Txn Type'] = self.bank_df['Txn Type'].str.upper().replace({'CR': 'C', 'DR': 'D'})

        # self.bank_df = self.bank_df[self.bank_df['extracted_id'] != 'PRABHUPAY-NPS'] # to delete the row with prabhupay-nps

        # added function checks the id and remerks and if they are same then it merges and remove one of them 

        self.soa_df = (
            self.soa_df
            .groupby(['Transaction Id', 'Remarks'], as_index=False)
            .agg({
                'Opening Balance': 'first',
                'Mode': 'first',
                'Transaction Type': 'first',
                'Amount': 'sum',
                'Closing Blance': 'last',
                'Date': 'first',
                'MerchantName': 'first'
            })
        )

        # self.soa_df = self.soa_df[self.soa_df['Transaction Type'] != 'Bank2Wallet'] # to delete the row with Bank2Wallet in soa

        # Step 2: SOA ID Extraction (custom logic added here)
        self.soa_df['Remarks'] = self.soa_df['Remarks'].astype(str).str.upper()
        self.soa_df['extracted_id'] = self.soa_df.apply(
            lambda row: (
                'EOD' if any(x in str(row.get('Transaction Type', '')) for x in ['BankVoucherEntry', 'NchlVoucherEntry'])
                else ('INT' if 'InternalSettlement' in str(row.get('Transaction Type', ''))
                      else ('MSS SETTLEMENT' if 'MSSRepayment' in str(row.get('Transaction Type', ''))else row.get('Transaction Id')))
            ),
            axis=1
        )
        self.soa_df
        

        # Step 3: Round + clean amounts
        self.bank_df['Amount'] = self.bank_df['Amount'].round(2)
        self.soa_df['Amount'] = self.soa_df['Amount'].round(2)
        self.bank_df['clean_amount'] = self.bank_df['Amount'].abs()

    def reconcile(self):
        import pandas as pd

        # Normalize text
        self.bank_df['Txn Type'] = self.bank_df['Txn Type'].str.upper().replace({'CR': 'C', 'DR': 'D'})
        self.soa_df['Mode'] = self.soa_df['Mode'].str.upper()

        # Reset index to get unique row identifiers
        self.bank_df = self.bank_df.reset_index(drop=True)
        self.soa_df = self.soa_df.reset_index(drop=True)
        self.bank_df['row_id'] = self.bank_df.index
        self.soa_df['row_id'] = self.soa_df.index

        # Create match keys (extracted_id + amount)
        self.bank_df['match_key'] = self.bank_df['extracted_id'].astype(str) + "-" + self.bank_df['clean_amount'].astype(str)
        self.soa_df['match_key'] = self.soa_df['extracted_id'].astype(str) + "-" + self.soa_df['Amount'].astype(str)

        def one_to_one_match(bank_type, soa_type):
            bank = self.bank_df[self.bank_df['Txn Type'] == bank_type].copy()
            soa = self.soa_df[self.soa_df['Mode'] == soa_type].copy()

            merged = pd.merge(bank, soa, on='match_key', suffixes=('_bank', '_soa'))

            # Sort to prioritize earliest match
            merged = merged.sort_values(by=['Date_bank', 'Date_soa'])

            # Enforce one-to-one by keeping only first match per bank and SOA row
            merged = merged.drop_duplicates(subset='row_id_bank', keep='first')
            merged = merged.drop_duplicates(subset='row_id_soa', keep='first')

            return merged

        # Match Debit (D) to Credit (CR) and Credit (C) to Debit (DR)
        matched_cr = one_to_one_match('D', 'CR')
        matched_dr = one_to_one_match('C', 'DR')

        all_matches = pd.concat([matched_cr, matched_dr], ignore_index=True)
        all_matches['match_pair'] = all_matches['Mode'] + "-" + all_matches['Txn Type']

        # Keep only valid direction matches
        valid = all_matches[all_matches['match_pair'].isin(['CR-D', 'DR-C'])].copy()

        # Create recon keys for later lookup
        valid['recon_key_bank'] = (
            valid['extracted_id_bank'].astype(str) + "-" +
            valid['clean_amount'].astype(str) + "-" +
            valid['Txn Type']
        )
        valid['recon_key_soa'] = (
            valid['extracted_id_soa'].astype(str) + "-" +
            valid['Amount_soa'].astype(str) + "-" +
            valid['Mode']
        )

        self.matched = valid

        # Get unmatched bank and soa using row_id
        matched_bank_ids = set(valid['row_id_bank'])
        matched_soa_ids = set(valid['row_id_soa'])

        self.unmatched_bank = self.bank_df[~self.bank_df['row_id'].isin(matched_bank_ids)].copy()
        self.unmatched_soa = self.soa_df[~self.soa_df['row_id'].isin(matched_soa_ids)].copy()

        # Flag extracted_ids in unmatched that were matched in some row
        matched_ids = set(valid['extracted_id_bank']).union(valid['extracted_id_soa'])
        self.unmatched_bank['id_already_matched'] = self.unmatched_bank['extracted_id'].isin(matched_ids)
        self.unmatched_soa['id_already_matched'] = self.unmatched_soa['extracted_id'].isin(matched_ids)

    def to_excel_bytes(self, df):
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        return output

    def run_all(self):
        self.preprocess()
        self.reconcile()

        # === Step 1: Prepare totals and unmatched totals ===
        # SOA totals
        soa_df = self.soa_df
        soa_df['Mode'] = soa_df['Mode'].astype(str).str.upper()
        soa_total_credit = soa_df.loc[soa_df['Mode'] == 'CR', 'Amount'].sum()
        soa_total_debit = soa_df.loc[soa_df['Mode'] == 'DR', 'Amount'].sum()
        soa_cr_count = soa_df.loc[soa_df['Mode'] == 'CR'].shape[0]
        soa_dr_count = soa_df.loc[soa_df['Mode'] == 'DR'].shape[0]

        # Bank totals
        bank_df = self.bank_df
        bank_df['Txn Type'] = bank_df['Txn Type'].astype(str).str.upper()
        bank_total_credit = bank_df.loc[bank_df['Txn Type'] == 'C', 'Amount'].sum()
        bank_total_debit = abs(bank_df.loc[bank_df['Txn Type'] == 'D', 'Amount'].sum())
        bank_cr_count = bank_df.loc[bank_df['Txn Type'] == 'C'].shape[0]
        bank_dr_count = bank_df.loc[bank_df['Txn Type'] == 'D'].shape[0]

        # Unmatched totals
        unmatched_bank = self.unmatched_bank.copy()
        unmatched_bank['Txn Type'] = unmatched_bank['Txn Type'].str.upper()
        unmatched_bank_total_credit = unmatched_bank.loc[unmatched_bank['Txn Type'] == 'C', 'Amount'].sum()
        unmatched_bank_total_debit = abs(unmatched_bank.loc[unmatched_bank['Txn Type'] == 'D', 'Amount'].sum())

        unmatched_soa = self.unmatched_soa.copy()
        unmatched_soa['Mode'] = unmatched_soa['Mode'].str.upper()
        unmatched_soa_total_credit = unmatched_soa.loc[unmatched_soa['Mode'] == 'CR', 'Amount'].sum()
        unmatched_soa_total_debit = unmatched_soa.loc[unmatched_soa['Mode'] == 'DR', 'Amount'].sum()

        # Subtotals
        sub_total_soa_cr = round(soa_total_credit - unmatched_soa_total_credit, 2)
        sub_total_soa_dr = round(soa_total_debit - unmatched_soa_total_debit, 2)
        sub_total_bank_cr = round(bank_total_credit - unmatched_bank_total_credit, 2)
        sub_total_bank_dr = round(abs(bank_total_debit - unmatched_bank_total_debit), 2)

        # Differences
        total_difference_cr = round(sub_total_soa_cr - sub_total_bank_dr, 2)
        total_difference_dr = round(sub_total_soa_dr - sub_total_bank_cr, 2)

        # === Step 2: Create Summary DataFrame ===
        summary_data = {
            '': ['CR', 'DR'],
            'soa': [soa_total_credit, soa_total_debit],
            'difference_amount_soa': [unmatched_soa_total_credit, unmatched_soa_total_debit],
            'Sub-total_soa': [sub_total_soa_cr, sub_total_soa_dr],
            'count_soa': [soa_cr_count, soa_dr_count],
            'transaction_type': ['DR', 'CR'],
            'bank': [bank_total_debit, bank_total_credit],
            'difference_amount_bank': [unmatched_bank_total_debit, unmatched_bank_total_credit],
            'Sub-total_bank': [sub_total_bank_dr, sub_total_bank_cr],
            'count_bank': [bank_dr_count, bank_cr_count],
            'total': [total_difference_cr, total_difference_dr],
        }

        summary_df = pd.DataFrame(summary_data)

        # === Step 3: Write Excel summary to BytesIO ===
        summary_output = BytesIO()
        with pd.ExcelWriter(summary_output, engine='xlsxwriter') as writer:
            workbook = writer.book
            worksheet = workbook.add_worksheet('Summary')

            headers = {
                0: ['', '', '', '', '', '', '', '', '', '', ''],
                1: ['', 'SOA', '', 'Sub-Total', 'Count','', 'BANK', '', 'Sub-Total', 'Count', 'Total Difference'],
            }
            footer ={
                2: ['ID', 'CR', 'DR', 'Remarks','','', 'ID', 'CR', 'DR', 'Remarks', '']
            }
            for row_idx, header_row in headers.items():
                for col_idx, value in enumerate(header_row):
                    worksheet.write(row_idx, col_idx, value)

            for row_idx, row in summary_df.iterrows():
                for col_idx, val in enumerate(row):
                    worksheet.write(row_idx + 2, col_idx, val)

            footer_row = len(summary_df) + 2
            for col_idx, value in enumerate(footer[2]):
                worksheet.write(footer_row, col_idx, value)

            # === Adding Footer with Unmatched Transaction IDs and Amounts ===
            footer_soa = self.unmatched_soa[['extracted_id', 'Amount', 'Mode', 'Remarks']]  # Only unmatched SOA
            footer_bank = self.unmatched_bank[['extracted_id', 'Amount', 'Txn Type']]  # Only unmatched Bank

            # Footer format: id | cr | dr | remarks | id | cr | dr | remarks
            

            footer_start_row = len(summary_df) + 3
            for i, (soa_row, bank_row) in enumerate(zip_longest(footer_soa.iterrows(), footer_bank.iterrows(), fillvalue=(None, pd.Series()))):
            #for i, (soa_row, bank_row) in enumerate(zip(footer_soa.iterrows(), footer_bank.iterrows())):
                soa_data = soa_row[1]
                bank_data = bank_row[1]

                # SOA Data
                if pd.notna(soa_data.get('extracted_id', None)):
                    worksheet.write(footer_start_row + i, 0, soa_data['extracted_id'])  # SOA ID
                    worksheet.write(footer_start_row + i, 1, soa_data['Amount'] if soa_data['Mode'] == 'CR' else '')  # SOA Credit
                    worksheet.write(footer_start_row + i, 2, soa_data['Amount'] if soa_data['Mode'] == 'DR' else '')  # SOA Debit
                    # worksheet.write(footer_start_row + i, 3, soa_data['Remarks'])  # SOA Remarks
                
                # Bank Data
                if pd.notna(bank_data.get('extracted_id', None)):
                    worksheet.write(footer_start_row + i, 6, bank_data['extracted_id'])  # Bank ID
                    worksheet.write(footer_start_row + i, 7, bank_data['Amount'] if bank_data['Txn Type'] == 'C' else '')  # Bank Credit
                    worksheet.write(footer_start_row + i, 8, bank_data['Amount'] if bank_data['Txn Type'] == 'D' else '')  # Bank Debit
                    worksheet.write(footer_start_row + i, 9, '')  # No remarks for Bank data
                    
            # Set column widths for better readability
            worksheet.set_column('A:K', 15)

        summary_output.seek(0)

        # === Step 4: Return all results ===
        return {
            "matched": self.to_excel_bytes(self.matched),
            "unmatched_bank": self.to_excel_bytes(self.unmatched_bank),
            "unmatched_soa": self.to_excel_bytes(self.unmatched_soa),
            "summary": {
                "matched": len(self.matched),
                "unmatched_bank": len(self.unmatched_bank),
                "unmatched_soa": len(self.unmatched_soa),
            },
            "summary_excel": summary_output,  # You can now download this from app.py
        }
