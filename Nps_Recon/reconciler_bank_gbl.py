import re
import pandas as pd
from base_reconciler import BaseReconciler


class ReconcilerGbl(BaseReconciler):

    def __init__(self, *args, **kwargs):
        # Initialize BaseReconciler (loads self.bank_df, etc.)
        super().__init__(*args, **kwargs)

        # ✅ ADD THE COLUMN HERE (bank-specific)
        self.bank_df['NReference'] = self.bank_df.apply(
            self.extract_nreference, axis=1
        )

    def extract_id(self, row):
        if pd.notna(row.get('Desc2')):
            desc = str(row['Desc2']).upper()

            if m := re.search(r"NPS-IF-(\d{8})", desc):
                return m.group(1)

            if m := re.search(r"FT-(\d{8})", desc):
                return m.group(1)

            if m := re.search(r"FTMS-(\d{7})", desc):
                return m.group(1)
            if "STLMNT" in desc:
                return "INT"

        if pd.notna(row.get('Desc1')):
            desc = str(row['Desc1']).upper()

            if m := re.search(r"LWT\d+", desc):
                return m.group(0)

            if m := re.search(r"10000(\d{7})", desc):
                return m.group(1)
            
            if "CIPS" in desc:
                return "EOD"
            if "MSS" in desc:
                return "MSS SETTLEMENT"

        return None

    def extract_nreference(self, row):
        # 1️⃣ Try to extract from Desc2
        if pd.notna(row.get('Desc2')):
            desc = str(row['Desc2']).upper()

            # Match S followed by digits, stopping before /
            match = re.search(r"(S\d+)", desc)
            if match:
                return match.group(1)

        # 2️⃣ Fallback to Reference No
        if pd.notna(row.get('Reference No')):
            return row['Reference No']

        return None
    def fill_missing_ids_with_reference(self, df):
        """
        Fill missing extracted_id in bank_df based on Reference No grouping.
        """
        df['extracted_id'] = df.apply(self.extract_id, axis=1)
        for ref_no, group in df.groupby('NReference'):
            valid_extracted_id = group['extracted_id'].dropna().iloc[0] if not group['extracted_id'].dropna().empty else None
            if valid_extracted_id:
                df.loc[df['NReference'] == ref_no, 'extracted_id'] = df.loc[df['NReference'] == ref_no, 'extracted_id'].fillna(valid_extracted_id)
        return df


