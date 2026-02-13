import re
import pandas as pd
from base_reconciler import BaseReconciler


class ReconcilerNepal(BaseReconciler):
    def extract_id(self, row):

        if pd.notna(row['Desc2']):
            desc = str(row['Desc2']).upper()
                # Pattern: Starts with LWT10000
            match_LWT = re.search(r"(LWT\d+)", desc)
            if match_LWT:
                return str(match_LWT.group(1))
            
            if "CIPS" in desc or "PREFNEPAL" in desc:
                return "EOD"
            
            
        if pd.notna(row['Desc3']):
            desc = str(row['Desc3']).upper()
            # Pattern: Starts with ftms
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:
                return match_ftms.group(1)
                    
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return str(match_nps.group(1))
            match_10000 = re.search(r"10000(\d{7})", desc)
            if match_10000:
                return str(match_10000.group(1))
            # new gateway
            match_1000 = re.search(r"1000(\d{8})", desc)
            if match_1000:
                return str(match_1000.group(1))
            if "CIPS" in desc or "PREFNEBL" in desc:
                return "EOD"
            
            match_ints = re.search(r"(IntS\d+)", desc)
            if match_ints or "STLMNT" in desc:
                return "INT"

        
                
        for column in ['Desc1']:
            if pd.notna(row[column]):
                desc = str(row[column])
                if "REV IME" in desc:
                    return "B2W REV"
                if "rev" in desc:
                    return "B2W REV"
            if "CIPS" in desc:
                return "EOD"
        return None
    
    def preprocess(self):
        # Remove any rows with 'IMEPAY' in Desc2
        self.bank_df = self.bank_df[~self.bank_df['Desc2'].astype(str).str.contains('IMEPAY:', na=False)]

        # Also remove 'Bank2Wallet' from SOA if needed
        self.soa_df = self.soa_df[self.soa_df['Transaction Type'] != 'Bank2Wallet']

        # Ensure numeric columns are cleaned
        for df in [self.bank_df, self.soa_df]:
            if 'Amount' in df.columns:
                df['Amount'] = (
                    df['Amount']
                    .astype(str)
                    .str.replace(',', '')
                    .str.strip()
                )
                df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

        super().preprocess()
    
    # def preprocess(self):
    #     # Remove any rows with 'IMEPAY' in Desc2
    #     self.bank_df = self.bank_df[~self.bank_df['Desc2'].astype(str).str.contains('IMEPAY:', na=False)]

    #     # Also remove 'Bank2Wallet' from SOA if needed
    #     self.soa_df = self.soa_df[self.soa_df['Transaction Type'] != 'Bank2Wallet']
    