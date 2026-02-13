import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerKamana(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row.get('Desc2')):
            desc = str(row['Desc2'])
            match_ints = re.search(r"(IntS\d+)", desc)
            if match_ints:
                return str(match_ints.group(1))
            
            if "PREF" in desc or "EOD" in desc:
                return "EOD"

                    
        if pd.notna(row['Desc1']):
            desc = str(row['Desc1']).upper()
            
            # Pattern: NPS-IF-xxxxxxxx
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return str(match_nps.group(1))

            
        for col in ['Desc2']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()            
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return str(match_nps.group(1))
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:
                return str(match_ftms.group(1))
            
            match_wt = re.search(r"WT1000000(\d{6})", desc)
            if match_wt:
                return str(match_wt.group(1))                
            
        for col in ['Desc1','Desc3']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()
            
            # match_10000 = re.search(r"10000\d{7}", desc)
            # if match_10000:
            #     return str(match_10000.group(0)[-7:])
            # new gateway
            match_1000 = re.search(r"1000\d{8}", desc)
            if match_1000:
                return str(match_1000.group(0)[-8:])
            if "REV" in desc:
                 return "REV"
            
            if "PREF" in desc or "EOD" in desc:
                return "EOD"
 
        return None

    def preprocess(self):
        # Remove any rows with 'PRABHUPAY-NPS' in Desc1
        self.bank_df = self.bank_df[~self.bank_df['Desc1'].astype(str).str.contains('PRABHUPAY-NPS', na=False)]

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