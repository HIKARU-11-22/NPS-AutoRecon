import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerPrabhu(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row.get('Desc2')):
            desc = str(row['Desc2'])
            match_ints = re.search(r"(IntS\d+)", desc)
            if match_ints:
                return match_ints.group(1)
            
        if pd.notna(row['Desc1']):
            desc = str(row['Desc1']).upper()
            match_prefund = re.search(r"(EOD[A-Z0-9\-]*\d{1,2}-\d{1,2}|PREFUNDING[A-Z0-9\-]*\d{1,2}-\d{1,2}|PREFPRVU[A-Z0-9\-]*\d{1,2}-\d{1,2})",desc)
            if match_prefund:
                return match_prefund.group(1)
            
            # Pattern: NPS-IF-xxxxxxxx
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return match_nps.group(1)
            
        for col in ['Desc2']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()
            
            match_10000 = re.search(r"10000\d{7}", desc)
            if match_10000:
                return match_10000.group(0)[-7:]
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return match_nps.group(1)
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:
                return match_ftms.group(1)
            
        for col in ['Desc1']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()
            
            match_10000 = re.search(r"10000\d{7}", desc)
            if match_10000:
                return match_10000.group(0)[-7:]
            if "REV" in desc:
                 return "REV"

        return None
    def preprocess(self):
        #  Remove any rows with 'PRABHUPAY-NPS' in Desc1
        self.bank_df = self.bank_df[~self.bank_df['Desc1'].astype(str).str.contains('PRABHUPAY-NPS', na=False)]

        #  Also remove 'Bank2Wallet' from SOA if needed
        self.soa_df = self.soa_df[self.soa_df['Transaction Type'] != 'Bank2Wallet']
        super().preprocess()