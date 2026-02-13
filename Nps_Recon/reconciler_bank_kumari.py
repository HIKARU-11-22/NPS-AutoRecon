import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerKumari(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc1']):
            desc = str(row['Desc1']).upper()     
            if "cIPS/" in desc:
                return "cIPS" 

            match_D10000 = re.search(r"D10000000(\d{8})", desc)
            if match_D10000:
                return match_D10000.group(1)   
                
            # Pattern: NPS-IF-xxxxxxxx
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return match_nps.group(1)  # Return only the 8-digit ID
            
            # Pattern: FTMS-xxxxxxx
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:
                return match_ftms.group(1)  # Return only the 7-digit ID
            # Pattern: WTXXXXX
            match_wt = re.search(r"WT1000000(\d{6})", desc)
            if match_wt:
                return match_wt.group(1)  # Return only the 6-digit ID
            # Pattern: 10000xxxxxxx
            match_10000 = re.search(r"10000(\d{7})", desc)
            if match_10000: 
                return match_10000.group(1)  # Return only the 7-digit ID
            
        return None
                
                