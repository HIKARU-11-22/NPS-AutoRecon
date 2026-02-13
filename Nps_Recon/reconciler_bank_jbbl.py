import re
import pandas as pd
from base_reconciler import BaseReconciler


class ReconcilerJyoti(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc1']):
            desc = str(row['Desc1']).upper()
            if "Rev-" in desc:
                 return "REV"
            match_lwt = re.search(r"(LWT\d+)", desc)
            if match_lwt:
                return match_lwt.group(1)
                
    
        if pd.notna(row['Desc2']):
            desc = str(row['Desc2']).upper()
            # Pattern: NPS-IF-xxxxxxxx
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return match_nps.group(1)
            
            # Pattern: FTMS-xxxxxxx
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:
                return match_ftms.group(1)

            match_wt1000000 = re.search(r"WT1000000(\d{6})", desc)
            if match_wt1000000:
                return match_wt1000000.group(1)
            # new gateway
            match_1000 = re.search(r"1000\d{8},NPSINSTA", desc)
            if match_1000:
                full_match = match_1000.group(0)
                return full_match[4:12]  # =MID(5, 8)

                
        for column in ['Desc1', 'Desc2', 'Desc3']:
            if pd.notna(row[column]):
                desc = str(row[column]).upper()
                # Pattern : eod 
                if "EOD" in desc:
                    return "EOD"
                if "NEPAL PAYMENT" in desc:
                    return "COMMISSION"
        
        return None