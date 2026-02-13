# reconciler_bank_nabil.py
import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerGarima(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc2']):
            desc = str(row['Desc2']).upper()
            # Pattern: Starts with 10000 
        match_10000 = re.search(r"10000\d{7}", desc)
        if match_10000:
            return match_10000.group(0)[5:12]  # =MID(6, 7)

            # match_ints = re.search(r"(IntS\d+)", desc)
            # if match_ints:
            #     return "INT"
            #     # return match_ints.group(1)
        match_eod = re.search(r"(pref[A-Z0-9\-]*\d{1,2}-\d{1,2}|PREF[A-Z0-9\-]*\d{1,2}-\d{1,2})", desc)
        if match_eod:
            return "EOD"
                
        for column in ['Desc3']:
            if pd.notna(row[column]):
                desc = str(row[column]).upper()

                # Pattern: NPS-IF-xxxxxxxx
                match_nps = re.search(r"NPS-IF-(\d{8})", desc)
                if match_nps:
                    return match_nps.group(1)

                # Pattern: FTMS-xxxxxxx
                match_ftms = re.search(r"FTMS-(\d{7})", desc)
                if match_ftms:
                    return match_ftms.group(1)
                
                if "CIPS" in desc:
                    return "EOD"
                

        for column in ['Desc1']:
            if pd.notna(row[column]):
                desc = str(row[column]).upper()
                if "NPS  COMMISSION" in desc:
                    return "COMMISSION"
           

        return None
