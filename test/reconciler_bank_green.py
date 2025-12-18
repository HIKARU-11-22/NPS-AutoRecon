import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerGreen(BaseReconciler):
    def extract_id(self,row):
        for column in ['Desc3']:
            if pd.notna(row[column]):
                desc = str(row[column])
                if "IntS1000000" in desc:
                    return "INT"
                
        for column in ['Desc1', 'Desc2', 'Desc3']:
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

                # Pattern: Starts with 10000 and has at least 12 digits (e.g. 100008718767)
                match_10000 = re.search(r"10000\d{7}", desc)
                if match_10000:
                    full_match = match_10000.group(0)
                    return full_match[5:12]  # =MID(6, 7)
                if "EOD" in desc:
                    return "EOD"
                

        return None