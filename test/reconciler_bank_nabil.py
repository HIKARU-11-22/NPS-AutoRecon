import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerNabil(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc1']):
            desc = str(row['Desc1'])
            match_eod = re.search(r"(PREF[A-Z0-9\-]*\d{1,2}-\d{1,2}|EOD[A-Z0-9\-]*\d{1,2}-\d{1,2})", desc)
            if match_eod:
                return match_eod.group(1)
        # Check Desc3 for NPS-IF and FTMS patterns
        if pd.notna(row['Desc3']):
            desc = str(row['Desc3']).upper()

            # Pattern: NPS-IF-xxxxxxxx
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return match_nps.group(1)  # Return only the 8-digit ID

            # Pattern: FTMS-xxxxxxx
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:
                return match_ftms.group(1)  # Return only the 7-digit ID

        # Check all description fields for other patterns
        for column in ['Desc1', 'Desc2', 'Desc3']:
            if pd.notna(row[column]):
                desc = str(row[column]).upper()

                # Pattern: MPAY NPS,10000xxxxxxx
                match_10000 = re.search(r"MPAY NPS,10000(\d{7})", desc)
                if match_10000:
                    return match_10000.group(1)

                # Pattern: -10000xxxxxxx
                match__10000 = re.search(r"-10000(\d{7})", desc)
                if match__10000:
                    return match__10000.group(1)
                
                if "PREFUNDING" in desc or "PREF" in desc or "NEPAL PAYMENT SYSTEM" in desc:
                    return "EOD"
                
            if pd.notna(row['Desc1']):
                desc = str(row['Desc1']).upper()
                match_A10000 = re.search(r"10000(\d{7})",desc)
                if match_A10000:
                    return match_A10000.group(0)[-7:]

                

        return None
