# reconciler_bank_machha.py

import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerMachha(BaseReconciler):
    def extract_id(self, row):
        # if pd.notna(row['Desc1']):
        #     desc = str(row['Desc1']).replace('_', '-')

        #     match_prefund = re.search(r"(EOD[A-Z0-9\-]*\d{2,2}-\d{2,2}|PREFUNDING[A-Z0-9\-]*\d{2,2}-\d{2,2}|PREFMBBL[A-Z0-9\-]*\d{2,2}-\d{2,2}|PREFMBL[A-Z0-9\-]*\d{2,2}-\d{2,2})|prefundmbl[A-Z0-9\-]*\d{2,2}-\d{2,2}|prefmbl[A-Z0-9\-]*\d{2,2}-\d{2,2}|prefundmbl[A-Z0-9\-]*\d{2,2}-\d{2,2}",desc)
        #     if match_prefund:
        #         return match_prefund.group(1)

        if pd.notna(row['Desc2']):
            desc = str(row['Desc2']).upper()
            # Pattern: NPS-IF-xxxxxxxx
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return match_nps.group(1)
            match_ft = re.search(r"FT-(\d{8})", desc)
            if match_ft:
                return match_ft.group(1)
            if "MSS" in desc:
                return "MSS SETTLEMENT"

        for col in ['Desc1', 'Desc2', 'Desc3']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()

                if any(x in desc for x in ["EOD", "PREFUNDING", "NEPAL PAYMENT SYST","FUN IPS","PREF","FUN CIP"]):
                    return "EOD"
                
                match_nps = re.search(r"NPS-IF-(\d{8})", desc)
                if match_nps:
                    return match_nps.group(1)

                if "STLMNT" in desc:
                    return "INT"
                
                # Match FTMS pattern
                match_ftms = re.search(r"FTMS-(\d{7})", desc)
                if match_ftms:
                    return match_ftms.group(1)

                # Match 10000xxxxx pattern
                match_10000 = re.search(r"10000\d{7}", desc)
                if match_10000:
                    return match_10000.group(0)[5:12]
                
                match_10001 = re.search(r"1000\d{8}", desc)
                if match_10001:
                    return match_10001.group(0)[4:12]

                # Fallback last
                match_custom = re.search(r"(\d{8})-[A-Z0-9_.]+", desc)
                if match_custom:
                    return match_custom.group(1)

        return None

