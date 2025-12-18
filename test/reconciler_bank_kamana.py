import re
import pandas as pd
from base_reconciler import BaseReconciler


class ReconcilerKamana(BaseReconciler):
    def extract_id(self, row):
        for column in ['Desc1', 'Desc2', 'Desc3']:
            if pd.notna(row[column]):
                desc = str(row[column]).upper()
                # Pattern: Srarts with WT10000
                match__WT10000 = re.search(r"WT1000000\d{6}", desc)
                if match__WT10000:
                    all_match = match__WT10000.group(0)
                    return all_match[-6:]

                # Pattern: NPS-IF-xxxxxxxx
                match_nps = re.search(r"NPS-IF-(\d{8})", desc)
                if match_nps:
                    return match_nps.group(1)

                # Pattern: FTMS-xxxxxxx
                match_ftms = re.search(r"FTMS-(\d{7})", desc)
                if match_ftms:
                    return match_ftms.group(1)

                # Pattern: Starts with 10000 
                match_10000 = re.search(r"10000\d{7}", desc)
                if match_10000:
                    full_match = match_10000.group(0)
                    return full_match[5:12] 
                match__10000 = re.search(r"-10000\d{7}", desc)
                if match__10000:
                    all_match = match__10000.group(0)
                    return all_match[-7:]
                match_ft = re.search(r"FT-\d{8}",desc)
                if match_ft:
                    rev_match = match_ft.group(0)
                    return rev_match[3:11]

                
                if "MSS-" in desc:
                    return "MSS SETTLEMENT"
                if "EOD" in desc:
                    return "EOD"

        return None
    