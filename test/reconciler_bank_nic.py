import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerNic(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc2']):
            desc = str(row['Desc2']).upper()

            match_nps = re.search(r"/NPS-IF-(\d{8})", desc)
            if match_nps:
                return match_nps.group(1)

            match_WT = re.search(r"/WT1000000(\d{6})", desc)
            if match_WT:
                return match_WT.group(1)

            match__10000 = re.search(r"/10000(\d{7})", desc)
            if match__10000:
                return match__10000.group(1)

            match_10000 = re.search(r"10000(\d{7})", desc)
            if match_10000:
                return match_10000.group(1)

            match_ftms = re.search(r"/FTMS-(\d{7})", desc)
            if match_ftms:
                return match_ftms.group(1)

            match_ft = re.search(r"FT-(\d{8})", desc)
            if match_ft:
                return match_ft.group(1)
            
            match_revft = re.search(r"REV-/REV-NPS-IF-(\d{8})",desc)
            if match_revft:
                return match_revft.group(1)
            if "STLMNT" in desc:
                return "INT"

        if pd.notna(row.get('Desc1')):
            desc = str(row['Desc1'])

            match_nps_bpi = re.search(r"NPS/Banking Payment Initiate-10000(\d{7})", desc)
            if match_nps_bpi:
                return match_nps_bpi.group(1)

            match_nps10000 = re.search(r"NPS/10000(\d{7})", desc)
            if match_nps10000:
                return match_nps10000.group(1)

            match_custom = re.search(r"-(?:\d{5})(\d{7})\b", desc)
            if match_custom:
                return match_custom.group(1)

            
            if "CIPS" in desc:
                return "EOD"

            if 'IPGadvice' in desc:
                return "IPG"

        return None
