import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerEvl(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc2']):
            desc = str(row['Desc2']).upper()

            match_nps_if = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps_if:
                return match_nps_if.group(1)
            
            match_WT = re.search(r"WT1000000(\d{6})", desc)
            if match_WT:
                return match_WT.group(1)

            match_ft = re.search(r"FT-(\d{8})", desc)
            if match_ft:
                return match_ft.group(1)
            
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:  
                return match_ftms.group(1)
            
            if "EOD" in desc or "PREF" in desc:
                return "EOD"
            
        if pd.notna(row['Desc3']):
            desc = str(row['Desc3']).upper()

            match__10000 = re.search(r"10000(\d{7})", desc)
            if match__10000:
                return match__10000.group(1)
            # new gateway
            match_1000 = re.search(r"1000(\d{8})", desc)
            if match_1000:
                return match_1000.group(1)
            
            match_nps_if = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps_if:
                return match_nps_if.group(1)
            

        if pd.notna(row['Desc1']):
            desc = str(row['Desc1']).upper()

            match_nps_if = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps_if:
                return match_nps_if.group(1)
            

            # match_Lwt = re.search(r"(LWT\d+)", desc)
            # if match_Lwt:
            #     return match_Lwt.group(1)
            
            # match_ints = re.search(r"(IntS\d+)", desc)
            # if match_ints or "STLMNT" in desc:
            #     return "INT"
            
            # match_revft = re.search(r"REV-/REV-NPS-IF-(\d{8})",desc)
            # if match_revft:
            #     return match_revft.group(1)
            # if "STLMNT" in desc:
            #     return "INT"

            # if "MSS-" in desc:
            #     return "MSS SETTLEMENT"

        return None
