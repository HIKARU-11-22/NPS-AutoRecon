import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerRBB(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc3']):
            desc = str(row['Desc3']).upper()

            match_ints = re.search(r"(IntS\d+)", desc)
            if match_ints or "STL" in desc:
                return "INT"
            
            match_ft = re.search(r"FT-(\d{8})", desc)
            if match_ft:
                return match_ft.group(1)

            match_nps_if = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps_if:
                return match_nps_if.group(1)
            match_TXN = re.search(r"TXN ID:(\d{8})", desc)
            if match_TXN:
                return match_TXN.group(1)
            
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:  
                return match_ftms.group(1)
            
            match_WT = re.search(r"WT1000000(\d{6})", desc)
            if match_WT:
                return match_WT.group(1)
            
                        
            match__1000 = re.search(r"1000(\d{8})", desc)
            if match__1000:
                return match__1000.group(1)
            
            
            
        if pd.notna(row['Desc2']):
            desc = str(row['Desc2']).upper()
            
            match__1000 = re.search(r"1000(\d{8})", desc)
            if match__1000:
                return match__1000.group(1)
            
            if "EOD" in desc:
                return "EOD"
            

        if pd.notna(row['Desc1']):
            desc = str(row['Desc1']).upper()
            
            match_nps_if = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps_if:
                return match_nps_if.group(1)
            
            # if "REV" in desc:
            #     return "REV"


           
            match_Lwt = re.search(r"(LWT\d+)", desc)
            if match_Lwt:
                return match_Lwt.group(1)

           

            
            # match_revft = re.search(r"REV-/REV-NPS-IF-(\d{8})",desc)
            # if match_revft:
            #     return match_revft.group(1)
            # if "STLMNT" in desc:
            #     return "INT"

            # match_custom = re.search(r"(?:\d{5})(\d{7})\b", desc)
            # if match_custom:
            #     return match_custom.group(1)
            if "MSS-" in desc:
                return "MSS SETTLEMENT"

        return None
