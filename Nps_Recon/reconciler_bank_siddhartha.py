import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerSiddhartha(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc2']):
                desc = str(row['Desc2']).upper()
                
                match_d10000 = re.search(r"D10000000(\d{8})", desc)
                if match_d10000:
                    return match_d10000.group(0)[9:17]
                match_dmss = re.search(r"DMSS100000(\d{7})",desc)
                if match_dmss:
                     return match_dmss.group(0)[10:17]
                match_s10000 = re.search(r"S10000000(\d{8})",desc)
                if match_s10000:
                     return match_s10000.group(0)[9:17]
                match_ft = re.search(r"FT-(\d{8})",desc)
                if match_ft:
                     return match_ft.group(1)
                if "STLMNT" in desc:
                    return "INT"
        for col in ['Desc1']:
            if pd.notna(row.get(col)):
                desc = str(row[col])
                # match_10000 = re.search(r"10000\d{7}", desc)
                # if match_10000:
                #     return match_10000.group(0)[5:12]
                # new gateway
                match_1000 = re.search(r"1000\d{8}", desc)
                if match_1000:
                    return match_1000.group(0)[4:12]
                if "MSS-" in desc:
                     return "MSS SETTLEMENT"
        return None
                
                