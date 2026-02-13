import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerExcel(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Desc1']):
            desc = str(row['Desc1'])
            match_10000 = re.search(r"10000(\d{7})", desc)
            if match_10000:
                return match_10000.group(1)
            if "COMMISSION" in desc:
                return "COMMISSION"
                
            if pd.notna(row['Desc2']):
                desc = str(row['Desc2'])
                match_A10000 = re.search(r"10000(\d{7})",desc)
                if match_A10000:
                    return match_A10000.group(1)              
                # new gateway
                match_A1000 = re.search(r"1000(\d{8})", desc)
                if match_A1000:
                    return match_A1000.group(1)

        return None
