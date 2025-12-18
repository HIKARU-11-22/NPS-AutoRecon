
import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerAdbl(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row['Reference No']):
                desc = str(row['Reference No']).upper()
                # Pattern: NPSD1000
                match_npsd = re.search(r"NPSD10000000(\d{8})", desc)
                if match_npsd:
                    return match_npsd.group(0)[-8:]
                match_npss = re.search(r"NPSS10000000(\d{8})", desc)
                if match_npss:
                    return match_npss.group(0)[-8:]
                match_npsdmss = re.search(r"NPSDMSS100000(\d{7})", desc)
                if match_npsdmss:
                     return match_npsdmss.group(0)[-7:]
                match_npsswt = re.search(r"NPSSWT1000000(\d{6})",desc)
                if match_npsswt:
                     return match_npsswt.group(0)[-6:]
                
        for col in ['Desc1', 'Desc2', 'Desc3']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()

                # if any(x in desc for x in ["PREFUNDING", "NEPAL PAYMENT SYST"]):
                #     return "EOD"
                if "STLMNT" in desc:
                    return "INT"

                match_10000 = re.search(r"10000\d{7}", desc)
                if match_10000:
                    return match_10000.group(0)[5:12]
                    
        return None