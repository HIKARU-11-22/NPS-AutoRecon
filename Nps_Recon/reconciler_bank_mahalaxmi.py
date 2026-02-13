import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerMahalaxmi(BaseReconciler):
    
    def extract_id(self, row):
        # if pd.notna(row['Desc1']):
        #     desc = str(row['Desc1']).upper()
        #     self.bank_df.drop("~Date summary", axis=0)

        for col in ['Desc1', 'Desc2', 'Desc3']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()
                # Pattern: NPS-IF-xxxxxxxx
                match_nps = re.search(r"NPS-IF-(\d{8})", desc)
                if match_nps:
                    return match_nps.group(1)
                
                 # Match FTMS pattern
                match_ftms = re.search(r"FTMS-(\d{7})", desc)
                if match_ftms:
                    return match_ftms.group(1)
                # match_10000 = re.search(r"10000\d{7}", desc)
                # if match_10000:
                #     return match_10000.group(0)[-7:]
                # new gateway
                match_1000 = re.search(r"1000\d{8}", desc)
                if match_1000:
                    return match_1000.group(0)[-8:]
                if any(x in desc for x in ["EOD", "PREFUNDING", "NEPAL PAYMENT SYST","FUN IPS"]):
                    return "EOD"


        return None

