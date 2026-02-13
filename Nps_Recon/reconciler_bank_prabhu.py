import re
import pandas as pd
from base_reconciler import BaseReconciler

class ReconcilerPrabhu(BaseReconciler):
    def extract_id(self, row):
        if pd.notna(row.get('Desc2')):
            desc = str(row['Desc2'])
            match_ints = re.search(r"(IntS\d+)", desc)
            if match_ints:
                return "INT"
            
        if pd.notna(row['Desc1']):
            desc = str(row['Desc1']).upper()
            match_prefund = re.search(r"(EOD[A-Z0-9\-]*\d{1,2}-\d{1,2}|PREF[A-Z0-9\-]*\d{1,2}-\d{1,2}|PREFPRVU[A-Z0-9\-]*\d{1,2}-\d{1,2})",desc)
            if match_prefund:
                return "EOD"
        
            
                # return str(match_prefund.group(1))
            
            # Pattern: NPS-IF-xxxxxxxx
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return str(match_nps.group(1))
        # if pd.notna(row['Desc3']):
        #     desc = str(row['Desc3'])
        #     if "null/[0-9]" in desc:
        #         return "EOD"

        for col in ['Desc2']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()
            
            match_10000 = re.search(r"10000\d{7}", desc)
            if match_10000:
                return str(match_10000.group(0)[-7:])
            # new gateway
            match_1000 = re.search(r"1000\d{8}", desc)
            if match_1000:
                return str(match_1000.group(0)[-8:])
            match_nps = re.search(r"NPS-IF-(\d{8})", desc)
            if match_nps:
                return str(match_nps.group(1))
            match_ftms = re.search(r"FTMS-(\d{7})", desc)
            if match_ftms:
                return str(match_ftms.group(1))
            
            if "NPS@999" in desc:
                return "EOD"
            
        for col in ['Desc1']:
            if pd.notna(row.get(col)):
                desc = str(row[col]).upper()
            
            # match_10000 = re.search(r"10000\d{7}", desc)
            # if match_10000:
            #     return str(match_10000.group(0)[-7:])
            # new gateway
            match_1000 = re.search(r"1000\d{8}", desc)
            if match_1000:
                return str(match_1000.group(0)[-8:])
            if "REV" in desc:
                 return "REV"
            

        return None

    def preprocess(self):
        # Remove any rows with 'PRABHUPAY-NPS' in Desc1
        self.bank_df = self.bank_df[~self.bank_df['Desc1'].astype(str).str.contains('PRABHUPAY-NPS', na=False)]

        # Also remove 'Bank2Wallet' from SOA if needed
        self.soa_df = self.soa_df[self.soa_df['Transaction Type'] != 'Bank2Wallet']

        # Ensure numeric columns are cleaned
        for df in [self.bank_df, self.soa_df]:
            if 'Amount' in df.columns:
                df['Amount'] = (
                    df['Amount']
                    .astype(str)
                    .str.replace(',', '')
                    .str.strip()
                )
                df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

        super().preprocess()



# class ReconcilerNic(BaseReconciler):
#     def extract_id(self, row):
#         if pd.notna(row['Desc2']):
#             desc = str(row['Desc2']).upper()

#             match_nps = re.search(r"/NPS-IF-(\d{8})", desc)
#             if match_nps:
#                 return str(match_nps.group(1))

#             match_WT = re.search(r"/WT1000000(\d{6})", desc)
#             if match_WT:
#                 return str(match_WT.group(1))

#             match__10000 = re.search(r"/10000(\d{7})", desc)
#             if match__10000:
#                 return str(match__10000.group(1))

#             match_10000 = re.search(r"10000(\d{7})", desc)
#             if match_10000:
#                 return str(match_10000.group(1))

#             match_ftms = re.search(r"/FTMS-(\d{7})", desc)
#             if match_ftms:
#                 return str(match_ftms.group(1))

#             match_ft = re.search(r"FT-(\d{8})", desc)
#             if match_ft:
#                 return str(match_ft.group(1))
            
#             match_revft = re.search(r"REV-/REV-NPS-IF-(\d{8})",desc)
#             if match_revft:
#                 return str(match_revft.group(1))
#             if "STLMNT" in desc:
#                 return "INT"

#         if pd.notna(row.get('Desc1')):
#             desc = str(row['Desc1'])

#             match_nps_bpi = re.search(r"NPS/Banking Payment Initiate-10000(\d{7})", desc)
#             if match_nps_bpi:
#                 return str(match_nps_bpi.group(1))

#             match_nps10000 = re.search(r"NPS/10000(\d{7})", desc)
#             if match_nps10000:
#                 return str(match_nps10000.group(1))

#             match_custom = re.search(r"-(?:\d{5})(\d{7})\b", desc)
#             if match_custom:
#                 return str(match_custom.group(1))

#             if "CIPS" in desc:
#                 return "EOD"

#             if 'IPGadvice' in desc:
#                 return "IPG"

#         return None

#     def preprocess(self):
#         # Ensure numeric columns are cleaned
#         for df in [self.bank_df, self.soa_df]:
#             if 'Amount' in df.columns:
#                 df['Amount'] = (
#                     df['Amount']
#                     .astype(str)
#                     .str.replace(',', '')
#                     .str.strip()
#                 )
#                 df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

#         super().preprocess()
