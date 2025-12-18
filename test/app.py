import streamlit as st
from reconciler_bank_machha import ReconcilerMachha
from reconciler_bank_nabil import ReconcilerNabil
from reconciler_bank_prime import ReconcilerPrime
from reconciler_bank_adbl import ReconcilerAdbl
from reconciler_bank_shangrila import ReconcilerShangrila
from reconciler_bank_manjushree import ReconcilerManjushree
from reconciler_bank_kamana import ReconcilerKamana
from reconciler_bank_prabhu import ReconcilerPrabhu
from reconciler_bank_siddhartha import ReconcilerSiddhartha
from reconciler_bank_nic import ReconcilerNic
from reconciler_bank_icfc import ReconcilerICFC
from reconciler_bank_green import ReconcilerGreen
from reconciler_bank_mahalaxmi import ReconcilerMahalaxmi


# Streamlit configuration
st.set_page_config(page_title="Reconciliation App", layout="centered")
st.title("📊 Bank vs SOA Reconciliation")

# --- User Inputs ---

# Dropdown to select the bank
bank_choice = st.selectbox("Choose Bank", ["-- Select --", "ADBL","Green","ICFC","Kamana","Machha","Mahalaxmi","Manjushree", "Nabil","NIC","Prabhu","Prime","Shangrila","Siddhartha"])

# File upload for Bank Statement and SOA
bank_file = st.file_uploader("Upload Bank Statement", type=["xlsx"])
soa_file = st.file_uploader("Upload SOA Report", type=["xlsx"])

# --- Logic to run reconciliation ---

if bank_choice != "-- Select --" and bank_file and soa_file:
    if st.button("Run Reconciliation"):
        try:
            # Select the appropriate reconciler class based on the bank chosen
            if bank_choice == "Machha":
                reconciler_class = ReconcilerMachha
            elif bank_choice == "Nabil":
                reconciler_class = ReconcilerNabil
            elif bank_choice == "ADBL":
                reconciler_class = ReconcilerAdbl
            elif bank_choice == "Shangrila":
                reconciler_class = ReconcilerShangrila
            elif bank_choice == "Manjushree":
                reconciler_class = ReconcilerManjushree
            elif bank_choice == "Kamana":
                reconciler_class = ReconcilerKamana
            elif bank_choice == "Prabhu":
                reconciler_class = ReconcilerPrabhu
            elif bank_choice == "Siddhartha":
                reconciler_class = ReconcilerSiddhartha
            elif bank_choice == "NIC":
                reconciler_class = ReconcilerNic
            elif bank_choice == "ICFC":
                reconciler_class = ReconcilerICFC
            elif bank_choice == "Green":
                reconciler_class = ReconcilerGreen
            elif bank_choice == "Mahalaxmi":
                reconciler_class = ReconcilerMahalaxmi
            else:
                reconciler_class = ReconcilerPrime

            # Initialize the reconciler and run the reconciliation process
            reconciler = reconciler_class(bank_file, soa_file)
            res = reconciler.run_all()

            # Save reconciliation results in session_state
            st.session_state["recon_result"] = res

            st.success("✅ Reconciliation Complete!")
            # st.write(res["summary"])  # You can also display a summary here if needed

        except Exception as e:
            st.error(f"Error during reconciliation: {e}")
else:
    st.info("Please select a bank and upload both files.")

# --- Display the download buttons ---

if "recon_result" in st.session_state:
    res = st.session_state["recon_result"]
    st.download_button("Summary Excel", res["summary_excel"], "reconciliation_summary.xlsx")
    st.download_button("Download Matched", res["matched"], "matched.xlsx")
    st.download_button("Download Unmatched Bank", res["unmatched_bank"], "unmatched_bank.xlsx")
    st.download_button("Download Unmatched SOA", res["unmatched_soa"], "unmatched_soa.xlsx")

    # Additional: Display a summary of the reconciliation
    st.write("### Reconciliation Summary:")
    st.write(res["summary"])

else:
    st.info("Run the reconciliation to generate the results.")



# to run this main need to use: streamlit run app.py or if not in same diractory streamlit run c:/Users/rosha/Downloads/test/test/app.py
# to ryn it as server streamlit run your_app.py --server.address=0.0.0.0 access through http://<your-local-IP>:8501


