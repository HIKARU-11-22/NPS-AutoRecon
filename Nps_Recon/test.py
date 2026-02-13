import pandas as pd

# Sample dataframe
df = pd.DataFrame({
    'Reference No': ['S53737956', 'S53738259'],
    'Desc2': ['', 'S53737956/16-12-2025'],
    'extracted_id': ['15517797', None]
})

# Step 1: Clean Reference No
df['Reference No'] = df['Reference No'].str.strip().str.upper()

# Step 2: Build mapping Reference No -> extracted_id
ref_id_map = df.loc[df['extracted_id'].notna() & df['Reference No'].notna(),
                    ['Reference No', 'extracted_id']].set_index('Reference No')['extracted_id'].to_dict()

# Step 3: Extract pure reference from Desc2 (before '/')
df['ref_from_desc2'] = df['Desc2'].str.split('/').str[0].str.strip().str.upper()

# Step 4: Fill missing extracted_id
mask = df['extracted_id'].isna()
df.loc[mask, 'extracted_id'] = df.loc[mask, 'ref_from_desc2'].map(ref_id_map)

print(df)
