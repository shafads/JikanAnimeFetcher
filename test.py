import pandas as pd

# Sample data
data = {
    'category': ['A', 'A', 'B', 'B'],
    'subcategory': ['X', 'Y', 'X', 'Y'],
    'value': [10, 20, 30, 40]
}

# Create DataFrame
df = pd.DataFrame(data)

# Set 'category' and 'subcategory' as multi-level index
df = df.set_index(['category', 'subcategory'])

print(df)