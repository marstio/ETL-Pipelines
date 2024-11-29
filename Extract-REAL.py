import pandas as pd
import numpy as np

# File paths
product_list_path = r'Business_Department\product_list.xlsx'
user_credit_card_path = r'Customer_Management_Department\user_credit_card.pickle'
user_data_path = r'Customer_Management_Department\user_data.json'
user_job_path = r'Customer_Management_Department\user_job.csv'

# Load the datasets
df_product_list = pd.read_excel(product_list_path)
df_user_credit_card = pd.read_pickle(user_credit_card_path)
df_user_data = pd.read_json(user_data_path)
df_user_job = pd.read_csv(user_job_path)

# Business Department Functions
def clean_product_list(df):
    # Drop the first column
    df = df.drop(df.columns[0], axis=1)
    
    # Clean product_id
    def clean_product_id(product_id):
        if isinstance(product_id, str):
            product_id = product_id.replace(" ", "").replace("-", "").upper()
            product_id = 'P' + product_id[7:]
        return product_id

    df['product_id'] = df['product_id'].apply(clean_product_id)

    # Get duplicate product IDs
    duplicate_product_ids = df[df.duplicated(['product_id'], keep=False)]

    # Generate new unique IDs for duplicate product_id entries
    def generate_unique_id(existing_ids):
        max_id = max([int(id[1:]) for id in existing_ids if id.startswith('P')])
        new_id = f'P{max_id + 1:05d}'
        return new_id

    existing_ids = set(df['product_id'])
    for index, row in duplicate_product_ids.iterrows():
        new_id = generate_unique_id(existing_ids)
        df.at[index, 'product_id'] = new_id
        existing_ids.add(new_id)

    # Clean product_name
    def clean_product_name(product_name):
        return product_name.strip().title()

    df['product_name'] = df['product_name'].apply(clean_product_name)
    
    return df

def clean_product_type(df):
    # Fill null values in product_type with 'Unknown'
    df['product_type'] = df['product_type'].fillna('Unknown')

    # Custom function to capitalize words except "and"
    def capitalize_except_and(product_type):
        words = product_type.split()
        capitalized_words = [word.capitalize() if word.lower() != 'and' else word.lower() for word in words]
        return ' '.join(capitalized_words)

    df['product_type'] = df['product_type'].apply(capitalize_except_and)
    
    return df

def standardize_price(df):
    df['price'] = df['price'].round(2)
    return df

# Customer Department Functions
def clean_user_credit_card(df):
    # Clean user_id
    def clean_user_id(user_id):
        if isinstance(user_id, str):
            user_id = user_id.replace(" ", "").replace("-", "").upper()
            user_id = 'U' + user_id[1:]
        return user_id

    df['user_id'] = df['user_id'].apply(clean_user_id)

    # Clean name
    def clean_name(name):
        if isinstance(name, str):
            name = name.strip().title()
            name = name.replace(".", ". ").replace("-", "- ")
        return name

    df['name'] = df['name'].apply(clean_name)
    
    return df

def clean_user_data(df):
    # Clean user_id
    def clean_user_id(user_id):
        if isinstance(user_id, str):
            user_id = user_id.replace(" ", "").replace("-", "").upper()
            user_id = 'U' + user_id[1:]
        return user_id

    df['user_id'] = df['user_id'].apply(clean_user_id)

    # Standardize creation_date and birthdate to YYYY-MM-DDTHH:MM:SS format
    def standardize_date(date):
        try:
            return pd.to_datetime(date).strftime('%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return 'Invalid Data'

    df['creation_date'] = df['creation_date'].apply(standardize_date)
    df['birthdate'] = df['birthdate'].apply(standardize_date)

    # Clean name
    def clean_name(name):
        if isinstance(name, str):
            name = name.strip().title()
            name = name.replace(".", ". ").replace("-", "- ")
        return name

    df['name'] = df['name'].apply(clean_name)

    # Capitalize country names
    df['country'] = df['country'].apply(lambda x: x.upper() if isinstance(x, str) else x)
    
    return df

def clean_user_job(df):
    # Drop the first column
    df = df.drop(df.columns[0], axis=1)

    # Clean user_id
    def clean_user_id(user_id):
        if isinstance(user_id, str):
            user_id = user_id.replace(" ", "").replace("-", "").upper()
            user_id = 'U' + user_id[1:]
        return user_id

    df['user_id'] = df['user_id'].apply(clean_user_id)

    # Clean name
    def clean_name(name):
        if isinstance(name, str):
            name = name.strip().title()
            name = name.replace(".", ". ").replace("-", "- ")
        return name

    df['name'] = df['name'].apply(clean_name)

    return df

# Function to check for duplicates in any DataFrame
def check_and_resolve_duplicates(df, subset, keep='first'):
    # Identify duplicates
    # duplicates = df[df.duplicated(subset=subset, keep=False)]
    # print(f"\nDuplicates in {subset}:")
    # print(duplicates)

    # Resolve duplicates by keeping the specified occurrence
    df = df.drop_duplicates(subset=subset, keep=keep)
    return df

# Apply Business Department Functions
df_product_list = clean_product_list(df_product_list)
df_product_list = clean_product_type(df_product_list)
df_product_list = standardize_price(df_product_list)

# Apply Customer Department Functions
df_user_credit_card = clean_user_credit_card(df_user_credit_card)
df_user_data = clean_user_data(df_user_data)
df_user_job = clean_user_job(df_user_job)

# Check and resolve duplicates
df_product_list = check_and_resolve_duplicates(df_product_list, subset=['product_name'])
df_user_credit_card = check_and_resolve_duplicates(df_user_credit_card, subset=['credit_card_number', 'issuing_bank'])
df_user_data = check_and_resolve_duplicates(df_user_data, subset=['user_id'])
df_user_job = check_and_resolve_duplicates(df_user_job, subset=['user_id'])

# Check for duplicates
print("\nDuplicate Product IDs:")
print(df_product_list[df_product_list.duplicated(['product_id'], keep=False)])

print("\nDuplicate User IDs in Credit Card Data:")
print(df_user_credit_card[df_user_credit_card.duplicated(['user_id'], keep=False)])

print("\nDuplicate User IDs in User Data:")
print(df_user_data[df_user_data.duplicated(['user_id'], keep=False)])

print("\nDuplicate User IDs in User Job Data:")
print(df_user_job[df_user_job.duplicated(['user_id'], keep=False)])


# Verify the changes
print("\nProduct List after standardization:")
print(df_product_list.head(10))

print("\nUser Credit Card after standardization:")
print(df_user_credit_card.head(10))

print("\nUser Data after standardization:")
print(df_user_data.head(10))

print("\nUser Job after standardization:")
print(df_user_job.head(10))