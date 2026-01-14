import sys
import pandas as pd
from typing import List, Dict, Optional
import argparse
import numpy as np

def load_csv(file_path: str, sample_size: int = None) -> Optional[pd.DataFrame]:
    """Load CSV file with support for gzip compression"""
    try:
        # Check if file is gzip compressed
        is_gzipped = False
        try:
            with open(file_path, 'rb') as f:
                magic = f.read(2)
                is_gzipped = magic.startswith(b'\x1f\x8b')
        except:
            pass
        
        if is_gzipped:
            if sample_size:
                df = pd.read_csv(file_path, compression='gzip', nrows=sample_size)
            else:
                df = pd.read_csv(file_path, compression='gzip')
        else:
            if sample_size:
                df = pd.read_csv(file_path, nrows=sample_size)
            else:
                df = pd.read_csv(file_path)
        
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

def check_missing_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
    try:
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ Missing columns: {missing_columns}")
            return False
        print("✅ All required columns are present.")
        return True
    except Exception as e:
        print(f"Error checking missing columns: {e}")
        return False

def check_missing_values(df: pd.DataFrame) -> bool:
    missing_cols_stats = df.isnull().sum().sort_values(ascending=False)
    if df.isnull().values.any():
        print(f"There are missing values in the data. These are\n{missing_cols_stats}")
        return False
    print("No missing values found in the data.")
    return True

def check_scientific_numbers(df: pd.DataFrame) -> bool:
    float_columns = df.select_dtypes(include=['float']).columns
    found_scientific = False
    
    for col in float_columns:
        scientific_mask = df[col].apply(lambda x: 'e' in f"{x:.16E}" or 'E' in f"{x:.16E}")
        if scientific_mask.any():
            found_scientific = True
            print(f"Column '{col}' contains scientific notation numbers")
    
    if found_scientific:
        return False
    return True

def check_integer_columns(df: pd.DataFrame, columns: List[str]) -> bool:
    for col in columns:
        if col in df.columns:
            non_integers = df[~df[col].apply(lambda x: isinstance(x, (int, float)) and float(x).is_integer())]
            if not non_integers.empty:
                print(f"Column '{col}' contains non-integer values in the following rows:")
                print(non_integers)
                return False
    print("All integer columns contain only integer values.")
    return True

def check_year_range(df: pd.DataFrame, start_year: int, end_year: int) -> bool:
    year_range = range(start_year, end_year + 1)
    actual_years = set(df['year'].unique())
    missing_years = set(year_range) - actual_years
    
    if missing_years:
        print(f"The 'year' column does not contain the complete range from {start_year} to {end_year}.")
        print(f"Actual years in data: {sorted(actual_years)}")
        print(f"Missing years: {sorted(missing_years)}")
        return False
    print("All years in the range are present in the 'year' column.")
    return True

def check_data_types(df: pd.DataFrame, expected_types: Dict[str, type]) -> bool:
    try:
        invalid_columns = []
        for column, expected_type in expected_types.items():
            if column in df.columns:
                if not df[column].map(lambda x: isinstance(x, expected_type)).all():
                    invalid_columns.append((column, expected_type))
        
        if invalid_columns:
            for col, exp_type in invalid_columns:
                print(f"Column '{col}' contains values not of the expected type: {exp_type.__name__}")
            return False
        print("All columns have the expected data types.")
        return True
    except Exception as e:
        print(f"Error checking data types: {e}")
        return False

def check_valid_categorical_values(df: pd.DataFrame, categorical_values: Dict[str, List[str]]) -> bool:
    invalid_columns = []
    for column, valid_values in categorical_values.items():
        if column in df.columns:
            invalid_values = df[~df[column].isin(valid_values)]
            if not invalid_values.empty:
                invalid_columns.append((column, invalid_values))
    
    if invalid_columns:
        for col, invalid_vals in invalid_columns:
            print(f"Column '{col}' contains invalid categorical values. Expected {categorical_values[col]}. Invalid values found in rows:")
            print(invalid_vals)
        return False
    print("All categorical columns contain valid categorical values.")
    return True

def check_non_empty_strings(df: pd.DataFrame, string_columns: List[str]) -> bool:
    try:
        invalid_columns = []
        for column in string_columns:
            if column in df.columns:  # Only check columns that exist
                empty_strings = df[df[column].apply(lambda x: isinstance(x, str) and x.strip() == '')]
                if not empty_strings.empty:
                    invalid_columns.append((column, empty_strings))
        
        if invalid_columns:
            for col, invalid_vals in invalid_columns:
                print(f"Column '{col}' contains empty strings in the following rows:")
                print(invalid_vals)
            return False
        print("All string columns contain non-empty strings.")
        return True
    except Exception as e:
        print(f"Error checking non-empty strings: {e}")
        return False

def check_specific_format(df: pd.DataFrame, column: str, pattern: str) -> bool:
    try:
        incorrect_format = df[~df[column].str.match(pattern)]
        if not incorrect_format.empty:
            print(f"Column '{column}' does not match the required format in the following rows:")
            print(incorrect_format)
            return False
        print(f"All values in column '{column}' match the required format.")
        return True
    except Exception as e:
        print(f"Error checking specific format: {e}")
        return False

def check_column_order(df: pd.DataFrame, required_order: list) -> bool:
    try:
        if list(df.columns) != required_order:
            print("The columns are not in the required order.")
            print("Current order:", list(df.columns))
            print("Expected order:", required_order)
            return False
        print("The columns are in the required order.")
        return True
    except Exception as e:
        print(f"Error checking column order: {e}")
        return False

def check_value_ranges(df: pd.DataFrame, range_checks: Dict[str, Dict[str, float]]) -> bool:
    """Check if numeric columns are within expected ranges"""
    try:
        invalid_columns = []
        for column, ranges in range_checks.items():
            if column in df.columns:
                if 'min' in ranges:
                    below_min = df[df[column] < ranges['min']]
                    if not below_min.empty:
                        invalid_columns.append((column, f"values below minimum {ranges['min']}"))
                
                if 'max' in ranges:
                    # Handle inf values specially
                    if np.isinf(df[column]).any():
                        print(f"Column '{column}' contains infinite values (inf), which is expected for age ranges")
                    else:
                        above_max = df[df[column] > ranges['max']]
                        if not above_max.empty:
                            invalid_columns.append((column, f"values above maximum {ranges['max']}"))
        
        if invalid_columns:
            for col, issue in invalid_columns:
                print(f"Column '{col}' contains {issue}")
            return False
        print("All numeric columns are within expected ranges.")
        return True
    except Exception as e:
        print(f"Error checking value ranges: {e}")
        return False
