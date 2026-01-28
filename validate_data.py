import sys
import pandas as pd
from typing import List, Dict, Optional
import argparse
import numpy as np
import os
import glob

# Import validation functions from app.py
from util import (
    load_csv, check_missing_columns, check_integer_columns, check_non_empty_strings
)


# Schema configurations for all Africa tables
SCHEMAS = {
    'africa_rural_urban': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'status', 'urban_rural', 'share', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'status': str,
            'urban_rural': str,
            'share': float,  # Nullable
            'population': int
        },
        'nullable_columns': ['share']
    },
    'africa_employed_employment_type': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'education', 'sector_group', 'main_job_type', 'sector', 'status', 'urban_rural', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,
            'sector_group': str,  # Nullable
            'main_job_type': str,  # Nullable
            'sector': str,  # Nullable
            'status': str,
            'urban_rural': str,  # Nullable
            'population': int
        },
        'nullable_columns': ['sector_group', 'main_job_type', 'sector', 'urban_rural'],
        'categorical_values': {
            'sector_group': ['Industry', 'Agriculture', 'Services']
        }
    },
    'total_working_population': {
        'required_columns': ['ccode', 'country', 'year', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'population': int  # Nullable
        },
        'nullable_columns': ['population']
    },
    'africa_education_inactive': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'education', 'reason_inactive', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,  # Nullable
            'reason_inactive': str,
            'status': str,
            'population': int
        },
        'nullable_columns': ['education'],
        'categorical_values': {
            'reason_inactive': ['childcare/pregnancy', 'discouraged', 'health-related', 'homemaker', 'other']
        }
    },
    'africa_education_student': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'education', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,
            'status': str,
            'population': int
        },
        'nullable_columns': []
    },
    'africa_education_unemployed': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'education', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,
            'status': str,
            'population': int
        },
        'nullable_columns': []
    },
    'employed_education_by_sector': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'education',  'sector_group', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,
            'sector_group': str,  # Nullable
            'status': str,
            'population': int
        },
        'nullable_columns': ['sector_group'],
        'categorical_values': {
            'sector_group': ['Industry', 'Agriculture', 'Services']
        }
    },
    'employed_working_poor': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'status_poor', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'status_poor': str,
            'status': str,
            'population': int
        },
        'nullable_columns': [],
        'categorical_values': {
            'status_poor': ['Extremely poor < USD 2.15 PPP', 'Moderately poor >= USD 2.15 and < USD 3.65 PPP', 'Not poor >= USD 3.65 PPP']
        }
    },
    'africa_sector_employed': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'sector', 'sector_group', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'sector': str,
            'sector_group': str,
            'status': str,
            'population': int
        },
        'nullable_columns': [],
        'categorical_values': {
            'sector_group': ['Industry', 'Agriculture', 'Services']
        }
    },
    'employed_education': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'education', 'sector_group', 'informal_formal', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,
            'sector_group': str,
            'informal_formal': str,
            'status': str,
            'population': int
        },
        'nullable_columns': []
    },
    'employed_formality_status': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'sector_group', 'informal_formal', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'sector_group': str,
            'informal_formal': str,
            'status': str,
            'population': int
        },
        'nullable_columns': []
    },
    'africa_employed_sector_group_income': {
        'required_columns': ['ccode', 'country', 'year', 'age', 'gender', 'sector_group', 'status', 'median_annual_income'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'year': int,
            'age': int,
            'gender': str,
            'sector_group': str,  # Nullable
            'status': str,
            'median_annual_income': float
        },
        'nullable_columns': ['sector_group'],
        'categorical_values': {
            'sector_group': ['Industry', 'Agriculture', 'Services']
        }
    },
    'subnational_student': {
        'required_columns': ['ccode', 'country', 'region', 'year', 'age', 'gender', 'education', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'region': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,
            'status': str,
            'population': int
        },
        'nullable_columns': []
    },
    'subnational_unemployed': {
        'required_columns': ['ccode', 'country', 'region', 'year', 'age', 'gender', 'education', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'region': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,
            'status': str,
            'population': int
        },
        'nullable_columns': []
    },
    'subnational_inactive': {
        'required_columns': ['ccode', 'country', 'region', 'year', 'age', 'gender', 'education', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'region': str,
            'year': int,
            'age': int,
            'gender': str,
            'education': str,
            'status': str,
            'population': int
        },
        'nullable_columns': []
    },
    'subnational_employed': {
        'required_columns': ['ccode', 'country', 'region', 'year', 'age', 'gender', 'sector', 'sector_group', 'status', 'informal_formal', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'region': str,
            'year': int,
            'age': int,
            'gender': str,
            'sector': str,  # Nullable
            'sector_group': str,  # Nullable
            'status': str,
            'informal_formal': str,  # Nullable
            'population': int
        },
        'nullable_columns': ['sector', 'sector_group', 'informal_formal']
    },
    'subnational_employed_working_poor': {
        'required_columns': ['ccode', 'country', 'region', 'year', 'age', 'gender', 'status_poor', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'region': str,
            'year': int,
            'age': int,
            'gender': str,
            'status_poor': str,
            'status': str,
            'population': int
        },
        'nullable_columns': []
    },
    'subnational_employed_sector_group_income': {
        'required_columns': ['ccode', 'country', 'region', 'year', 'age', 'gender', 'sector_group', 'status', 'median_annual_income'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'region': str,
            'year': int,
            'age': int,
            'gender': str,
            'sector_group': str,  # Nullable
            'status': str,
            'median_annual_income': float
        },
        'nullable_columns': ['sector_group']
    },
    'subnational_employed_employment_type': {
        'required_columns': ['ccode', 'country', 'region', 'year', 'age', 'gender', 'sector_group', 'main_job_type', 'status', 'population'],
        'expected_types': {
            'ccode': str,
            'country': str,
            'region': str,
            'year': int,
            'age': int,
            'gender': str,
            'sector_group': str,  # Nullable
            'main_job_type': str,  # Nullable
            'status': str,
            'population': int
        },
        'nullable_columns': ['sector_group', 'main_job_type']
    }
}


def get_table_name_from_filename(csv_file: str) -> Optional[str]:
    """Extract table name from CSV filename"""
    filename = os.path.basename(csv_file)
    # Remove .csv extension
    table_name = filename.replace('.csv', '')
    return table_name if table_name in SCHEMAS else None


def check_column_types(df: pd.DataFrame, expected_types: Dict[str, type], nullable_cols: List[str]) -> bool:
    """Check if columns have the expected types, handling nullable columns appropriately"""
    all_valid = True
    
    for col_name, expected_type in expected_types.items():
        if col_name not in df.columns:
            continue
        
        # For nullable columns, check non-null values only
        if col_name in nullable_cols:
            non_null_values = df[col_name].dropna()
            if len(non_null_values) > 0:
                if expected_type == int:
                    try:
                        pd.to_numeric(non_null_values, errors='raise').astype('Int64')
                    except (ValueError, TypeError):
                        print(f"  ❌ Column '{col_name}' contains non-integer values (nullable column)")
                        all_valid = False
                elif expected_type == float:
                    try:
                        pd.to_numeric(non_null_values, errors='raise').astype(float)
                    except (ValueError, TypeError):
                        print(f"  ❌ Column '{col_name}' contains non-float values (nullable column)")
                        all_valid = False
                elif expected_type == str:
                    non_strings = non_null_values[~non_null_values.apply(lambda x: isinstance(x, str))]
                    if len(non_strings) > 0:
                        print(f"  ❌ Column '{col_name}' contains non-string values (nullable column)")
                        all_valid = False
        else:
            # For non-nullable columns, check all values
            if expected_type == int:
                try:
                    pd.to_numeric(df[col_name], errors='raise').astype('Int64')
                except (ValueError, TypeError):
                    print(f"  ❌ Column '{col_name}' contains non-integer values")
                    all_valid = False
            elif expected_type == float:
                try:
                    pd.to_numeric(df[col_name], errors='raise').astype(float)
                except (ValueError, TypeError):
                    print(f"  ❌ Column '{col_name}' contains non-float values")
                    all_valid = False
            elif expected_type == str:
                non_strings = df[~df[col_name].apply(lambda x: isinstance(x, str) or pd.isna(x))]
                if len(non_strings) > 0:
                    print(f"  ❌ Column '{col_name}' contains non-string values")
                    all_valid = False
    
    if all_valid:
        print("  ✅ All column types are valid")
    
    return all_valid


def check_nullable_columns(df: pd.DataFrame, nullable_cols: List[str], non_nullable_cols: List[str]) -> bool:
    """Check that non-nullable columns don't have nulls"""
    all_valid = True
    
    for col in non_nullable_cols:
        if col not in df.columns:
            continue
        null_count = df[col].isnull().sum()
        if null_count > 0:
            print(f"  ❌ Column '{col}' is non-nullable but contains {null_count} null values")
            all_valid = False
    
    if all_valid:
        print("  ✅ Nullable/non-nullable constraints are satisfied")
    
    return all_valid


def check_categorical_values_with_nullable(df: pd.DataFrame, categorical_values: Dict[str, List[str]], nullable_cols: List[str]) -> bool:
    """Check categorical values, handling nullable columns appropriately"""
    if not categorical_values:
        return True
    
    all_valid = True
    
    for column, valid_values in categorical_values.items():
        if column not in df.columns:
            continue
        
        # For nullable columns, only check non-null values
        if column in nullable_cols:
            non_null_df = df[df[column].notna()]
            if len(non_null_df) > 0:
                invalid_values = non_null_df[~non_null_df[column].isin(valid_values)]
                if not invalid_values.empty:
                    print(f"  ❌ Column '{column}' contains invalid categorical values (nullable column)")
                    print(f"     Expected: {valid_values}")
                    unique_invalid = invalid_values[column].unique()
                    print(f"     Found invalid values: {list(unique_invalid)}")
                    all_valid = False
        else:
            # For non-nullable columns, check all values
            invalid_values = df[~df[column].isin(valid_values)]
            if not invalid_values.empty:
                print(f"  ❌ Column '{column}' contains invalid categorical values")
                print(f"     Expected: {valid_values}")
                unique_invalid = invalid_values[column].unique()
                print(f"     Found invalid values: {list(unique_invalid)}")
                all_valid = False
    
    if all_valid and categorical_values:
        print("  ✅ All categorical columns contain valid values")
    
    return all_valid


def validate_africa_csv(file_path: str, table_name: str, sample_size: int = 100000) -> bool:
    """Validate an Africa CSV file against its schema"""
    print(f"\n{'='*80}")
    print(f"Validating: {table_name}")
    print(f"File: {file_path}")
    print(f"{'='*80}")
    
    # Get schema configuration
    schema = SCHEMAS.get(table_name)
    if not schema:
        print(f"  ❌ Unknown table name: {table_name}")
        print(f"  Available tables: {', '.join(SCHEMAS.keys())}")
        return False
    
    # Load CSV sample
    print(f"\n  Loading sample data (first {sample_size} rows)...")
    df = load_csv(file_path, sample_size=sample_size)
    if df is None:
        return False
    
    print(f"  ✅ Loaded {len(df)} rows, {len(df.columns)} columns")
    
    required_columns = schema['required_columns']
    expected_types = schema['expected_types']
    nullable_cols = schema['nullable_columns']
    categorical_values = schema.get('categorical_values', {})
    non_nullable_cols = [col for col in required_columns if col not in nullable_cols]
    string_columns = [col for col in required_columns if expected_types[col] == str]
    
    # Run validations
    tests = []
    
    # 1. Check missing columns
    print(f"\n  Checking for missing columns...")
    tests.append(check_missing_columns(df, required_columns))
    
    # 2. Check for extra columns (fail if found)
    print(f"\n  Checking for extra columns...")
    extra_columns = [col for col in df.columns if col not in required_columns]
    if extra_columns:
        print(f"  ❌ Found extra columns not in schema: {extra_columns}")
        tests.append(False)
    else:
        print("  ✅ No extra columns found")
        tests.append(True)
    
    # 3. Check nullable constraints
    print(f"\n  Checking nullable constraints...")
    tests.append(check_nullable_columns(df, nullable_cols, non_nullable_cols))
    
    # 4. Check column types
    print(f"\n  Checking column types...")
    tests.append(check_column_types(df, expected_types, nullable_cols))
    
    # 5. Check categorical values if defined
    if categorical_values:
        print(f"\n  Checking categorical values...")
        tests.append(check_categorical_values_with_nullable(df, categorical_values, nullable_cols))
    
    # 6. Check non-empty strings for non-nullable string columns
    non_nullable_string_cols = [col for col in string_columns if col in non_nullable_cols]
    if non_nullable_string_cols:
        print(f"\n  Checking non-empty strings...")
        tests.append(check_non_empty_strings(df, non_nullable_string_cols))
    
    # 7. Check integer columns for non-nullable integer columns
    non_nullable_int_cols = [col for col in non_nullable_cols 
                            if expected_types[col] == int and col in df.columns]
    if non_nullable_int_cols:
        print(f"\n  Checking integer columns...")
        tests.append(check_integer_columns(df, non_nullable_int_cols))
    
    # Summary
    all_passed = all(tests)
    
    if all_passed:
        print(f"\n  ✅ '{table_name}' passed all validation checks!")
    else:
        print(f"\n  ❌ '{table_name}' failed some validation checks!")
    
    return all_passed


def find_csv_files(folder_path: str) -> List[str]:
    """Find all CSV files in the specified folder"""
    # Support both .csv and .CSV extensions
    csv_patterns = [
        os.path.join(folder_path, "*.csv"),
        os.path.join(folder_path, "*.CSV")
    ]
    
    csv_files = []
    for pattern in csv_patterns:
        csv_files.extend(glob.glob(pattern))
    
    # Remove duplicates and sort
    csv_files = sorted(list(set(csv_files)))
    return csv_files


def check_all_required_files_exist(folder_path: str) -> bool:
    """Check that all 19 required CSV files exist in the data folder"""
    print("\n" + "="*80)
    print("Checking for existence of all required files")
    print("="*80)
    
    # Get all expected table names from SCHEMAS
    expected_tables = list(SCHEMAS.keys())
    expected_files = [f"{table_name}.csv" for table_name in expected_tables]
    
    missing_files = []
    found_files = []
    
    for table_name in expected_tables:
        expected_file = f"{table_name}.csv"
        file_path = os.path.join(folder_path, expected_file)
        
        # Check for both .csv and .CSV extensions
        if os.path.exists(file_path) or os.path.exists(os.path.join(folder_path, expected_file.upper())):
            found_files.append(expected_file)
        else:
            missing_files.append(expected_file)
    
    print(f"\nExpected files: {len(expected_files)}")
    print(f"Found files: {len(found_files)}")
    print(f"Missing files: {len(missing_files)}")
    
    if missing_files:
        print(f"\n❌ Missing required files:")
        for missing_file in missing_files:
            print(f"   - {missing_file}")
        return False
    else:
        print(f"\n✅ All {len(expected_files)} required files are present")
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validate all Africa CSV files in a folder against schema definitions"
    )
    parser.add_argument(
        "folder_path",
        help="Path to the folder containing CSV files to validate"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100000,
        help="Number of rows to sample from each CSV for validation (default: 100000)"
    )
    
    args = parser.parse_args()
    
    # Check if folder exists
    if not os.path.isdir(args.folder_path):
        print(f"❌ Error: '{args.folder_path}' is not a valid directory")
        sys.exit(1)
    
    # Check for existence of all required files
    all_files_exist = check_all_required_files_exist(args.folder_path)
    if not all_files_exist:
        print("\n❌ Error: Not all required files are present. Proceeding with available files.")
    
    # Find all CSV files in the folder
    csv_files = find_csv_files(args.folder_path)
    
    if not csv_files:
        print(f"❌ No CSV files found in folder: {args.folder_path}")
        sys.exit(1)
    
    print("\n" + "="*80)
    print("Africa CSV Files Batch Validation")
    print("="*80)
    print(f"Folder: {args.folder_path}")
    print(f"Found {len(csv_files)} CSV file(s)")
    print(f"Sample size: {args.sample_size} rows per file")
    print("="*80)
    
    # Validate each CSV file
    results = []
    skipped_files = []
    
    for csv_file in csv_files:
        # Determine table name from filename
        table_name = get_table_name_from_filename(csv_file)
        
        if not table_name:
            filename = os.path.basename(csv_file)
            print(f"\n⚠️  Skipping '{filename}': Could not determine table name")
            print(f"   Available tables: {', '.join(SCHEMAS.keys())}")
            skipped_files.append(csv_file)
            continue
        
        # Validate the CSV file
        try:
            is_valid = validate_africa_csv(csv_file, table_name, args.sample_size)
            results.append((csv_file, table_name, is_valid))
        except Exception as e:
            print(f"\n❌ Error validating {csv_file}: {e}")
            import traceback
            traceback.print_exc()
            results.append((csv_file, table_name, False))
    
    # Summary
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)
    
    passed = [r for r in results if r[2]]
    failed = [r for r in results if not r[2]]
    
    print(f"\nTotal files processed: {len(results)}")
    print(f"✅ Passed: {len(passed)}")
    print(f"❌ Failed: {len(failed)}")
    
    if skipped_files:
        print(f"⚠️  Skipped: {len(skipped_files)}")
    
    if passed:
        print(f"\n✅ Passed files:")
        for csv_file, table_name, _ in passed:
            filename = os.path.basename(csv_file)
            print(f"   - {filename} ({table_name})")
    
    if failed:
        print(f"\n❌ Failed files:")
        for csv_file, table_name, _ in failed:
            filename = os.path.basename(csv_file)
            print(f"   - {filename} ({table_name})")
    
    if skipped_files:
        print(f"\n⚠️  Skipped files:")
        for csv_file in skipped_files:
            filename = os.path.basename(csv_file)
            print(f"   - {filename}")
    
    print("="*80)
    
    # Exit with error code if any files failed
    if failed:
        sys.exit(1)
    else:
        sys.exit(0)
