# ayec-data-validation-script
Validation scripts for checking CSV files against expected schemas, column types, and data constraints.

## Installation

Install required dependencies:

```bash
pip install -r requirements.txt
```

## Scripts

### `util.py`

A helper file for loading and manipulating data


### `validate_data.py`

Validates all CSV files in a folder against their schema definitions.

**Usage:**
```bash
python validate_data.py <folder_path> [--sample-size N]
```

**Examples:**
```bash
# Validate all CSV files in a folder
python validate_data.py /path/to/csv/folder

# Validate with custom sample size
python validate_data.py /path/to/csv/folder --sample-size 5000
```

**Options:**
- `folder_path`: Path to folder containing CSV files (required)
- `--sample-size`: Number of rows to sample from each CSV (default: 10000)

**Output:**
- Validates each CSV file found in the folder
- Provides a summary report showing passed/failed files
- Exits with code 0 if all files pass, code 1 if any fail

## Supported Tables

The scripts validate the following Africa data tables:

- `africa_rural_urban`
- `africa_employed_employment_type`
- `total_working_population`
- `africa_education_inactive`
- `africa_education_student`
- `africa_education_unemployed`
- `employed_education_by_sector`
- `employed_working_poor`
- `africa_sector_employed`
- `employed_education`
- `employed_formality_status`
- `africa_employed_sector_group_income`
- `subnational_student`
- `subnational_unemployed`
- `subnational_inactive`
- `subnational_employed`
- `subnational_employed_working_poor`
- `subnational_employed_sector_group_income`
- `subnational_employed_employment_type`

## Validation Checks

The scripts perform the following validations:

1. **Missing Columns**: Checks that all required columns are present
2. **Column Types**: Validates data types (Int64, Float64, String)
3. **Nullable Constraints**: Ensures non-nullable columns don't contain nulls
4. **Categorical Values**: Validates `sector_group` contains only: `Industry`, `Agriculture`, `Services`
5. **Non-empty Strings**: Checks that required string columns are not empty
6. **Integer Validation**: Ensures integer columns contain valid integers

## Notes

- CSV filenames should match table names (e.g., `africa_rural_urban.csv`)
- Only the first N rows (default: 10,000) are sampled for validation to conserve memory
- Files with unrecognized table names are skipped with a warning
