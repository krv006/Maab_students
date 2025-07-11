import sys
from pathlib import Path

# Get current script's directory
current_dir = Path(__file__).resolve().parent
# Go up one level to project root
project_root = current_dir.parent
# Add project root to Python path
sys.path.append(str(project_root))

from cleaning_1c.shayana_1c_cleaning import Processing_1c_source
from common.config_handler import config
import pandas as pd
from common.error_handler import error_manager, ExpectedCustomError

def process_all_tasks():
    #-RUNNING 1C FIRST
    error_manager.log_info("Processing `Shayana_1c`...")
    
    shayana = Processing_1c_source()
    shayana.process()
    
    # LOADING 1C DF
    # Read and process data
    df = pd.read_excel(config.path_for_1c_pivoted, header=[0,1],index_col=[0,1,2])

    # unpivot table
    shayana_df = df.stack(0,future_stack=True).reset_index()
    # rename columns
    shayana_df.columns = [config.client_header_name, config.oblast_header_name, config.address_header_name, config.drugs_header_name, config.quantity_header_name, config.total_sales_header_name]
    # drop qty total sales nan values
    shayana_df = shayana_df[~(shayana_df[[config.quantity_header_name, config.total_sales_header_name]].isna().all(axis=1))]
    # add month_year columns
    shayana_df[config.date_header_name] = None
    shayana_df[config.reserve_header_name] = None
    # Cleaning and formatting
    shayana_df[config.address_header_name] = shayana_df[config.address_header_name].astype(str).str.strip()
    shayana_df[config.oblast_header_name] = shayana_df[config.oblast_header_name].astype(str).str.strip()
    shayana_df[config.drugs_header_name] = shayana_df[config.drugs_header_name].astype(str).str.strip()
    shayana_df[config.client_header_name] = shayana_df[config.client_header_name].astype(str).str.strip()
    shayana_df[config.date_header_name] = pd.to_datetime(shayana_df[config.date_header_name], errors='coerce',format='%d.%m.%Y')
    
    for col in [config.quantity_header_name, config.total_sales_header_name]:
        shayana_df[col] = (
            shayana_df[col]
            .replace('', '0')
            .astype(float)
            .fillna(0)
        )

    # price column
    shayana_df[config.price_header_name] = shayana_df.apply(
    lambda row: row[config.total_sales_header_name] / row[config.quantity_header_name]
    if row[config.quantity_header_name] != 0 else 0,
    axis=1
    )
    shayana_df = shayana_df[[config.drugs_header_name, config.client_header_name, config.oblast_header_name, config.address_header_name, config.quantity_header_name, config.price_header_name, config.reserve_header_name, config.date_header_name]]
    
    # shayana_df.to_excel(config.path_for_1c_optovik, index= False, sheet_name= config.sheet_name_1c)
    with pd.ExcelWriter(config.path_for_1c_optovik, engine='openpyxl') as writer:
        # Write the DataFrame
        shayana_df.to_excel(writer, index=False, sheet_name=config.sheet_name_1c)

        # Get access to the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets[config.sheet_name_1c]

        # Define your custom number format
        custom_format = '_(* #,##0.00_);_(* -#,##0.00_);_(* "-"??_);_(@_)'

        # Apply format to numeric cells (excluding header)
        for col_idx in [5, 6]:  # 1-based index for Excel
            for row_idx in range(2, shayana_df.shape[0] + 2):  # Skip header
                cell = worksheet.cell(row=row_idx, column=col_idx)
                if isinstance(cell.value, (int, float)):  # Optional: only apply to numeric
                    cell.number_format = custom_format

    error_manager.log_info(f"  Workbook saved as: {config.path_for_1c_optovik.name}")

    pivoted_path = Path(config.path_for_1c_pivoted)

    if pivoted_path.exists():
        pivoted_path.unlink()
        error_manager.log_info("  The temporary File deleted.")
    else:
        error_manager.log_info("The temporary File not found.")

def main():
    try:
        process_all_tasks()
        error_manager.log_complete(f"ALL 1c CLEANING PROCESSES COMPLETED SUCCESSFULY.")
    except ExpectedCustomError as e:
        error_manager.log_error(e)
    except Exception as e:
        error_manager.log_exception(e)

if __name__ == "__main__":
    main()