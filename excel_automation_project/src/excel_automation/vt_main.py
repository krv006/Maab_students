import sys
from pathlib import Path
# Get current script's directory
current_dir = Path(__file__).resolve().parent
# Go up one level to project root
project_root = current_dir.parent
# Add project root to Python path
sys.path.append(str(project_root))

from common.error_handler import error_manager, ExpectedCustomError
from common.config_handler import config
from excel_automation.sales_pivot_reporter import SalesPivotReporter
from excel_automation.map_split_region import MapSplitRegion
from excel_automation.outlining_apply_formula import OutlineAndFormulas
from excel_automation.stage1_2_3_process import StagesProcesses
from common.data_manager import DataManager
from common.drug_mapping_validator import DrugMappingValidator
from common.region_teritory_finder import TerritoryHandler
import pandas as pd


def process_all_tasks():
    data_manager = DataManager()
    #-VALIDATION 
    validator = DrugMappingValidator(data_manager, False)    
    validator.process_all()
    error_manager.log_info("Validation processes completed successfully.\n")

    # REGION AND TERITORY FINDING
    # load teritory json
    error_manager.log_info("Region and territory identification process...")
    territory_handler = TerritoryHandler(data_manager)
    region_mapping = territory_handler._load_region_mapping(config.path_for_region_js)
    territory_patterns = territory_handler._compile_territory_patterns(config.path_for_teritory_js)

    for sheet,optovik_df in data_manager.mapped_optoviks.items():
        # Process territories 
        client_data = optovik_df[[config.client_header_name, config.region_header_name, config.territory_header_name]].copy()
        territory_processed = territory_handler.region_territory_writer(client_data,region_mapping,territory_patterns,sheet)
        
        optovik_df[[config.client_header_name, config.region_header_name, config.territory_header_name]] = territory_processed

        # STORE DATA
        data_manager.add_final_raw_optoviks(sheet,optovik_df)

    # AFTER PROCESSING ALL DATAFRAMES: Extract any still-missing values
    has_missing = TerritoryHandler.extract_all_missing_values(data_manager.final_raw_optoviks)
    if has_missing:
        raise ExpectedCustomError(
            
        "\n⚠️ WARNING: Some territories are still missing after automated filling."
        "\n\n📁 A file has been created at:"
        f"\n{config.path_for_regions_manual_correction}"
        "\n\nThis file contains the list of clients with missing Region or Territory information."
        "\n\n📌 HOW TO FIX:"
        "\n1. Open the Excel file above and manually fill in the missing **Region** and **Territory** values for each client."
        f"\n2. Save the file with the following exact name: {config.regions_to_be_corrected}"
        "\n3. Move the file to the 'prepared' folder inside your project directory: /data/prepared/"
        "\n4. After completing the above steps, re-run the script."

        "\n\nOnce these corrections are applied, the script will continue processing automatically."
        )
    else:
        error_manager.log_info("All territories successfully processed with no missing values\n")


    # PIVOT TABLE
    error_manager.log_info("Transforming Tables into pivot tabls...")
    reporter = SalesPivotReporter()
    for sheet, optovik_df in data_manager.final_raw_optoviks.items():
        pivoted_df = reporter.create_pivot_table(optovik_df)
        final_df = reporter.table_manipulation(pivoted_df)
        # STORE DATA
        data_manager.add_pivoted_optovik(sheet, final_df)
    error_manager.log_info("  Completed.\n")

# //////////////////////////////
    # IMPORTANT STEP
        
    error_manager.log_info("Creating a file with separate sheets for each region...")
    step_one = MapSplitRegion(data_manager)
    step_one.load_and_prepare_data()
    step_one.split_dfs_by_region()
    step_one.concat_dataframes()

    # Save main vtorichka to Excel
    with pd.ExcelWriter(config.path_for_vtorichka, engine='openpyxl') as writer:
        for sheet, df in step_one.concated_by_regions_dfs.items():
            # Process DataFrame
            new_df = step_one.add_empty_drug_group_columns(df)
            reordered_df = step_one.reorder_columns_by_drug_grouping(new_df)
            # Write to Excel
            reordered_df.to_excel(writer, sheet_name=sheet, index=True)
            # Get the openpyxl worksheet object
            ws = writer.sheets[sheet]
            step_two = OutlineAndFormulas(step_one.last_row_indexes, step_one.list_for_outlining_columns, data_manager)
            # Apply modifications
            ws = step_two.apply_outline_to_customers(ws, sheet)
            ws = step_two.apply_outline_to_drugs(ws)
            ws = step_two.apply_excel_functions(ws, sheet,total_sheet=True)
            step_two.apply_formatting(ws)

        # TOTAL SHEET DATA
        custom_ws = writer.book.create_sheet(title="Total")
        step_two.total_sheet_writer(custom_ws)
        # Reorder sheets - put Total sheet as first sheet,
        sheets = writer.book._sheets
        total_sheet = writer.book['Total']
        sheets.remove(total_sheet)
        sheets.insert(0, total_sheet)
        writer.book._sheets = sheets

    error_manager.log_info(f"  Workbook saved as: {config.path_for_vtorichka.name}\n")


    error_manager.log_info("Exporting Vtorichka data into individual files by region, with separate sheets for each territory...")
    for region, teritory_dict in step_one.concated_by_teritories_dfs.items():
        with pd.ExcelWriter(config.path_for_po_gorodom / f"{region}.xlsx", engine='openpyxl') as writer:
            # FIRST SAVE EACH REGION TO AN EXCEL FILE
            region_df = step_one.concated_by_regions_dfs[region]

            proces_df = step_one.add_empty_drug_group_columns(region_df)
            proces_df = step_one.reorder_columns_by_drug_grouping(proces_df)

            # SAVE IT
            proces_df.to_excel(writer, sheet_name=region, index=True)
            ws = writer.sheets[region]
            step_two = OutlineAndFormulas(step_one.last_row_indexes, step_one.list_for_outlining_columns, data_manager)
            # Apply modifications
            ws = step_two.apply_outline_to_customers(ws, region)
            ws = step_two.apply_outline_to_drugs(ws)
            ws = step_two.apply_excel_functions(ws, region)
            step_two.apply_formatting(ws)

            if len(teritory_dict.keys()) < 2:
                continue

            for teritory, df in teritory_dict.items():
                proces_df = step_one.add_empty_drug_group_columns(df)
                proces_df = step_one.reorder_columns_by_drug_grouping(proces_df)
                proces_df.to_excel(writer, sheet_name=teritory)
                # SAVE IT
                proces_df.to_excel(writer, sheet_name=teritory, index=True)
                ws = writer.sheets[teritory]
                step_two = OutlineAndFormulas(step_one.last_row_indexes, step_one.list_for_outlining_columns,data_manager)
                # Apply modifications
                ws = step_two.apply_outline_to_customers(ws, f"{teritory}_|_")
                ws = step_two.apply_outline_to_drugs(ws)
                ws = step_two.apply_excel_functions(ws, teritory)
                step_two.apply_formatting(ws)
    error_manager.log_info(f"  All Workbooks saved in: {config.path_for_po_gorodom}\n")

    # //////////////test////////////////

    error_manager.log_info("Exporting Stage files...")
    staging = StagesProcesses(data_manager)
    staging.run_stage()
    error_manager.log_info(f"  Stage files saved.\n")

    # ////////////////////////////////

def main():
    try:
        process_all_tasks()
        error_manager.log_complete(f"ALL VTORICHKA CREATION PROCESSES COMPLETED SUCCESSFULY.")
    except ExpectedCustomError as e:
        error_manager.log_error(e)
    except Exception as e:
        error_manager.log_exception(e)

if __name__ == "__main__":
    main()
