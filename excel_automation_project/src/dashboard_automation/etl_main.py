import sys
from pathlib import Path

# Get current script's directory
current_dir = Path(__file__).resolve().parent
# Go up one level to project root
project_root = current_dir.parent
# Add project root to Python path
sys.path.append(str(project_root))

from common.config_handler import config
from common.error_handler import error_manager, ExpectedCustomError
from common.data_manager import DataManager
from common.drug_mapping_validator import DrugMappingValidator
from common.region_teritory_finder import TerritoryHandler
from dashboard_automation.new_database_etl import SalesDataWarehouse

def process_all_tasks():
    data_manager = DataManager()
    #-VALIDATION 
    validator = DrugMappingValidator(data_manager, True)    
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

    # DATABASE INSERT ETL

    error_manager.log_info("Populating the database with new data ...")
    db_insert = SalesDataWarehouse()
    db_insert.run_etl(data_manager.final_raw_optoviks,data_manager.drug_groups_df_melted)


def main():
    try:
        process_all_tasks()
        error_manager.log_complete(f"ALL ETL PROCESSES COMPLETED SUCCESSFULY.")
    except ExpectedCustomError as e:
        error_manager.log_error(e)
    except Exception as e:
        error_manager.log_exception(e)

if __name__ == "__main__":
    main()
