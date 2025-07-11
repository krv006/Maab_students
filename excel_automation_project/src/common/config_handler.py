import json
from pathlib import Path
from typing import Dict
import sys
from common.error_handler import error_manager, ExpectedCustomError

class ConfigHandler:
    _instance = None
    _config: Dict = None
    
    def __new__(cls):
        if not cls._instance:
            try:
                # Detect EXE or script mode
                if getattr(sys, 'frozen', False):
                    cls._project_root = Path(sys.executable).parent
                else:
                    cls._project_root = Path(__file__).resolve().parents[2]

                config_file_path = cls._project_root / "config" / "config.json"

                if not config_file_path.exists():
                    raise ExpectedCustomError(
                        f"\n❌ Configuration file not found: {config_file_path}"
                        f"\n\n📌 HOW TO FIX:\n"
                        f"1. Make sure the file 'config.json' exists in the 'config' folder inside the project directory.\n"
                        f"2. If missing, restore or recreate the file using a valid template.\n"
                        f"3. Folder path expected: {config_file_path.parent}\n"
                        f"4. File path expected: {config_file_path}\n"
                    )

                cls._instance = super(ConfigHandler, cls).__new__(cls)
                cls._load_config(str(config_file_path))
                cls._ensure_data_structure()

            except ExpectedCustomError:
                raise  # already formatted, re-raise
            except Exception as e:
                raise ExpectedCustomError(
                    f"\n❌ Unexpected error while initializing configuration: {e}"
                    f"\n\n📌 Please check that your project folder structure and permissions are correct."
                ) from e

        return cls._instance
    
    @classmethod
    def _load_config(cls, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                cls._config = json.load(f)
        except FileNotFoundError:
            raise RuntimeError(f"Config file not found at {config_path}")
        except json.JSONDecodeError:
            raise ExpectedCustomError(
                f"❌ Invalid JSON format in config file: {e}\n"
                f"\n📌 How to fix:\n"
                f"- Check for syntax errors in 'config.json'."
            )
        
    @classmethod
    def _ensure_data_structure(cls):
        # Define the base 'data' folder
        data_folder = cls._project_root / "data"
        data_folder.mkdir(exist_ok=True)  # Create 'data' if it doesn't exist
        # Loop through and ensure each subfolder exists
        for sub in ["prepared", "to_fix", "final", "1c_files"]:
            (data_folder / sub).mkdir(exist_ok=True)  # Create if missing

    def _get_path(self, key: str) -> Path:
        raw = self._config.get(key)
        if not raw:
            raise ExpectedCustomError(
                f"❌ Config key '{key}' missing or empty in config.json.\n"
                f"\n📌 HOW TO FIX:\n"
                f"1. Open 'config/config.json'.\n"
                f"2. Add or set '{key}' to a valid relative path string.\n"
                f"3. Save and restart the application."
            )
        return self._project_root / raw
        

    # DATABASE CONNECTION
    @property
    def database_conn_string(self) -> str:
        conn = self._config.get("database_connection_string")
        if not conn:
            raise ExpectedCustomError(
                f"❌ 'database_connection_string' not set in config.json.\n"
                f"\n📌 HOW TO FIX:\n"
                f"1. Add 'database_connection_string' with your DB URI.\n"
            )
        return conn
    
    # 1c path
    @property
    def source_path_for_1c(self) -> Path:
        return self._get_path("source_path_for_1c")
    @property
    def path_for_1c_pivoted(self) -> Path:
        return self._get_path("path_for_1c_pivoted")
    @property
    def path_for_1c_optovik(self) -> Path:
        return self._get_path("path_for_1c_optovik")
    @property
    def sheet_name_1c(self) -> str:
        return self._config.get("1c_data_sheet_name", "Shayana")
    
    # PATHS
    @property
    def source_path_for_optiviks(self) -> Path:
        return self._get_path("optiviks_source_path")

    @property
    def source_path_for_dictionary(self) -> Path:
        return self._get_path("dictionary_path")

    @property
    def path_for_vtorichka(self) -> Path:
        return self._get_path("path_for_vtorichka")

    @property
    def source_path_drug_groups(self) -> Path:
        return self._get_path("source_path_drug_groups")
    
    @property
    def path_for_regions_manual_correction(self) -> Path:
        return self._get_path("regions_manual_correction")
    
    @property
    def regions_to_be_corrected(self) -> Path:
        return self._get_path("regions_to_be_corrected")
    
    @property
    def duplicate_clients(self) -> Path:
        return self._get_path("duplicate_clients")

    @property
    def path_for_region_js(self) -> Path:
        return self._get_path("path_for_region_js")
    
    @property
    def path_for_teritory_js(self) -> Path:
        return self._get_path("path_for_teritory_js")

    @property
    def path_for_budget_difference(self) -> Path:
        return self._get_path("budget_difference_path")
    
    @property
    def path_for_unmatched_drugs_txt(self) -> Path:
        return self._get_path("path_for_unmatched_drugs_txt")

    @property
    def path_for_po_gorodom(self) -> Path:
        base_path = Path(self._get_path("path_for_po_gorodom"))
        folder_path = base_path / "po_gorodom"
        folder_path.mkdir(parents=True,exist_ok=True)
        return folder_path

# //////////////////////////////////////

    def _get_value(self, key: str, description: str) -> str:
        val = self._config.get(key)
        if val is None:
            raise ExpectedCustomError(
                f"\n❌ Config key '{key}' for {description} is missing.\n"
                f"\n📌 HOW TO FIX:\n"
                f"- Add '{key}' in config.json with a valid string."
            )
        return val
    
    @property
    def database_region_match_bool(self) -> bool:
        return self._get_value("region_matching_from_database", "boolean value for the database matching to fill regions")

    @property
    def reserve_column_values_list(self) -> list:
        return self._config.get("reserve_column_values_list")


    @property
    def vtorichka_sheet_name(self) -> str:
        return self._config.get("vtorichka_sheet_name", "Вторичка")
    
    @property
    def main_header_name(self) -> str:
        return self._config.get("main_header_name_for_client_info", "Данные клиентов")
    
    @property
    def client_header_name(self) -> str:
        return self._config.get("header_name_for_clients", "Клиент")
    
    @property
    def region_header_name(self) -> str:
        return self._config.get("header_name_for_region", "Регион")
    
    @property
    def territory_header_name(self) -> str:
        return self._config.get("header_name_for_territory", "Территори")
    
    @property
    def quantity_header_name(self) -> str:
        return self._config.get("header_name_for_quantity", "Количество")
    
    @property
    def total_sales_header_name(self) -> str:
        return self._config.get("header_name_for_total_sales", "Сумма продажи")
    
    @property
    def total_header_name(self) -> str:
        return self._config.get("header_name_for_total", "Итого")
    
    @property
    def drugs_header_name(self) -> str:
        return self._config.get("header_name_for_drugs", "Препарат")
    
    @property
    def price_header_name(self) -> str:
        return self._config.get("header_name_for_price", "Сумма с наценкой")
    @property
    def reserve_header_name(self) -> str:
        return self._config.get("header_name_for_reserve", "Резерв")
    
    @property
    def date_header_name(self) -> str:
        return self._config.get("header_name_date", "Дата")
    
    @property
    def ungroup_drugs_header_name(self) -> str:
        return self._config.get("header_name_for_ungroup_drugs", "Others")
        
    @property
    def oblast_header_name(self) -> str:
        return self._config.get("header_nmae_for_oblast", "Область")
    
    @property
    def address_header_name(self) -> str:
        return self._config.get("header_nmae_for_address", "Адрес")


    # drug name dictionary  
    @property
    def dict_cust_drug_names_index(self) -> int:
        return self._config.get("dictionary_customer_drug_names_column", 1) - 1 #df is 0 based
    
    @property
    def dict_std_drug_names_index(self) -> int:
        return self._config.get("dictionary_standart_drug_names_column", 2) - 1 #df is 0 based
    
    @property
    def cust_drug_names(self) -> str:
        return self._config.get("dictionary_cust_drug_names", "cust_drug_names")
    
    @property
    def std_drug_names(self) -> str:
        return self._config.get("dictionary_std_drug_names", "std_drug_names")


    # optiviks column index
    @property
    def optivik_drug_col_index(self) -> int:
        return self._config.get("optivik_drug_col", 1) - 1 #df is 0 based
    @property
    def optivik_customer_col_index(self) -> int:
        return self._config.get("optivik_customer_col", 2) - 1 #df is 0 based
    @property
    def optivik_region_col_index(self) -> int:
        return self._config.get("optivik_region_col", 3) - 1 #df is 0 based
    @property
    def optivik_territory_col_index(self) -> int:
        return self._config.get("optivik_territory_col", 4) - 1 #df is 0 based
    @property
    def optivik_quantity_col_index(self) -> int:
        return self._config.get("optivik_quantity_col", 5) - 1 #df is 0 based
    @property
    def optivik_price_col_index(self) -> int:
        return self._config.get("optivik_price_col", 6) - 1 #df is 0 based
    @property
    def optivik_reserve_col_index(self) -> int:
        return self._config.get("reserve_text_col", 7) - 1 #df is 0 based
    @property
    def optivik_month_year_col_index(self) -> int:
        return self._config.get("optivik_month_year_col",  8) - 1 #df is 0 based
    

    @property
    def budget_dif_drugs_col_index(self) -> int:
        return self._config.get("budget_difference_drugs_col", 1) - 1 #df is 0 based
    @property
    def budget_dif_vtorichka_col_index(self) -> int:
        return self._config.get("budget_difference_vtorichka_col", 2) - 1 #df is 0 based
    @property
    def budget_dif_by_region_index(self) -> int:
        return self._config.get("budget_difference_by_region", 3) - 1 #df is 0 based
    

    @property
    def budg_dif_drugs(self) -> str:
        return self._config.get("budg_dif_drugs", "dif_drugs")
    @property
    def budg_dif_vtorich(self) -> str:
        return self._config.get("budg_dif_vtorich", "by_vtorichka")
    @property
    def budg_dif_by_reg(self) -> str:
        return self._config.get("budg_dif_by_reg", "by_region")
    


    
    @property
    def final_sum_minus(self) -> int:
        return 100 - self._config.get("final_sum_minus_percent", 10) 

    @property
    def final_sum_leksiya(self) -> int:
        return self._config.get("final_sum_for_leksiya_percent", 2)


    # FINAL SUM 
    @property
    def final_sum(self) -> str:
        return self._config.get("final_sum", "FINAL SUM")
    @property
    def final_sum_minus10(self) -> str:
        return self._config.get("final_sum_minus10", "FINAL SUM ( Minus 10 % )")
    @property
    def final_sum_reklama(self) -> str:
        return self._config.get("final_sum_reklama", "Final summa for Reklama")
    @property
    def final_sum_leksiya_text(self) -> str:
        return self._config.get("final_sum_leksiya", "Final summa for Leksiya")
    
try:
    config = ConfigHandler()
except ExpectedCustomError as e:
    error_manager.log_error(e)
except Exception as e:
    error_manager.log_exception(e)



