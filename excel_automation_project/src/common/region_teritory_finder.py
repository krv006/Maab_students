import sys
from pathlib import Path
# Get current script's directory
current_dir = Path(__file__).resolve().parent
# Go up one level to project root
project_root = current_dir.parent
# Add project root to Python path
sys.path.append(str(project_root))

import pandas as pd
import re
import json
from common.config_handler import config
from collections import defaultdict
import unicodedata
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from common.error_handler import error_manager, ExpectedCustomError

class TerritoryHandler:
    def __init__(self,data_manager):
        self.dm = data_manager
        self.region_col = config.region_header_name
        self.territory_col = config.territory_header_name
        self.database_match_for_regions = config.database_region_match_bool
        self.connection_string = config.database_conn_string

        # DATABASE CONNECTION
        if self.database_match_for_regions:
            try:
                self.engine = create_engine(self.connection_string, echo=False)

                with self.engine.connect() as conn:
                    result = conn.execute(text("SELECT TOP 1 * FROM dim_customer"))
                    row = result.fetchone()
                    if row:
                        error_manager.log_info("Database connection successful, and 'dim_customer' contains data.")
                        self.use_database_to_map = True
                    else:
                        error_manager.log_info("⚠️Database Connection successful, but 'dim_customer' table is empty.")
                        self.use_database_to_map = False
            except SQLAlchemyError as e:
                self.use_database_to_map = False
                error_manager.log_info("  ❌ Database Connection failed. Code can not fill region and teritory through database.")
                error_manager.log_info("Error details:", str(e))
                
    def _load_region_mapping(self, path):
        """Precompute region normalization mapping"""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                region_data = json.load(f)

            mapping = {}
            for region, variations in region_data.items():
                for v in variations:
                    norm_v = self._normalize_text(v)
                    mapping[norm_v] = region.strip()
            return mapping

        except FileNotFoundError:
            raise ExpectedCustomError(
                f"\n❌ Region mapping file not found: {path}\n\n"
                "📌 HOW TO FIX:\n"
                "1. Ensure the file exists at the path above.\n"
                "2. Check that the file is named correctly (case-sensitive).\n"
                "3. Restore the file if it was deleted or moved."
            )
        except json.JSONDecodeError as e:
            raise ExpectedCustomError(
                f"\n❌ Invalid JSON format in file: {path}\n"
                f"📍 Error: {e.msg} at line {e.lineno}, column {e.colno}\n\n"
                "📌 HOW TO FIX:\n"
                "1. Open the file and fix any formatting errors (e.g., commas, brackets).\n"
                "2. You can validate it online: https://jsonlint.com"
            )
        except UnicodeDecodeError:
            raise ExpectedCustomError(
                f"\n❌ Encoding error while reading file: {path}\n\n"
                "📌 HOW TO FIX:\n"
                "1. Ensure the file is saved with UTF-8 encoding.\n"
                "2. Re-save it using Notepad, VS Code, or another editor with UTF-8 selected."
            )
        except PermissionError:
            raise ExpectedCustomError(
                f"\n❌ Permission denied when trying to open file: {path}\n\n"
                "📌 HOW TO FIX:\n"
                "1. Close the file if it is open in Excel or another program.\n"
                "2. Make sure you have read access to the file."
            )
        except Exception as e:
            raise ExpectedCustomError(
                f"\n❌ Unexpected error while loading region mapping file: {path}\n"
                f"💥 {str(e)}"
            ) from e

    def _compile_territory_patterns(self, path):
        """Precompile regex patterns for territory matching"""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                territory_data = json.load(f)

            patterns = defaultdict(lambda: defaultdict(list))
            for region_in_regions, region_d in territory_data.items():
                for region, teritories_d in region_d.items():
                    self.dm.add_all_regions_list(region)
                    for territory, variations_list in teritories_d.items():
                        self.dm.add_all_teritories_list(territory)
                        for v_list in variations_list:
                            for v in v_list:
                                norm_v = self._normalize_text(v)
                                pattern = re.compile(r"\b" + r"\s+".join(map(re.escape, norm_v.split())) + r"\b", flags=re.IGNORECASE)
                                patterns[region_in_regions][region].append((pattern, territory))
            return patterns

        except FileNotFoundError:
            raise ExpectedCustomError(
                f"\n❌ Territory mapping file not found: {path}\n\n"
                "📌 HOW TO FIX:\n"
                "1. Ensure the file exists at the path above.\n"
                "2. Check that the file is named correctly.\n"
                "3. Restore the file if it was deleted or moved."
            )
        except json.JSONDecodeError as e:
            raise ExpectedCustomError(
                f"\n❌ Invalid JSON format in file: {path}\n"
                f"📍 Error: {e.msg} at line {e.lineno}, column {e.colno}\n\n"
                "📌 HOW TO FIX:\n"
                "1. Open the file and fix JSON errors (commas, braces, etc.).\n"
                "2. You can test the file on https://jsonlint.com"
            )
        except UnicodeDecodeError:
            raise ExpectedCustomError(
                f"\n❌ Encoding error while reading file: {path}\n\n"
                "📌 HOW TO FIX:\n"
                "1. Save the file in UTF-8 encoding using any text editor.\n"
                "2. Avoid using non-standard encodings."
            )
        except PermissionError:
            raise ExpectedCustomError(
                f"\n❌ Permission denied for file: {path}\n\n"
                "📌 HOW TO FIX:\n"
                "1. Ensure the file is not open in another app (like Excel).\n"
                "2. Check folder and file access permissions."
            )
        except Exception as e:
            raise ExpectedCustomError(
                f"\n❌ Unexpected error while loading territory patterns file: {path}\n"
                f"💥 {str(e)}"
            ) from e


    def _normalize_text(self, text):
        try:
            """Optimized text normalization, removing all ASCII and Unicode punctuation."""
            if isinstance(text, list):
                return [self._normalize_text(t) for t in text]
            
            # Strip whitespace and lowercase
            text = text.strip().lower()
            if text:
                # Normalize Unicode forms (e.g., compatibility characters) first
                text = unicodedata.normalize('NFKC', text)
                # Remove any character whose Unicode category starts with 'P' (all punctuation)
                # This catches ASCII punctuation and Unicode punctuation like ‘ ’ “ ” — etc.
                text = ''.join(
                    ch for ch in text
                    if not unicodedata.category(ch).startswith('P')
                )
            return text
        except Exception as e:
            raise RuntimeError(f"Region teritory finder Text normalization failed: {e}")


    def _fill_missings_from_database(self, main_dataframe, sheet):
        """
        Fills missing Region and Territory in the main_dataframe using dim_customer lookup table.
        Logs the number of regions and territories filled.
        """
        try:
            # Identify rows needing fill
            region_missing = main_dataframe[self.region_col].isna()
            territory_missing = main_dataframe[self.territory_col].isna()
            mask = region_missing | territory_missing

            # Collect unique clients needing fill
            region_nan_clients = main_dataframe.loc[mask, config.client_header_name]
            unique_keys = region_nan_clients.unique().tolist()

            # Chunk processing
            CHUNK_SIZE = 1000
            existing_chunks = []
            for start in range(0, len(unique_keys), CHUNK_SIZE):
                chunk = unique_keys[start:start + CHUNK_SIZE]
                placeholders = ','.join(f':val{i}' for i in range(len(chunk)))
                query = text(f"""
                    SELECT Customer, Region, Territory
                    FROM dim_customer
                    WHERE Customer IN ({placeholders})
                """)
                params = {f'val{i}': key for i, key in enumerate(chunk)}
                chunk_df = pd.read_sql_query(query, self.engine, params=params)
                existing_chunks.append(chunk_df)

            existing = pd.concat(existing_chunks, ignore_index=True) if existing_chunks else pd.DataFrame(columns=['Customer', 'Region', 'Territory'])

            # Filter out valid Region/Territory
            region_df = existing[existing['Region'].notna() & existing['Region'].str.strip().astype(bool)]
            territory_df = existing[existing['Territory'].notna() & existing['Territory'].str.strip().astype(bool)]

            client_region_map = region_df.drop_duplicates('Customer').set_index('Customer')['Region']
            client_territory_map = territory_df.drop_duplicates('Customer').set_index('Customer')['Territory']

            # Compute before counts
            before_region_count = region_missing.sum()
            before_territory_count = territory_missing.sum()

            # Fill missing Regions
            if region_missing.any():
                main_dataframe.loc[region_missing, self.region_col] = (
                    main_dataframe.loc[region_missing, config.client_header_name]
                    .map(client_region_map)
                )

            # Fill missing Territories
            if territory_missing.any():
                main_dataframe.loc[territory_missing, self.territory_col] = (
                    main_dataframe.loc[territory_missing, config.client_header_name]
                    .map(client_territory_map)
                )

            # Compute after counts
            filled_regions = before_region_count - main_dataframe[self.region_col].isna().sum()
            filled_territories = before_territory_count - main_dataframe[self.territory_col].isna().sum()

            # Log info
            error_manager.log_info(f"  {sheet}: Filled {filled_regions} regions and {filled_territories} territories FROM DATABASE.")

            return main_dataframe

        except Exception:
            error_manager.log_info(f"Error filling missing regions/territories for sheet {sheet}")
            raise
            

    def _fill_from_manual_correction(self, main_dataframe, sheet):
        """Fill missing regions/territories from manually corrected file if exists"""

        try:
            manual_df = pd.read_excel(config.regions_to_be_corrected)

            # 🔍 Required column names
            required_columns = [config.client_header_name, self.region_col, self.territory_col]
            # ❗ Check if all required columns exist
            missing_columns = [col for col in required_columns if col not in manual_df.columns]

            if missing_columns:
                raise ExpectedCustomError(
                    f"❌ Missing required column(s) in '{config.regions_to_be_corrected}': {', '.join(missing_columns)}\n\n"
                    "✔️ Make sure your Excel file contains the following column headers exactly:\n"
                    f"- {config.client_header_name}\n- {self.region_col}\n- {self.territory_col}\n\n"
                    "➡️ Tip: Check for typos, extra spaces, or formatting issues in the header row."
                )
            manual_df = pd.read_excel(config.regions_to_be_corrected)
            manual_df[config.client_header_name] = manual_df[config.client_header_name].astype(str).str.strip()
            manual_df[self.region_col] = manual_df[self.region_col].astype(str).str.strip()
            manual_df[self.territory_col] = manual_df[self.territory_col].astype(str).str.strip()

            region_mask = manual_df[self.region_col].isin(self.dm.all_regions_list)
            territory_mask = manual_df[self.territory_col].isin(self.dm.all_teritories_list)
            invalid_rows = manual_df[~region_mask | ~territory_mask]

            preview_rows = invalid_rows[[self.region_col, self.territory_col]].head(10).to_string(index=False)
            if not invalid_rows.empty:
                valid_regions = ", ".join(sorted(self.dm.all_regions_list))
                valid_territories = ", ".join(sorted(self.dm.all_teritories_list))

                raise ExpectedCustomError(
                    f"\n\n❌ Invalid region or territory names detected in '{config.regions_to_be_corrected.name}'.\n\n"
                    "Here are a few sample rows:\n"
                    f"{preview_rows}\n\n"
                    "✔️ Valid region names should match entries in your region JSON.\n"
                    "✔️ Valid territory names should match entries in your territory JSON.\n\n"
                    f"📌 Valid Regions: {valid_regions}\n"
                    f"📌 Valid Territories: {valid_territories}\n\n"
                    "➡️ Tip: Check for typos, extra spaces, or casing issues."
                )
            
            # Create mappings for regions and territories
            region_map = manual_df.set_index(config.client_header_name)[self.region_col].dropna().to_dict()
            territory_map = manual_df.set_index(config.client_header_name)[self.territory_col].dropna().to_dict()

            # Fill missing regions
            region_mask = main_dataframe[self.region_col].isna()
            main_dataframe.loc[region_mask, self.region_col] = (main_dataframe.loc[region_mask, config.client_header_name].map(region_map))
            
            # Fill missing territories
            territory_mask = main_dataframe[self.territory_col].isna()
            main_dataframe.loc[territory_mask, self.territory_col] = (main_dataframe.loc[territory_mask, config.client_header_name].map(territory_map))
            
            error_manager.log_info(f"  {sheet}: Filled {region_mask.sum()} regions and {territory_mask.sum()} territories from MANUAL CORRECTION")
            return main_dataframe
        except:
            raise

    
    def region_territory_writer(self,main_dataframe,region_mapping,territory_patterns,sheet):
        try:

            # Vectorized region processing
            main_dataframe[self.region_col] = (
                main_dataframe[self.region_col]
                .apply(lambda x: region_mapping.get(self._normalize_text(x), None))
                .astype(object)
            )
            
            # Vectorized territory processing
            def get_territory(row):
                region = row[self.region_col]
                address = row[self.territory_col]
                if pd.isna(region) or pd.isna(address) or region not in territory_patterns:
                    return (None, None)
                    
                norm_addr = self._normalize_text(address)
                for real_region, patterns in territory_patterns[region].items():
                    for pattern, territory_str in patterns:
                        if pattern.search(norm_addr):
                            return real_region, territory_str
                return (None, None)

            main_dataframe[['new_region', self.territory_col]] = (
                main_dataframe
                    .apply(get_territory, axis=1, result_type='expand')
            )

            # only overwrite self.region_col where new_region is not null
            mask = main_dataframe['new_region'].notna()
            main_dataframe.loc[mask, self.region_col] = main_dataframe.loc[mask, 'new_region']
            # drop the helper
            main_dataframe.drop(columns=['new_region'], inplace=True)

            # calculations 
            region_nan = main_dataframe[self.region_col].isna().sum()
            teritory_nan = main_dataframe[self.territory_col].isna().sum()
            row_count  = len(main_dataframe)

            filled_regions = row_count - region_nan
            filled_territories = row_count - teritory_nan

             # Log results
            error_manager.log_info(f"  {sheet}: Filled {filled_regions} regions and {filled_territories} territories VIA WRITER.")


            # Step 1: Fill missing from database
            if self.database_match_for_regions and self.use_database_to_map:
                main_dataframe = self._fill_missings_from_database(main_dataframe, sheet)
                
            # Step 2: Fill from manual corrections if available
            regions_path = Path(config.regions_to_be_corrected)
            if regions_path.exists():
                main_dataframe = self._fill_from_manual_correction(main_dataframe, sheet)

            return main_dataframe
        except Exception:
            raise
            
    @staticmethod
    def extract_all_missing_values(dataframes):
        try:
            def _get_next_versioned_filename(base_name, folder, extension):
                version = 1
                while True:
                    filename = f"{base_name}_v{version}.{extension}"
                    full_path = Path(folder) / filename
                    if not full_path.exists():
                        return full_path
                    version += 1
        
            """Extract ALL missing territories from ALL dataframes into single Excel file"""
            all_missing = []
            
            for sheet_name, df in dataframes.items():

                # Identify missing rows
                missing_region = df[config.region_header_name].isna()
                missing_territory = df[config.territory_header_name].isna()
                missing_mask = missing_region | missing_territory
                
                if missing_mask.any():
                    # Create missing data report
                    missing_df = df[missing_mask].copy()
                    missing_df = missing_df[[config.client_header_name, config.region_header_name, config.territory_header_name]]
                    
                    missing_df['Sheet'] = sheet_name
                    all_missing.append(missing_df)
            
            if all_missing:
                # Combine all missing records
                combined_missing = pd.concat(all_missing).drop_duplicates(subset=config.client_header_name)
                
                # make file name by assigning v1, v2
                folder = config.path_for_regions_manual_correction.parent
                base_name = config.path_for_regions_manual_correction.stem
                extension = config.path_for_regions_manual_correction.suffix.lstrip('.') 
                file_path = _get_next_versioned_filename(base_name, folder, extension)

                # Save to Excel
                file_path.parent.mkdir(parents=True, exist_ok=True)
                combined_missing.to_excel(file_path, index=False)
                error_manager.log_info(f"\nExtracted {len(combined_missing)} missing values to {file_path}")
                return True
            return False
        except Exception as e:
            raise RuntimeError(f"extract_all_missing_values method failed: {e}")


