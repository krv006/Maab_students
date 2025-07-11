import sys
from pathlib import Path
# Get current script's directory
current_dir = Path(__file__).resolve().parent
# Go up one level to project root
project_root = current_dir.parent
# Add project root to Python path
sys.path.append(str(project_root))

import pandas as pd
from common.config_handler import config

class SalesPivotReporter():

    def create_pivot_table(self,merged_df):
        """Create formatted pivot table"""
        try:
            pivoted_df = merged_df.pivot_table(
                index=[config.client_header_name,config.region_header_name,config.territory_header_name],
                columns=config.drugs_header_name,
                values=[config.quantity_header_name, config.total_sales_header_name],
                aggfunc='sum'
                # fill_value=0
            )
            return pivoted_df
        except Exception as e:
            raise RuntimeError(f"create_pivot_table method failed: {e}")

    def table_manipulation(self,optovik_df):
        # Flatten and order columns
        try:
            columns = []
            for drug in optovik_df.columns.get_level_values(1).unique():
                columns.extend([(config.quantity_header_name, drug), ( config.total_sales_header_name, drug)])
            optovik_df = optovik_df.reindex(columns=columns).reset_index()
            optovik_df.columns = optovik_df.columns.swaplevel(0, 1)

            # rename main headers
            current_columns = list(optovik_df.columns.values)
            new_columns = [ (config.main_header_name, config.client_header_name), 
                            (config.main_header_name, config.region_header_name), 
                            (config.main_header_name, config.territory_header_name)
            ]
            current_columns[:3] = new_columns
            optovik_df.columns = pd.MultiIndex.from_tuples(current_columns)
            return optovik_df
        except Exception as e:
            raise RuntimeError(f"table_manipulation method failed: {e}")
if __name__ == "__main__":
    #-PROCESSING

    print("You Can't run from heare")
    