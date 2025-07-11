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
import numpy as np
from collections import defaultdict

class MapSplitRegion():
    def __init__(self, data_manager):
        self.dm = data_manager

        self.optiviks_container = {}
        self.main_df = None

        self.regionvise_dfs = defaultdict(list)
        self.teritoryvise_dfs = defaultdict(lambda: defaultdict(list))
        self.concated_by_regions_dfs = {}
        self.concated_by_teritories_dfs = defaultdict(dict)

        self.last_row_indexes = defaultdict(dict)

        self.gr_name_empty_dfs = {}

        self.list_for_outlining_columns = []

    def load_and_prepare_data(self):
        for sheet,optovik_df in self.dm.pivoted_optoviks.items():
            optovik_df = self._emtpy_row_adding_top(optovik_df,sheet)
            if sheet in config.reserve_column_values_list:
                self._finding_last_row_index(optovik_df,sheet,sheet)
            self._finding_last_row_index(optovik_df,config.vtorichka_sheet_name,sheet)

            self.optiviks_container[sheet] = optovik_df


    def _emtpy_row_adding_top(self,df,sheet):
        # 1. Create an empty row (all values NaN)
        empty_row = pd.DataFrame([[np.nan] * len(df.columns)],columns=df.columns)
        # 2. Concatenate it above the original DataFrame
        df = pd.concat([empty_row, df], ignore_index=True)
        # 3. Write the sheet name in the newly inserted first row
        df.loc[0, (config.main_header_name, config.client_header_name)] = sheet
        return df

    def _finding_last_row_index(self,df,sheet_name,optovik, teritory=False):
        last_index = df.index[-1]
        if teritory:
            # self.dm.add_last_row_indexes(f"{sheet_name}_|_",optovik,last_index)
            self.last_row_indexes[f"{sheet_name}_|_"][optovik] = last_index
        else:
            # self.dm.add_last_row_indexes(sheet_name,optovik,last_index)
            self.last_row_indexes[sheet_name][optovik] = last_index

    def split_dfs_by_region(self):
        for optovik, optivik_df in self.optiviks_container.items():
            if optovik in config.reserve_column_values_list:
                continue
            
            # FIND ALL REGIONS
            regions = optivik_df[(config.main_header_name, config.region_header_name)].dropna().unique()
            for region in regions:
                # SEPARATE EACH REGION
                mask = optivik_df[(config.main_header_name, config.region_header_name)] == region
                region_df = optivik_df[mask]

                region_df_final = self._emtpy_row_adding_top(region_df,optovik)
                self._finding_last_row_index(region_df_final,region,optovik)

                # STORE IT
                self.regionvise_dfs[region].append(region_df_final)

                # SPLIT by TERRITORY
                
                teritories = region_df[(config.main_header_name, config.territory_header_name)].dropna().unique()
                for teritory in teritories:
                    t_mask = region_df[(config.main_header_name, config.territory_header_name)] == teritory
                    teritory_df = region_df[t_mask]

                    teritory_df = self._emtpy_row_adding_top(teritory_df, optovik)
                    self._finding_last_row_index(teritory_df,teritory,optovik, teritory=True)

                    # store
                    self.teritoryvise_dfs[region][teritory].append(teritory_df)

    def concat_dataframes(self):
        # CONCAT ALL main DATAFRAMES
        self.main_df = pd.concat(self.optiviks_container.values())
        self.concated_by_regions_dfs[config.vtorichka_sheet_name] = self.main_df
        
        
        for optovik in self.optiviks_container.keys():
            if optovik in config.reserve_column_values_list:
                reservs_df = self.optiviks_container[optovik].reindex(columns=self.main_df.columns)
                self.concated_by_regions_dfs[optovik] = reservs_df

        # CONCAT REGIONWISE DATAFRAMES
        for region, optovik_list in self.regionvise_dfs.items():
            self.concated_by_regions_dfs[region] = pd.concat(optovik_list)
        
        # CONCAT teritory DATAFRAMES
        for region_t, teritory_dict in self.teritoryvise_dfs.items():
            for teritory, df_list  in teritory_dict.items():
                self.concated_by_teritories_dfs[region_t][teritory] = pd.concat(df_list)

    def add_empty_drug_group_columns(self,dataframe):
        for gr_nm in self.dm.drug_groups_df.columns:
            new_col = pd.DataFrame({
                (gr_nm, config.quantity_header_name): [np.nan],
                (gr_nm, config.total_sales_header_name): [np.nan]
            })
            self.gr_name_empty_dfs[gr_nm] = new_col

        # Concatenate horizontally to form one row
        groups = pd.concat(self.gr_name_empty_dfs.values(), axis=1)

        dataframe = pd.concat([dataframe, groups], axis=1)
        return dataframe

    def reorder_columns_by_drug_grouping(self,dataframe):
        # re order columns
        new_order = list(dataframe.columns[:3]) #insert headers first
        columns_to_check = dataframe.columns[3:] 
        for gr_name, gr_df in self.gr_name_empty_dfs.items():
            # add the group column
            new_order.extend([
                col for col in list(gr_df.columns)
            ])
            # add drug names that depends above grup
            new_order.extend([
                col for col in columns_to_check if col[0] in list(self.dm.drug_groups_df[gr_name])
            ])
            
        self.list_for_outlining_columns = [i for i in self.gr_name_empty_dfs.keys()]
        
        # others check
        if len(new_order) != len(dataframe.columns):
            others_df = pd.DataFrame({
                (config.ungroup_drugs_header_name, config.quantity_header_name): [np.nan],
                (config.ungroup_drugs_header_name, config.total_sales_header_name): [np.nan]
            })
            dataframe = pd.concat([dataframe, others_df], axis=1)

            new_order.extend([(config.ungroup_drugs_header_name, config.quantity_header_name), (config.ungroup_drugs_header_name, config.total_sales_header_name)])

            for item in dataframe.columns:
                if item not in new_order:
                    new_order.append(item)
            self.list_for_outlining_columns.append(config.ungroup_drugs_header_name)

        # apply replacment
        dataframe = dataframe[new_order]

        # total column
        total_df = pd.DataFrame({
            (config.total_header_name, config.quantity_header_name): [np.nan],
            (config.total_header_name, config.total_sales_header_name): [np.nan]
        })

        dataframe = pd.concat([dataframe, total_df], axis=1)
        return dataframe


if __name__ == "__main__":
    print("You can't run it from here")


