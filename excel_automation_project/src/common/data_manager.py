from collections import defaultdict

class DataManager:
    def __init__(self):
        self.mapped_optoviks = {}
        self.drug_name_dict = {}

        self.pivoted_optoviks = {}

        self.final_raw_optoviks = {}

        self.last_row_indexes = defaultdict(dict)
        
        self.total_sheet_data = defaultdict(list)

        self.all_regions_list = set()
        self.all_teritories_list = set()

        self.budget_dict = None
        self.drug_groups_df = None
        self.drug_groups_df_melted = None

        
    def add_mapped_optovik(self, sheet_name, dataframe):
        self.mapped_optoviks[sheet_name] = dataframe
        
    def add_dictionary(self, sheet_name, dictionary):
        self.drug_name_dict[sheet_name] = dictionary

    def add_budget_dict(self, budget_dict):
        self.budget_dict = budget_dict

    def add_drug_groups_df(self, drug_groups_df):
        self.drug_groups_df = drug_groups_df

    def add_drug_groups_df_melted(self, drug_groups_df_melted):
        self.drug_groups_df_melted = drug_groups_df_melted

    def add_final_raw_optoviks(self, sheet_name, dictionary):
        self.final_raw_optoviks[sheet_name] = dictionary
        
    def add_pivoted_optovik(self, sheet_name, dictionary):
        self.pivoted_optoviks[sheet_name] = dictionary
        
        
    # Add other data management methods as needed
    def add_last_row_indexes(self, sheet_name, optovik,value):
        self.last_row_indexes[sheet_name][optovik] = value

    def add_total_sheet_data(self, sheet_name, value, extend=False):
        if extend:
            self.total_sheet_data[sheet_name].extend(value)
        else:
            self.total_sheet_data[sheet_name].append(value)


    def add_all_regions_list(self, value):
        self.all_regions_list.add(value)

    def add_all_teritories_list(self, value):
        self.all_teritories_list.add(value)