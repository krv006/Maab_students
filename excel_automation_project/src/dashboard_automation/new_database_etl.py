import sys
from pathlib import Path

# Get current script's directory
current_dir = Path(__file__).resolve().parent
# Go up one level to project root
project_root = current_dir.parent
# Add project root to Python path
sys.path.append(str(project_root))

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import pandas as pd
from datetime import datetime
import hashlib
from common.config_handler import config
from common.error_handler import error_manager, ExpectedCustomError

class SalesDataWarehouse:
    def __init__(self):
        try:
            self.connection_string = config.database_conn_string
            self.engine = None
            self.dimension_tables = [
                'dim_optovik', 'dim_customer', 'dim_product', 'dim_time'
            ]
            
            # Store ID mappings for dimensions
            self.id_maps = {
                'optovik': {},
                'customer': {},
                'product': {},
                'time': {}
            }
        except Exception as e:
            raise RuntimeError(f"\n❌ Failed to initialize SalesDataWarehouse: {e}")

    def database_connection(self) -> bool:
        try:
            self.engine = create_engine(self.connection_string, echo=False)
            self.ensure_schema()
        except Exception as e:
            raise ExpectedCustomError(
        "\n❌ ETL process failed: Could not connect to the SQL Server.\n"
        "📌 Reason: A network-related or instance-specific error occurred.\n\n"
        "🔍 Troubleshooting steps:\n"
        "1. ✅ Make sure the SQL Server name in your connection string in config is correct.\n"
        "2. 🖥️ Check if the SQL Server instance is **running**.\n"
        "3. 🌐 Ensure the server allows **remote connections** (SQL Server Configuration Manager).\n"
        "4. 🔥 Verify that the **firewall** is not blocking the port (default: 1433).\n"
        "5. 📶 Ensure the **Named Pipes** or **TCP/IP protocols** are enabled.\n"
        "6. 🧪 Try connecting using `sqlcmd` or SSMS to verify connectivity.\n\n"
        f"🔧 Technical details: {e}"
        )

    def ensure_schema(self):
        """Ensure tables exist without dropping existing data"""
        try:
            DDL_GROUPS = [
                ('dim_optovik', [
                    """CREATE TABLE dim_optovik (
                        Optovik_ID INT IDENTITY(1,1) PRIMARY KEY,
                        Optovik NVARCHAR(255) NOT NULL UNIQUE,
                        last_updated DATETIME NOT NULL
                    )"""
                ]),
                
                ('dim_customer', [
                    """CREATE TABLE dim_customer (
                        Customer_ID INT IDENTITY(1,1) PRIMARY KEY,
                        Customer NVARCHAR(255) COLLATE Latin1_General_CS_AS NOT NULL UNIQUE,
                        Region NVARCHAR(100),
                        Territory NVARCHAR(100),
                        last_updated DATETIME NOT NULL
                    )"""
                ]),
                
                ('dim_product', [
                    """CREATE TABLE dim_product (
                        Product_ID INT IDENTITY(1,1) PRIMARY KEY,
                        Product NVARCHAR(255) COLLATE Latin1_General_CS_AS NOT NULL UNIQUE,
                        Product_groups NVARCHAR(100),
                        last_updated DATETIME NOT NULL
                    )"""
                ]),
                
                ('dim_time', [
                    """CREATE TABLE dim_time (
                        Time_ID INT IDENTITY(1,1) PRIMARY KEY,
                        Year SMALLINT NOT NULL,
                        Month TINYINT NOT NULL,
                        Day TINYINT NOT NULL,
                        UNIQUE (Year, Month, Day)
                    )"""
                ]),
                
                ('fact_sales', [
                    """CREATE TABLE fact_sales (
                        Sales_ID BIGINT IDENTITY(1,1) PRIMARY KEY,
                        Optovik_ID INT NOT NULL REFERENCES dim_optovik(Optovik_ID),
                        Customer_ID INT NOT NULL REFERENCES dim_customer(Customer_ID),
                        Product_ID INT NOT NULL REFERENCES dim_product(Product_ID),
                        Time_ID INT NOT NULL REFERENCES dim_time(Time_ID),
                        Quantity DECIMAL(18,4),
                        TotalSales DECIMAL(18,2),
                        RowHash CHAR(64) NOT NULL UNIQUE,
                        LoadDate DATETIME DEFAULT GETDATE()
                    )""",
                    """CREATE INDEX idx_fact_optovik ON fact_sales (Optovik_ID)""",
                    """CREATE INDEX idx_fact_customer ON fact_sales (Customer_ID)""",
                    """CREATE INDEX idx_fact_product ON fact_sales (Product_ID)""",
                    """CREATE INDEX idx_fact_time ON fact_sales (Time_ID)"""
                ])
            ]
            
            with self.engine.connect() as conn:
                for table_name, ddl_list in DDL_GROUPS:
                    # Check if table exists using SQLAlchemy
                    result = conn.execute(
                        text("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :table_name"),
                        {"table_name": table_name}
                    )
                    if not result.fetchone():
                        for ddl in ddl_list:
                                conn.execute(text(ddl))
                                conn.commit()
        except SQLAlchemyError as e:
            raise SQLAlchemyError(f"\n❌ Schema creation failed: {e}")

    def transform_data(self, optovik_dict, drug_groups_df_melted, stage=False):
        """Transform raw data into structured format"""
            # Combine all sheets
        try:
            dfs = []
            for sheet, df in optovik_dict.items():
                df['Optovik'] = sheet
                dfs.append(df)
            main_df = pd.concat(dfs, ignore_index=True)

            
            main_df.drop(columns=["Optoviks"], inplace=True)
            
            # Extract date parts
            if stage:
                date_col = config.date_header_name
                main_df.drop(columns=[date_col], inplace=True)
            else:
                date_col = config.date_header_name
                main_df['Year'] = main_df[date_col].dt.year
                main_df['Month'] = main_df[date_col].dt.month
                main_df['Day'] = main_df[date_col].dt.day
                main_df.drop(columns=[date_col], inplace=True)

            
            # Rename columns
            column_map = {
                config.client_header_name: "Customer",
                config.drugs_header_name: "Product",
                config.region_header_name: "Region",
                config.territory_header_name: "Territory",
                config.quantity_header_name: "Quantity",
                config.total_sales_header_name: "TotalSales"
            }
            main_df.rename(columns=column_map, inplace=True)
            
            # Map product groups
            pg_map = drug_groups_df_melted.set_index('Products_gr')['Product_groups'].to_dict()
            main_df['Product_groups'] = main_df['Product'].map(pg_map)
            
            return main_df
        except Exception as e:
            raise RuntimeError(f"\n❌ Data transformation failed: {e}")

    def upsert_dimension(self, df, table_name, business_key, attributes):
        """Generic dimension upsert with Type 1 SCD handling"""
        try:
                
            if df.empty:
                return {}
            
            # Fix SettingWithCopyWarning
            df = df.copy()

            # Get existing records
            unique_keys = df[business_key].unique().tolist()
            if not unique_keys:
                return {}

            # Build column list for query
            columns = [business_key, 'last_updated']
            if attributes:
                columns = [business_key] + attributes + ['last_updated']
            
            # Chunk processing for large datasets
            CHUNK_SIZE = 1000
            existing_chunks = []
            for i in range(0, len(unique_keys), CHUNK_SIZE):
                chunk = unique_keys[i:i+CHUNK_SIZE]
                placeholders = ','.join([':val' + str(i) for i in range(len(chunk))])
                query = text(f"""
                    SELECT {', '.join(columns)}
                    FROM {table_name}
                    WHERE {business_key} IN ({placeholders})
                """)
                params = {f'val{j}': key for j, key in enumerate(chunk)}
                chunk_df = pd.read_sql_query(query, self.engine, params=params)
                existing_chunks.append(chunk_df)
            
            existing = pd.concat(existing_chunks, ignore_index=True)
            
            # Prepare updates and inserts
            now = datetime.now()
            df['last_updated'] = now
            
            # New records - ensure no duplicates
            new_mask = ~df[business_key].isin(existing[business_key])
            new_records = df[new_mask].drop_duplicates(subset=[business_key])
            
            # Changed records (Type 1 SCD) - Only if attributes exist
            changed_records = pd.DataFrame()
            if attributes and not existing.empty:
                merged = df.merge(
                    existing, 
                    on=business_key, 
                    suffixes=('', '_old'),
                    how='inner'
                )
                
                changed_mask = False
                for attr in attributes:
                    # Handle potential NaN values
                    merged[attr] = merged[attr].fillna('')
                    merged[f"{attr}_old"] = merged[f"{attr}_old"].fillna('')
                    changed_mask |= (merged[attr] != merged[f"{attr}_old"])
                changed_records = merged[changed_mask]
            
            # Execute updates
            if not changed_records.empty and attributes:
                set_clause = ', '.join([f"{attr} = :{attr}" for attr in attributes])
                update_stmt = text(f"""
                    UPDATE {table_name}
                    SET {set_clause}, last_updated = :now
                    WHERE {business_key} = :key_value
                """)
                params = []
                for _, row in changed_records.iterrows():
                    param_dict = {attr: row[attr] for attr in attributes}
                    param_dict['now'] = now
                    param_dict['key_value'] = row[business_key]
                    params.append(param_dict)
                
                try:
                    with self.engine.begin() as conn:
                        conn.execute(update_stmt, params)
                    error_manager.log_info(f"  Updated {len(changed_records)} records in {table_name}")
                except SQLAlchemyError as e:
                    error_manager.log_info(f"Failed to update {table_name}: {str(e)}")
            
            # Execute inserts - one by one to catch duplicates
            inserted_count = 0
            if not new_records.empty:
                insert_cols = [business_key] + attributes + ['last_updated'] if attributes else [business_key, 'last_updated']
                insert_stmt = text(f"""
                    INSERT INTO {table_name} ({', '.join(insert_cols)})
                    VALUES ({', '.join([':' + col for col in insert_cols])})
                """)
                
                for _, row in new_records.iterrows():
                    values = {col: row[col] for col in insert_cols}
                    try:
                        with self.engine.begin() as conn:
                            conn.execute(insert_stmt, values)
                        inserted_count += 1
                    except IntegrityError:  # Duplicate key error
                        error_manager.log_info(f"Warning: Skipping duplicate {business_key}: {values[business_key]}")
                    except SQLAlchemyError as e:
                        raise RuntimeError(f"  Failed to insert into {table_name}: {str(e)}")
                
                error_manager.log_info(f"  Inserted {inserted_count} records into {table_name}")
            
            # Retrieve dimension IDs
            id_chunks = []
            for i in range(0, len(unique_keys), CHUNK_SIZE):
                chunk = unique_keys[i:i+CHUNK_SIZE]
                placeholders = ','.join([':val' + str(i) for i in range(len(chunk))])
                # Handle special case for dim_optovik
                id_col = "Optovik_ID" if table_name == "dim_optovik" else f"{table_name.split('_')[1]}_ID"
                id_query = text(f"""
                    SELECT {business_key}, {id_col} 
                    FROM {table_name} 
                    WHERE {business_key} IN ({placeholders})
                """)
                params = {f'val{j}': key for j, key in enumerate(chunk)}
                chunk_df = pd.read_sql_query(id_query, self.engine, params=params)
                id_chunks.append(chunk_df)
            
            id_map = pd.concat(id_chunks, ignore_index=True)
            id_dict = id_map.set_index(business_key).iloc[:, 0].to_dict()
            
            return id_dict
        except Exception as e:
            raise RuntimeError(f"\n❌ Upsert dimension '{table_name}' failed: {e}")

    def upsert_time_dimension(self, df):
        """Special handling for time dimension (append only)"""
        try:
            if df.empty:
                return {}
            
            # Get unique dates
            time_df = df[['Year', 'Month', 'Day']].drop_duplicates()
            
            # Convert to integers and handle NaN/float issues
            time_df = time_df.dropna()
            time_df['Year'] = time_df['Year'].astype(int)
            time_df['Month'] = time_df['Month'].astype(int)
            time_df['Day'] = time_df['Day'].astype(int)
            
            # Find existing dates
            existing = pd.read_sql_query("""
                SELECT Year, Month, Day, Time_ID 
                FROM dim_time
            """, self.engine)
            
            # Find new dates
            merged = time_df.merge(existing, on=['Year', 'Month', 'Day'], how='left')
            new_dates = merged[merged['Time_ID'].isna()][['Year', 'Month', 'Day']]
            
            # Insert new dates
            if not new_dates.empty:
                insert_stmt = text("""
                    INSERT INTO dim_time (Year, Month, Day)
                    VALUES (:Year, :Month, :Day)
                """)
                try:
                    with self.engine.begin() as conn:
                        # Convert to list of dictionaries
                        data_dicts = new_dates.to_dict(orient='records')
                        conn.execute(insert_stmt, data_dicts)
                    error_manager.log_info(f"Inserted {len(new_dates)} new dates into dim_time")
                except SQLAlchemyError as e:
                    raise SQLAlchemyError(f"Failed to insert dates: {str(e)}")
            
            # Retrieve all IDs
            id_map = pd.read_sql_query(
                "SELECT Year, Month, Day, Time_ID FROM dim_time", 
                self.engine
            )
            id_dict = {
                (r.Year, r.Month, r.Day): r.Time_ID 
                for r in id_map.itertuples()
            }
            
            return id_dict
        except Exception as e:
            raise RuntimeError(f"\n❌ Upsert time dimension failed: {e}")

    def load_fact_data(self, fact_df):
        """Efficient fact table loading with hash-based deduplication"""
        try:
            if fact_df.empty:
                error_manager.log_info("Warning: No fact records to load")
                return 0
            
            # Create staging table
            staging_name = "#staging_fact_sales"
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"""
                        CREATE TABLE {staging_name} (
                            RowHash CHAR(64) PRIMARY KEY,
                            Optovik_ID INT,
                            Customer_ID INT,
                            Product_ID INT,
                            Time_ID INT,
                            Quantity DECIMAL(18,4),
                            TotalSales DECIMAL(18,2)
                        )
                    """))
                
                # Bulk insert to staging
                insert_stmt = text(f"""
                    INSERT INTO {staging_name} 
                    VALUES (:RowHash, :Optovik_ID, :Customer_ID, 
                            :Product_ID, :Time_ID, :Quantity, :TotalSales)
                """)
                data_dicts = fact_df.to_dict(orient='records')
                with self.engine.begin() as conn:
                    conn.execute(insert_stmt, data_dicts)
                
                # Load new facts
                with self.engine.begin() as conn:
                    result = conn.execute(text(f"""
                        INSERT INTO fact_sales (
                            RowHash, Optovik_ID, Customer_ID, 
                            Product_ID, Time_ID, Quantity, TotalSales
                        )
                        SELECT 
                            s.RowHash, s.Optovik_ID, s.Customer_ID,
                            s.Product_ID, s.Time_ID, s.Quantity, s.TotalSales
                        FROM {staging_name} s
                        LEFT JOIN fact_sales f ON s.RowHash = f.RowHash
                        WHERE f.RowHash IS NULL
                    """))
                    new_count = result.rowcount
                    error_manager.log_info(f"    Loaded {new_count} new fact records.")
                
            except SQLAlchemyError as e:
                error_manager.log_info(f"Warning: Fact load failed: {str(e)}")
                new_count = 0
            finally:
                # Cleanup staging
                try:
                    with self.engine.begin() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {staging_name}"))
                except SQLAlchemyError:
                    pass
            return new_count
        
        except SQLAlchemyError as e:
            error_manager.log_error(f"Fact load failed: {e}")
            return 0
        except Exception as e:
            raise RuntimeError(f"\n❌ Fact data load failed: {e}")

    def run_etl(self, optovik_dict, drug_groups_df_melted):
        """Main ETL orchestration method"""
        try:
            # Step 1: Connect to database
            self.database_connection()

            # Step 2: Transform data
            main_df = self.transform_data(optovik_dict, drug_groups_df_melted)
            
            # Step 3: Process dimensions and get ID mappings
            # Optovik dimension
            optovik_df = main_df[['Optovik']].drop_duplicates()
            self.id_maps['optovik'] = self.upsert_dimension(
                optovik_df, 'dim_optovik', 'Optovik', []
            )

            conflicting_customers = (
                main_df.groupby('Customer')[['Region', 'Territory']]
                .nunique()
                .gt(1)
                .any(axis=1)
            )
            if conflicting_customers.any():
                raise ValueError("Some customers have conflicting Region or Territory mappings!")

            customer_df = main_df.drop_duplicates(subset=['Customer', 'Region', 'Territory'])
            self.id_maps['customer'] = self.upsert_dimension(
                customer_df, 'dim_customer', 'Customer', ['Region', 'Territory']
            )

            product_df = main_df.drop_duplicates(subset=['Product','Product_groups'])
            self.id_maps['product'] = self.upsert_dimension(
                product_df, 'dim_product', 'Product', ['Product_groups']
            )
            
            # Time dimension
            self.id_maps['time'] = self.upsert_time_dimension(main_df)
            
            # Step 4: Prepare fact table
            error_manager.log_info("  Preparing fact data...")
            fact_data = []
            skipped_rows = 0
            valid_rows = 0
            
            for _, row in main_df.iterrows():
                # Get dimension IDs with fallback to None
                optovik_id = self.id_maps['optovik'].get(row['Optovik'])
                customer_id = self.id_maps['customer'].get(row['Customer'])
                product_id = self.id_maps['product'].get(row['Product'])
                time_id = self.id_maps['time'].get((row['Year'], row['Month'], row['Day']))
                
                # Skip invalid references
                if None in (optovik_id, customer_id, product_id, time_id):
                    skipped_rows += 1
                    continue
                
                valid_rows += 1
                # Generate row hash
                hash_str = (
                    f"{optovik_id}|{customer_id}|{product_id}|{time_id}|"
                    f"{row['Quantity']}|{row['TotalSales']}"
                )
                row_hash = hashlib.sha256(hash_str.encode()).hexdigest()
                
                fact_data.append((
                    row_hash, optovik_id, customer_id, 
                    product_id, time_id, row['Quantity'], row['TotalSales']
                ))
            
            error_manager.log_info(f"    Valid rows: {valid_rows}, Skipped rows: {skipped_rows}")
            
            fact_df = pd.DataFrame(fact_data, columns=[
                'RowHash', 'Optovik_ID', 'Customer_ID', 
                'Product_ID', 'Time_ID', 'Quantity', 'TotalSales'
            ])
            
            # Step 5: Load fact data
            error_manager.log_info("  Loading fact data...")
            new_rows = self.load_fact_data(fact_df)
            
            # Step 6: Finalize
            error_manager.log_info(f"  ETL completed successfully. Loaded {new_rows} new records.")
            return new_rows
            
        except Exception as e:
            raise
        finally:
            if self.engine:
                self.engine.dispose()
                error_manager.log_info("  Database connection closed")