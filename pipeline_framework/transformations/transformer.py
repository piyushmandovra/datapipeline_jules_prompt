import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Transformer:
    """
    A class to apply a series of transformations to a pandas DataFrame.
    """
    def __init__(self):
        """
        Initializes the Transformer.
        `custom_transformations` can be used to register user-defined functions.
        """
        self.custom_transformations = {} # Placeholder for future custom function registration
        logging.info("Transformer initialized.")

    def _rename_columns(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Renames columns in the DataFrame."""
        mapper = params.get('mapper')
        if not isinstance(mapper, dict):
            logging.error("'_rename_columns' expects 'mapper' dictionary in params.")
            raise ValueError("'_rename_columns' expects 'mapper' dictionary in params.")
        
        # Check for non-existent columns in the mapper keys
        missing_cols = [col for col in mapper.keys() if col not in df.columns]
        if missing_cols:
            logging.warning(f"Columns to rename not found in DataFrame: {missing_cols}. They will be ignored.")
            # Filter out non-existent columns from the mapper
            mapper = {k: v for k, v in mapper.items() if k in df.columns}

        if not mapper: # If all mapped columns were missing
            logging.warning("No valid columns found to rename. DataFrame remains unchanged.")
            return df

        logging.info(f"Applying rename_columns with mapper: {mapper}")
        return df.rename(columns=mapper)

    def _add_column(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Adds a new column to the DataFrame."""
        column_name = params.get('column_name')
        value = params.get('value') # Can be a static value or an expression string
        expression = params.get('expression') # e.g., "df['col_a'] * 2"

        if not column_name or not isinstance(column_name, str):
            logging.error("'_add_column' expects 'column_name' (string) in params.")
            raise ValueError("'_add_column' expects 'column_name' (string) in params.")
        
        if column_name in df.columns:
            logging.warning(f"Column '{column_name}' already exists. It will be overwritten.")

        if expression is not None:
            if not isinstance(expression, str):
                logging.error("'expression' in _add_column must be a string.")
                raise ValueError("'expression' in _add_column must be a string.")
            try:
                # WARNING: Using eval can be dangerous with untrusted input.
                # In a production system, consider safer alternatives like numexpr or ast.literal_eval
                # for simple expressions, or a more controlled DSL.
                df[column_name] = pd.eval(expression, engine='python', local_dict={'df': df})
                logging.info(f"Applying add_column '{column_name}' using expression: {expression}")
            except Exception as e:
                logging.error(f"Error evaluating expression for new column '{column_name}': {e}")
                raise ValueError(f"Error evaluating expression for new column '{column_name}': {e}")
        elif value is not None:
            df[column_name] = value
            logging.info(f"Applying add_column '{column_name}' with static value: {value}")
        else:
            logging.error("'_add_column' expects either 'value' or 'expression' in params.")
            raise ValueError("'_add_column' expects either 'value' or 'expression' in params.")
        return df

    def _drop_columns(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Drops specified columns from the DataFrame."""
        columns_to_drop = params.get('columns_to_drop')
        if not isinstance(columns_to_drop, list) or not all(isinstance(c, str) for c in columns_to_drop):
            logging.error("'_drop_columns' expects 'columns_to_drop' (list of strings) in params.")
            raise ValueError("'_drop_columns' expects 'columns_to_drop' (list of strings) in params.")
        
        # Only attempt to drop columns that actually exist, log others
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        non_existing_columns = [col for col in columns_to_drop if col not in df.columns]
        if non_existing_columns:
            logging.warning(f"Columns to drop not found in DataFrame: {non_existing_columns}. They will be ignored.")

        if not existing_columns_to_drop:
            logging.warning("No existing columns found to drop. DataFrame remains unchanged.")
            return df
            
        logging.info(f"Applying drop_columns: {existing_columns_to_drop}")
        return df.drop(columns=existing_columns_to_drop)

    def _filter_rows(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        """Filters rows based on a query string."""
        query_string = params.get('query_string')
        if not isinstance(query_string, str):
            logging.error("'_filter_rows' expects 'query_string' (string) in params.")
            raise ValueError("'_filter_rows' expects 'query_string' (string) in params.")
        
        logging.info(f"Applying filter_rows with query: {query_string}")
        try:
            return df.query(query_string)
        except Exception as e:
            logging.error(f"Error applying filter query '{query_string}': {e}")
            # Depending on requirements, might return original df or raise
            raise ValueError(f"Invalid query string for filtering: {e}")


    def apply_transformations(self, df: pd.DataFrame, transformation_configs: list[dict]) -> pd.DataFrame:
        """
        Applies a series of transformations to the DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame.
            transformation_configs (list[dict]): A list of transformation configurations.
                Each dict should have "name" and "params".

        Returns:
            pd.DataFrame: The transformed DataFrame.
        """
        if not isinstance(df, pd.DataFrame):
            logging.error("Input 'df' must be a pandas DataFrame.")
            raise TypeError("Input 'df' must be a pandas DataFrame.")
        if not isinstance(transformation_configs, list):
            logging.error("'transformation_configs' must be a list of dictionaries.")
            raise TypeError("'transformation_configs' must be a list of dictionaries.")

        current_df = df.copy() # Work on a copy to avoid modifying original DataFrame outside the function

        for config in transformation_configs:
            if not isinstance(config, dict) or 'name' not in config or 'params' not in config:
                logging.error(f"Invalid transformation config: {config}. Must be a dict with 'name' and 'params'.")
                # Optionally, skip this transformation or raise an error
                continue 

            transform_name = config['name']
            params = config['params']
            logging.info(f"Attempting transformation: {transform_name} with params: {params}")

            # Look for built-in methods (e.g., _rename_columns)
            method_name = f"_{transform_name}"
            transform_method = getattr(self, method_name, None)

            if transform_method and callable(transform_method):
                try:
                    current_df = transform_method(current_df, params)
                    logging.info(f"Successfully applied transformation: {transform_name}")
                except Exception as e:
                    logging.error(f"Error applying transformation '{transform_name}': {e}")
                    # Depending on desired behavior, re-raise, skip, or handle
                    # For now, let's re-raise to make issues visible during development
                    raise
            # Placeholder for custom registered functions
            # elif transform_name in self.custom_transformations:
            #     try:
            #         current_df = self.custom_transformations[transform_name](current_df, params)
            #         logging.info(f"Successfully applied custom transformation: {transform_name}")
            #     except Exception as e:
            #         logging.error(f"Error applying custom transformation '{transform_name}': {e}")
            #         raise
            else:
                logging.error(f"Unknown transformation: {transform_name}. Skipping.")
                # Optionally raise an error for unknown transformations
                # raise ValueError(f"Unknown transformation: {transform_name}")
        
        return current_df

if __name__ == '__main__':
    print("Running Transformer test example...")

    # Sample DataFrame
    data = {
        'id': [1, 2, 3, 4, 5],
        'old_name_A': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'category': ['X', 'Y', 'X', 'Z', 'Y'],
        'value1': [10, 20, 30, 40, 50],
        'value2': [100, 200, 150, 250, 180],
        'to_drop': [None, None, None, None, None]
    }
    sample_df = pd.DataFrame(data)
    print("\nOriginal DataFrame:")
    print(sample_df)

    transformer = Transformer()

    # Define transformation configurations
    transformation_configs = [
        {"name": "rename_columns", "params": {"mapper": {"old_name_A": "name", "value1": "metric1"}}},
        {"name": "add_column", "params": {"column_name": "metric1_plus_10", "expression": "df['metric1'] + 10"}},
        {"name": "add_column", "params": {"column_name": "source_system", "value": "SystemA"}},
        {"name": "filter_rows", "params": {"query_string": "metric1 > 15 and category != 'Z'"}},
        {"name": "drop_columns", "params": {"columns_to_drop": ["value2", "to_drop", "non_existent_col"]}}
    ]

    print(f"\nApplying transformations: {transformation_configs}")
    try:
        transformed_df = transformer.apply_transformations(sample_df.copy(), transformation_configs) # Pass a copy
        print("\nTransformed DataFrame:")
        print(transformed_df)
    except Exception as e:
        print(f"Error during transformation: {e}")
        logging.exception("Error in main transformation block:")

    print("\n--- Test 2: Unknown transformation name ---")
    unknown_transform_config = [
        {"name": "non_existent_transformation", "params": {}}
    ]
    try:
        print(f"Applying unknown transformation: {unknown_transform_config}")
        transformed_df_unknown = transformer.apply_transformations(sample_df.copy(), unknown_transform_config)
        print("\nDataFrame after unknown transformation (should log error and skip):")
        print(transformed_df_unknown) # Should be unchanged if skipping, or error if raising
    except Exception as e:
        print(f"Caught expected error for unknown transformation: {e}")

    print("\n--- Test 3: Invalid parameters for a transformation ---")
    invalid_params_config = [
        {"name": "rename_columns", "params": {"wrong_param_name": {"old": "new"}}}
    ]
    try:
        print(f"Applying transformation with invalid params: {invalid_params_config}")
        transformed_df_invalid_params = transformer.apply_transformations(sample_df.copy(), invalid_params_config)
        print("\nDataFrame after invalid params (should raise error):")
        print(transformed_df_invalid_params)
    except Exception as e:
        print(f"Caught expected error for invalid params: {e}")

    print("\n--- Test 4: Add column that already exists (should warn and overwrite) ---")
    add_existing_col_config = [
        {"name": "add_column", "params": {"column_name": "category", "value": "OVERWRITTEN"}}
    ]
    try:
        print(f"Applying transformation to add existing column: {add_existing_col_config}")
        df_add_existing = transformer.apply_transformations(sample_df.copy(), add_existing_col_config)
        print("\nDataFrame after adding existing column:")
        print(df_add_existing)
    except Exception as e:
        print(f"Error during Test 4: {e}")

    print("\nTransformer test example finished.")
