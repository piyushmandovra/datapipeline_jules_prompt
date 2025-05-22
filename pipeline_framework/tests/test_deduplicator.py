import unittest
from pipeline_framework.deduplication.deduplicator import generate_spark_deduplication_code, generate_merge_into_script

class TestDeduplicator(unittest.TestCase):

    def test_generate_drop_duplicates_code(self):
        """Test 'drop_duplicates' strategy for PySpark code snippet generation."""
        # Basic drop_duplicates
        params_basic = {
            'strategy': 'drop_duplicates',
            'deduplication_keys': ['user_id', 'event_type'],
            'input_dataframe_name': 'my_df'
        }
        script_basic = generate_spark_deduplication_code(params_basic)
        self.assertIn('my_df = my_df.dropDuplicates(["user_id", "event_type"])', script_basic)
        self.assertNotIn("withWatermark", script_basic)

        # drop_duplicates with watermark
        params_watermark = {
            'strategy': 'drop_duplicates',
            'deduplication_keys': ['session_id'],
            'watermark_column': 'event_timestamp',
            'watermark_delay_threshold': '10 minutes',
            'input_dataframe_name': 'streaming_df' # Test different df name
        }
        script_watermark = generate_spark_deduplication_code(params_watermark)
        self.assertIn('streaming_df = streaming_df.withWatermark("event_timestamp", "10 minutes")', script_watermark)
        self.assertIn('streaming_df = streaming_df.dropDuplicates(["session_id"])', script_watermark)
        
        # Default input_dataframe_name
        params_default_df = {
            'strategy': 'drop_duplicates',
            'deduplication_keys': ['id']
        }
        script_default_df = generate_spark_deduplication_code(params_default_df)
        self.assertIn('df = df.dropDuplicates(["id"])', script_default_df)


    def test_generate_merge_into_sql(self):
        """Test 'merge_into' strategy for Spark SQL generation."""
        # Basic MERGE INTO
        params_basic_merge = {
            'strategy': 'merge_into',
            'target_table_name': 'silver.users',
            'source_view_name': 'new_users_vw',
            'deduplication_keys': ['user_id']
        }
        sql_basic = generate_spark_deduplication_code(params_basic_merge)
        self.assertIn("MERGE INTO silver.users AS target", sql_basic)
        self.assertIn("USING new_users_vw AS source", sql_basic)
        self.assertIn("ON target.user_id = source.user_id", sql_basic)
        self.assertNotIn("WHEN MATCHED THEN", sql_basic) # No clauses by default
        self.assertNotIn("WHEN NOT MATCHED THEN", sql_basic)

        # MERGE INTO with all clauses
        params_full_merge = {
            'strategy': 'merge_into',
            'target_table_name': 'gold.customers',
            'source_view_name': 'customer_updates_vw',
            'deduplication_keys': ['customer_id', 'store_id'],
            'merge_condition_extras': "AND source.is_active = true",
            'when_matched_update_clause': "SET target.name = source.name, target.updated_ts = current_timestamp()",
            'when_not_matched_insert_clause': "INSERT (customer_id, store_id, name, created_ts, updated_ts) VALUES (source.customer_id, source.store_id, source.name, current_timestamp(), current_timestamp())",
            'when_not_matched_by_source_delete_clause': "DELETE",
            'when_not_matched_by_source_update_clause': "SET target.is_valid = false"
        }
        sql_full = generate_spark_deduplication_code(params_full_merge)
        self.assertIn("ON target.customer_id = source.customer_id AND target.store_id = source.store_id AND source.is_active = true", sql_full)
        self.assertIn("WHEN MATCHED THEN SET target.name = source.name, target.updated_ts = current_timestamp()", sql_full)
        self.assertIn("WHEN NOT MATCHED THEN INSERT (customer_id, store_id, name, created_ts, updated_ts) VALUES (source.customer_id, source.store_id, source.name, current_timestamp(), current_timestamp())", sql_full)
        self.assertIn("WHEN NOT MATCHED BY SOURCE THEN DELETE", sql_full)
        self.assertIn("WHEN NOT MATCHED BY SOURCE THEN SET target.is_valid = false", sql_full) # Second one

        # MERGE INTO with "NONE" clauses
        params_none_clauses = {
            'strategy': 'merge_into',
            'target_table_name': 'logs.events',
            'source_view_name': 'incoming_events_vw',
            'deduplication_keys': ['event_id'],
            'when_matched_update_clause': "NONE",
            'when_not_matched_insert_clause': "NONE"
        }
        sql_none = generate_spark_deduplication_code(params_none_clauses)
        self.assertNotIn("WHEN MATCHED THEN", sql_none)
        self.assertNotIn("WHEN NOT MATCHED THEN", sql_none)

    def test_generate_merge_into_script_basic(self):
        """Test generation of a full PySpark script for MERGE INTO."""
        params = {
            'source_table_name': 'bronze.raw_events',
            'target_table_name': 'silver.deduplicated_events',
            'deduplication_keys': ['event_id', 'event_timestamp'],
            'when_matched_update_clause': "SET target.payload = source.payload",
            'when_not_matched_insert_clause': "INSERT *",
            'spark_app_name': "EventDeduplicationApp",
            'extra_spark_configs': {"spark.sql.shuffle.partitions": "200"}
        }
        full_script = generate_merge_into_script(params)
        
        self.assertIn(f'.appName("{params["spark_app_name"]}")', full_script)
        self.assertIn(f'spark.conf.set("spark.sql.shuffle.partitions", "200")', full_script)
        
        source_view_name_for_sql = f"{params['source_table_name'].replace('.', '_')}_source_vw"
        self.assertIn(f'spark.read.table("{params["source_table_name"]}").createOrReplaceTempView("{source_view_name_for_sql}")', full_script)
        
        # Check if the MERGE SQL is embedded
        merge_sql_params_for_check = params.copy()
        merge_sql_params_for_check['strategy'] = 'merge_into'
        merge_sql_params_for_check['source_view_name'] = source_view_name_for_sql # Important
        expected_sql_part = generate_spark_deduplication_code(merge_sql_params_for_check)
        self.assertIn(expected_sql_part, full_script)
        self.assertIn(f'spark.catalog.dropTempView("{source_view_name_for_sql}")', full_script)


    def test_deduplicator_missing_required_param(self):
        """Test ValueError for missing required parameters in both functions."""
        # For generate_spark_deduplication_code
        with self.assertRaisesRegex(ValueError, "Missing or empty required parameters: \\['deduplication_keys'\\]"):
            generate_spark_deduplication_code({'strategy': 'drop_duplicates'})

        with self.assertRaisesRegex(ValueError, "Missing or empty required parameters: \\['target_table_name', 'source_view_name', 'deduplication_keys'\\]"):
            generate_spark_deduplication_code({'strategy': 'merge_into'})
            
        with self.assertRaisesRegex(ValueError, "'deduplication_keys' cannot be empty for 'merge_into' strategy."):
            generate_spark_deduplication_code({
                'strategy': 'merge_into',
                'target_table_name': 't',
                'source_view_name': 's',
                'deduplication_keys': [] # Empty list
            })

        # For generate_merge_into_script
        with self.assertRaisesRegex(ValueError, "Missing or empty required parameters: \\['target_table_name', 'source_table_name', 'deduplication_keys'\\]"):
            generate_merge_into_script({})
            
        with self.assertRaisesRegex(ValueError, "Missing or empty required parameters: \\['source_table_name'\\]"):
            generate_merge_into_script({
                'target_table_name': 't_table',
                'deduplication_keys': ['id']
            })


    def test_invalid_strategy(self):
        """Test ValueError for invalid strategy."""
        with self.assertRaisesRegex(ValueError, "Invalid 'strategy': foobar. Must be 'drop_duplicates' or 'merge_into'."):
            generate_spark_deduplication_code({'strategy': 'foobar'})
            
        # Test missing strategy
        with self.assertRaisesRegex(ValueError, "Missing 'strategy' parameter."):
            generate_spark_deduplication_code({})

if __name__ == '__main__':
    unittest.main()
