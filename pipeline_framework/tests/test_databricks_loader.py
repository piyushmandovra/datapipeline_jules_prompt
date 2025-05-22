import unittest
from pipeline_framework.data_loading.databricks_loader import generate_autoloader_script

class TestDatabricksLoader(unittest.TestCase):

    def test_generate_autoloader_script_basic(self):
        """Test basic Autoloader script generation."""
        params = {
            'input_path': "s3://my-bucket/landing/",
            'input_format': "json",
            'output_delta_table_name': "bronze.my_data",
            'schema_location': "s3://my-bucket/schemas/my_data_schema",
            'checkpoint_location': "s3://my-bucket/checkpoints/my_data_checkpoint"
        }
        script = generate_autoloader_script(params)
        self.assertTrue(script) # Not empty
        self.assertIn('spark.readStream.format("cloudFiles")', script)
        self.assertIn(f".option(\"cloudFiles.format\", \"{params['input_format']}\")", script)
        self.assertIn(f".load(\"{params['input_path']}\")", script)
        self.assertIn(f".table(\"{params['output_delta_table_name']}\")", script)
        self.assertIn(f".option(\"cloudFiles.schemaLocation\", \"{params['schema_location']}\")", script)
        self.assertIn(f".option(\"checkpointLocation\", \"{params['checkpoint_location']}\")", script)
        self.assertIn(".trigger(availableNow=True)", script) # Default trigger

    def test_generate_autoloader_script_with_options(self):
        """Test Autoloader script generation with various optional parameters."""
        params = {
            'input_path': "s3://my-bucket/landing_csv/",
            'input_format': "csv",
            'output_delta_table_name': "bronze.my_csv_data",
            'schema_location': "s3://my-bucket/schemas/my_csv_data_schema",
            'checkpoint_location': "s3://my-bucket/checkpoints/my_csv_data_checkpoint",
            'file_format_options': {
                "header": "true",
                "delimiter": ","
            },
            'extra_spark_configs': {
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
                "spark.some.other.config": 123
            },
            'trigger_type': "processingTime",
            'trigger_interval': "5 minutes",
            'output_mode': "overwrite",
            'spark_app_name': "MyCustomAutoloaderApp"
        }
        script = generate_autoloader_script(params)
        self.assertTrue(script)
        self.assertIn(f".option(\"header\", \"true\")", script)
        self.assertIn(f".option(\"delimiter\", \",\")", script)
        self.assertIn('spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")', script)
        self.assertIn('spark.conf.set("spark.some.other.config", "123")', script) # Note: value is stringified
        self.assertIn(f".trigger(processingTime='{params['trigger_interval']}')", script)
        self.assertIn(f".outputMode(\"{params['output_mode']}\")", script)
        self.assertIn(f'.appName("{params["spark_app_name"]}")', script)

    def test_generate_autoloader_script_missing_required_param(self):
        """Test script generation fails if a required parameter is missing."""
        params_no_input_path = {
            # 'input_path': "s3://my-bucket/landing/", # Missing
            'input_format': "json",
            'output_delta_table_name': "bronze.my_data",
            'schema_location': "s3://my-bucket/schemas/my_data_schema",
            'checkpoint_location': "s3://my-bucket/checkpoints/my_data_checkpoint"
        }
        with self.assertRaisesRegex(ValueError, "Missing required parameter 'input_path'"):
            generate_autoloader_script(params_no_input_path)

        params_no_format = {
            'input_path': "s3://my-bucket/landing/",
            # 'input_format': "json", # Missing
            'output_delta_table_name': "bronze.my_data",
            'schema_location': "s3://my-bucket/schemas/my_data_schema",
            'checkpoint_location': "s3://my-bucket/checkpoints/my_data_checkpoint"
        }
        with self.assertRaisesRegex(ValueError, "Missing required parameter 'input_format'"):
            generate_autoloader_script(params_no_format)
            
    def test_trigger_once_option(self):
        """Test 'once' trigger option."""
        params = {
            'input_path': "s3://my-bucket/landing/",
            'input_format': "json",
            'output_delta_table_name': "bronze.my_data",
            'schema_location': "s3://my-bucket/schemas/my_data_schema",
            'checkpoint_location': "s3://my-bucket/checkpoints/my_data_checkpoint",
            'trigger_type': "once"
        }
        script = generate_autoloader_script(params)
        self.assertIn(".trigger(once=True)", script)

if __name__ == '__main__':
    unittest.main()
