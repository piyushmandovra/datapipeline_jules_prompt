import logging
from cryptography.fernet import Fernet, InvalidToken

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Decryptor:
    """
    Handles data decryption using Fernet symmetric encryption.
    """
    def __init__(self, key: bytes):
        """
        Initializes the Decryptor with an encryption key.

        Args:
            key (bytes): The Fernet encryption key.
                         **Security Note**: This key should be managed securely.
                         For production, use a secrets management system (e.g., HashiCorp Vault,
                         AWS Secrets Manager, Azure Key Vault, GCP Secret Manager) and retrieve
                         the key at runtime. Avoid hardcoding keys or storing them in version control.
                         In Airflow, use Connections or a secrets backend.
        """
        if not key or not isinstance(key, bytes):
            logging.error("Decryption key must be a non-empty bytes object.")
            raise ValueError("Decryption key must be a non-empty bytes object.")
        try:
            self.fernet = Fernet(key)
            logging.info("Decryptor initialized successfully.")
        except Exception as e:
            logging.exception("Failed to initialize Fernet with the provided key:")
            raise ValueError(f"Invalid Fernet key: {e}")

    def decrypt_data(self, encrypted_data: bytes) -> bytes | None:
        """
        Decrypts the provided data.

        Args:
            encrypted_data (bytes): The data to decrypt.

        Returns:
            bytes | None: The decrypted data, or None if decryption fails.
        """
        if not encrypted_data or not isinstance(encrypted_data, bytes):
            logging.error("Encrypted data must be a non-empty bytes object.")
            return None
        try:
            decrypted_data = self.fernet.decrypt(encrypted_data)
            logging.info("Data decrypted successfully.")
            return decrypted_data
        except InvalidToken:
            logging.error("Decryption failed: Invalid token. The key may be incorrect or the data corrupted.")
            return None
        except Exception as e:
            logging.exception("An unexpected error occurred during decryption:")
            return None

if __name__ == '__main__':
    print("Running Decryptor test example...")

    try:
        # 1. Generate a new Fernet key
        test_key = Fernet.generate_key()
        print(f"Generated Test Key: {test_key.decode()}")

        # 2. Create an instance of the Decryptor
        decryptor_instance = Decryptor(test_key)

        # 3. Define some sample data (string)
        original_data_str = "This is some secret data for testing!"
        original_data_bytes = original_data_str.encode('utf-8')
        print(f"Original Data: '{original_data_str}'")

        # 4. Encrypt the sample data using the Fernet instance directly (for testing)
        # (The Decryptor class is only for decryption, so we use Fernet directly for encryption here)
        temp_fernet_for_encryption = Fernet(test_key)
        encrypted_data_bytes = temp_fernet_for_encryption.encrypt(original_data_bytes)
        print(f"Encrypted Data (bytes): {encrypted_data_bytes}")

        # 5. Call the decrypt_data method with the encrypted data
        decrypted_data_bytes = decryptor_instance.decrypt_data(encrypted_data_bytes)

        if decrypted_data_bytes:
            decrypted_data_str = decrypted_data_bytes.decode('utf-8')
            print(f"Decrypted Data: '{decrypted_data_str}'")
            assert original_data_str == decrypted_data_str
            print("Test successful: Original and decrypted data match.")
        else:
            print("Test failed: Decryption returned None.")

        print("\nTesting error case: incorrect key...")
        wrong_key = Fernet.generate_key()
        decryptor_wrong_key = Decryptor(wrong_key)
        decrypted_with_wrong_key = decryptor_wrong_key.decrypt_data(encrypted_data_bytes)
        if decrypted_with_wrong_key is None:
            print("Test successful: Decryption with wrong key returned None as expected.")
        else:
            print(f"Test failed: Decryption with wrong key returned data: {decrypted_with_wrong_key}")

        print("\nTesting error case: corrupted data...")
        corrupted_data = encrypted_data_bytes[:-5] + b'12345' # Tamper with the data
        decrypted_corrupted_data = decryptor_instance.decrypt_data(corrupted_data)
        if decrypted_corrupted_data is None:
            print("Test successful: Decryption of corrupted data returned None as expected.")
        else:
            print(f"Test failed: Decryption of corrupted data returned data: {decrypted_corrupted_data}")

    except Exception as e:
        print(f"An error occurred during the test: {e}")
        logging.exception("Error in __main__ test block:")
