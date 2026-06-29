import uuid
import pickle
from cryptography.fernet import Fernet
import os


class GETKEY:
    def __init__(self, service="payment_service", key_file="key"):
        self.pickle_file = f"./{service}/{service}.pkl"
        self.key_file = f"./{service}/{key_file}.key"
        self.key = self.load_or_generate_key()

    def generate_key(self):
        return Fernet.generate_key()

    def encrypt_data(self, data):
        fernet = Fernet(self.key)
        encrypted_data = fernet.encrypt(data.encode())
        return encrypted_data

    def decrypt_data(self, encrypted_data):
        fernet = Fernet(self.key)
        decrypted_data = fernet.decrypt(encrypted_data).decode()
        return decrypted_data

    def load_or_generate_key(self):
        if not os.path.exists(self.key_file):
            key = self.generate_key()
            with open(self.key_file, "wb") as keyfile:
                keyfile.write(key)
            return key
        else:

            with open(self.key_file, "rb") as keyfile:
                return keyfile.read()

    def get_uuid(self):
        if os.path.exists(self.pickle_file):
            # Load the encrypted UUID from pickle
            with open(self.pickle_file, "rb") as file:
                encrypted_uuid = pickle.load(file)  # nosec
            # Decrypt the UUID
            uuid_str = self.decrypt_data(encrypted_uuid)
            print(f"Loaded UUID from pickle: {uuid_str}")
        else:
            # Generate a new UUID and encrypt it
            uuid_str = str(uuid.uuid4())
            encrypted_uuid = self.encrypt_data(uuid_str)
            # Store the encrypted UUID in pickle
            with open(self.pickle_file, "wb") as file:
                pickle.dump(encrypted_uuid, file)
            print(f"Generated and stored new UUID: {uuid_str}")

        return uuid_str


# Usage Example:
