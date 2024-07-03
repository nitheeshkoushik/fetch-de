import configparser
from cryptography.fernet import Fernet

class Encryptor:

    def __init__(self, config_file): 
        config = configparser.ConfigParser()
        config.read(config_file)

        self.key = config.get('encryption', 'key')
        self.cipher_suite = Fernet(self.key)
    
    def encrypt(self, message):
        encryptedMessage = self.cipher_suite.encrypt(message.encode())
        return encryptedMessage

