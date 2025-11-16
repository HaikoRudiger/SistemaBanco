# crypto_utils.py
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64
import os

# chave AES-256 (32 bytes)
SECRET_KEY = os.getenv("CRYPTO_KEY", "MINHA_CHAVE_SECRETA_32_BYTES_123456").encode()

def encrypt_value(value: float) -> str:
    """
    Recebe valor número → devolve string criptografada (Base64)
    """
    cipher = AES.new(SECRET_KEY, AES.MODE_CBC)
    iv = cipher.iv
    valor_bytes = str(value).encode()
    ct = cipher.encrypt(pad(valor_bytes, AES.block_size))
    return base64.b64encode(iv + ct).decode()

def decrypt_value(encoded: str) -> float:
    """
    Recebe string criptografada → devolve valor float
    """
    data = base64.b64decode(encoded)
    iv = data[:16]
    ct = data[16:]
    cipher = AES.new(SECRET_KEY, AES.MODE_CBC, iv)
    valor_bytes = unpad(cipher.decrypt(ct), AES.block_size).decode()
    return float(valor_bytes)
