# utils.py

import hmac
import hashlib
import random
from sqlalchemy.orm import Session
import models

def verify_hmac(secret, data, hmac_header):
    calculated_hmac = hmac.new(
        secret.encode("utf-8"),
        data,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(calculated_hmac, hmac_header)

# --- ADDED: Barcode Generation Logic ---

def calculate_ean13_check_digit(barcode_without_check_digit: str) -> str:
    """Calculates the check digit for an EAN-13 barcode."""
    if len(barcode_without_check_digit) != 12:
        raise ValueError("Input for check digit calculation must be 12 digits long.")
    
    digits = [int(d) for d in barcode_without_check_digit]
    
    # Sum odd and even positions (1-based index)
    odd_sum = sum(digits[0::2])
    even_sum = sum(digits[1::2])
    
    total_sum = odd_sum + (even_sum * 3)
    check_digit = (10 - (total_sum % 10)) % 10
    
    return str(check_digit)

def generate_ean13(db: Session) -> str:
    """
    Generates a unique, EAN-13 compliant barcode by creating a random
    12-digit base and calculating the 13th check digit.
    It ensures the generated barcode is unique in the database.
    """
    while True:
        # Using a common prefix for Romania and random numbers
        base = '594' + ''.join([str(random.randint(0, 9)) for _ in range(9)])
        check_digit = calculate_ean13_check_digit(base)
        barcode = base + check_digit
        
        # Check for uniqueness in the database
        exists = db.query(models.ProductVariant).filter(models.ProductVariant.barcode == barcode).first()
        if not exists:
            return barcode