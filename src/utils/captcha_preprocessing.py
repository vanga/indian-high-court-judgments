"""
Image preprocessing utilities for CAPTCHA handling.
"""
import cv2
import numpy as np
from pathlib import Path
from typing import Optional

def preprocess_captcha_image(image_path: Path) -> Optional[Path]:
    """
    Preprocess alphanumeric CAPTCHA image to improve OCR accuracy.
    
    Args:
        image_path: Path to the CAPTCHA image
        
    Returns:
        Path: Path to preprocessed image, or None if processing fails
    """
    try:
        # Read image
        img = cv2.imread(str(image_path))
        if img is None:
            return None
            
        # Convert to grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Apply adaptive thresholding for better character separation
        thresh = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
            cv2.THRESH_BINARY_INV, 11, 2
        )
        
        # Remove small noise
        kernel = np.ones((2,2), np.uint8)
        denoised = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel)
        
        # Dilate slightly to make characters more solid
        kernel = np.ones((2,1), np.uint8)  # Vertical kernel
        dilated = cv2.dilate(denoised, kernel, iterations=1)
        
        # Remove any remaining horizontal lines that could connect characters
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1,3))
        cleaned = cv2.morphologyEx(dilated, cv2.MORPH_OPEN, horizontal_kernel)
        
        # Save preprocessed image
        output_path = image_path.with_stem(image_path.stem + "_processed")
        cv2.imwrite(str(output_path), cleaned)
        
        return output_path
        
    except Exception as e:
        print(f"Error preprocessing image: {str(e)}")
        return None

def clean_captcha_text(text: str) -> str:
    """
    Get CAPTCHA text from OCR output.
    Since High Court CAPTCHAs only contain alphanumeric characters,
    we just return the text as is.
    
    Args:
        text: Raw text from OCR
        
    Returns:
        str: The OCR text result
    """
    return text.strip()

def validate_captcha_chars(image_path: Path) -> bool:
    """
    Validate if the image appears to have the correct number of characters
    for an alphanumeric CAPTCHA (typically 6 characters).
    
    Args:
        image_path: Path to CAPTCHA image
        
    Returns:
        bool: True if the image likely contains the expected number of characters
    """
    try:
        # Read image
        img = cv2.imread(str(image_path))
        if img is None:
            return False
            
        # Convert to grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Threshold the image
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        
        # Find connected components (characters)
        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        # Filter contours by size to remove noise
        valid_contours = [c for c in contours if cv2.contourArea(c) > 50]  # Adjust threshold as needed
        
        # Check if number of contours is close to expected (6 characters)
        return 5 <= len(valid_contours) <= 7  # Allow some flexibility
        
    except Exception:
        return False