"""
CAPTCHA handling utilities for the Indian High Court Judgments project.
"""
import logging
import uuid
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
import requests
import traceback
from typing import Optional, Tuple, Dict, Any
from cachetools import TTLCache
from .error_utils import setup_logging, log_error
from .captcha_preprocessing import preprocess_captcha_image, clean_captcha_text, is_likely_math_captcha
from src.captcha_solver.main import get_text

logger = logging.getLogger(__name__)

class RateLimiter:
    """Rate limiter for CAPTCHA requests"""
    
    def __init__(self, max_requests: int = 10, time_window: int = 60):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum requests allowed in time window
            time_window: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    def wait_if_needed(self):
        """Wait if rate limit is exceeded"""
        now = datetime.now()
        self.requests = [t for t in self.requests 
                        if t > now - timedelta(seconds=self.time_window)]
                        
        if len(self.requests) >= self.max_requests:
            # Calculate required delay
            oldest_request = min(self.requests)
            delay = (oldest_request + timedelta(seconds=self.time_window) - now).total_seconds()
            if delay > 0:
                logger.debug(f"Rate limit reached, waiting {delay:.2f} seconds")
                time.sleep(delay)
                
        self.requests.append(now)

class CaptchaHandler:
    """Handles CAPTCHA solving and related session management"""
    
    def __init__(self, session_id: str, app_token: str, root_url: str, 
                 captcha_tmp_dir: Path, captcha_failures_dir: Path):
        """
        Initialize the CAPTCHA handler.
        
        Args:
            session_id: Current session ID
            app_token: Current application token
            root_url: Base URL for the application
            captcha_tmp_dir: Directory for temporary CAPTCHA files
            captcha_failures_dir: Directory for failed CAPTCHA attempts
        """
        self.session_id = session_id
        self.app_token = app_token
        self.root_url = root_url
        self.captcha_tmp_dir = captcha_tmp_dir
        self.captcha_failures_dir = captcha_failures_dir
        
        # Configuration
        self.max_retries = 5
        self.verify_ssl = False
        self.request_timeout = 30
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(max_requests=10, time_window=60)
        
        # Initialize solution cache (10 minute TTL)
        self.solution_cache = TTLCache(maxsize=100, ttl=600)
        
        # Performance metrics
        self.metrics = {
            'total_attempts': 0,
            'successful_solves': 0,
            'failed_solves': 0,
            'cache_hits': 0,
            'avg_solve_time': 0,
            'total_solve_time': 0
        }
        
        # Ensure directories exist
        self.captcha_tmp_dir.mkdir(parents=True, exist_ok=True)
        self.captcha_failures_dir.mkdir(parents=True, exist_ok=True)
        
    def update_metrics(self, success: bool, solve_time: float):
        """Update performance metrics"""
        self.metrics['total_attempts'] += 1
        if success:
            self.metrics['successful_solves'] += 1
        else:
            self.metrics['failed_solves'] += 1
            
        self.metrics['total_solve_time'] += solve_time
        self.metrics['avg_solve_time'] = (
            self.metrics['total_solve_time'] / self.metrics['total_attempts']
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics"""
        return {
            **self.metrics,
            'success_rate': (
                self.metrics['successful_solves'] / self.metrics['total_attempts']
                if self.metrics['total_attempts'] > 0 else 0
            ),
            'cache_hit_rate': (
                self.metrics['cache_hits'] / self.metrics['total_attempts']
                if self.metrics['total_attempts'] > 0 else 0
            )
        }
    
    def get_cookie(self, cookie_name: str) -> str:
        """Get cookie string for requests"""
        return f"{cookie_name}={self.session_id}"
    
    def solve_math_expression(self, expression: str) -> str:
        """
        Solve a mathematical expression from CAPTCHA.
        
        Args:
            expression: Math expression to solve (e.g., "5 + 3", "8 - 2")
            
        Returns:
            str: Solution to the expression
            
        Raises:
            ValueError: If expression is invalid or unsupported
        """
        expression = expression.strip().replace(" ", "").replace(".", "")
        
        # Handle different operators
        if "+" in expression:
            nums = expression.split("+")
            return str(int(nums[0]) + int(nums[1]))
        elif "-" in expression:
            nums = expression.split("-")
            return str(int(nums[0]) - int(nums[1]))
        elif any(op in expression for op in ["*", "X", "x", "×"]):
            expression = (
                expression.replace("x", "*")
                .replace("×", "*")
                .replace("X", "*")
            )
            nums = expression.split("*")
            return str(int(nums[0]) * int(nums[1]))
        elif "/" in expression or "÷" in expression:
            expression = expression.replace("÷", "/")
            nums = expression.split("/")
            return str(int(nums[0]) // int(nums[1]))
            
        raise ValueError(f"Unsupported mathematical expression: {expression}")
    
    def is_math_expression(self, text: str) -> bool:
        """Check if text is a mathematical expression"""
        operators = ["+", "-", "*", "/", "÷", "x", "×", "X"]
        return any(op in text for op in operators)
    
    def download_captcha_image(self, captcha_url: str, cookie: str) -> Tuple[Path, bool]:
        """
        Download CAPTCHA image from URL.
        
        Returns:
            Tuple[Path, bool]: (Path to saved image, Success status)
        """
        try:
            response = requests.get(
                captcha_url,
                headers={"Cookie": cookie},
                verify=self.verify_ssl,
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to download CAPTCHA image: HTTP {response.status_code}")
                return None, False
                
            # Generate unique filename
            unique_id = uuid.uuid4().hex[:8]
            image_path = self.captcha_tmp_dir / f"captcha_{unique_id}.png"
            
            with open(image_path, "wb") as f:
                f.write(response.content)
                
            return image_path, True
            
        except Exception as e:
            logger.error(f"Error downloading CAPTCHA: {str(e)}")
            return None, False
    
    def solve_captcha(self, captcha_url: str, cookie_name: str, 
                     is_math_captcha: bool = False, context: dict = None,
                     retries: int = 0) -> Optional[str]:
        """
        Solve a CAPTCHA challenge with improved handling and caching.
        
        Args:
            captcha_url: URL to download CAPTCHA image
            cookie_name: Name of the session cookie
            is_math_captcha: Whether this is a math CAPTCHA
            context: Additional context for logging
            retries: Current retry attempt number
            
        Returns:
            Optional[str]: CAPTCHA solution if successful, None otherwise
        """
        start_time = time.time()
        
        try:
            # Check rate limiting
            self.rate_limiter.wait_if_needed()
            
            # Check cache first
            cache_key = f"{captcha_url}:{self.session_id}"
            if cache_key in self.solution_cache:
                self.metrics['cache_hits'] += 1
                return self.solution_cache[cache_key]
            
            if retries >= self.max_retries:
                logger.error("Max CAPTCHA retries exceeded")
                self.update_metrics(False, time.time() - start_time)
                return None
                
            # Download CAPTCHA image
            image_path, success = self.download_captcha_image(
                captcha_url, 
                self.get_cookie(cookie_name)
            )
            
            if not success:
                return self.solve_captcha(
                    captcha_url, cookie_name, is_math_captcha, context, retries + 1
                )
            
            # Preprocess image
            processed_path = preprocess_captcha_image(image_path)
            if processed_path:
                # Detect CAPTCHA type if not specified
                if not is_math_captcha:
                    is_math_captcha = is_likely_math_captcha(processed_path)
                
                # Get text from processed image
                captcha_text = clean_captcha_text(get_text(str(processed_path)))
                processed_path.unlink()  # Clean up
            else:
                # Fallback to original image if preprocessing fails
                captcha_text = clean_captcha_text(get_text(str(image_path)))
            
            # Handle math vs text CAPTCHA
            if is_math_captcha:
                if self.is_math_expression(captcha_text):
                    try:
                        solution = self.solve_math_expression(captcha_text)
                        image_path.unlink()  # Clean up
                        
                        # Cache successful solution
                        self.solution_cache[cache_key] = solution
                        self.update_metrics(True, time.time() - start_time)
                        
                        return solution
                    except Exception as e:
                        logger.error(f"Failed to solve math CAPTCHA: {str(e)}")
                        # Move failed attempt to failures directory with context
                        fail_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                        new_name = f"failed_{fail_time}_{image_path.name}"
                        new_path = self.captcha_failures_dir / new_name
                        
                        # Save failure context
                        context_path = new_path.with_suffix('.json')
                        with open(context_path, 'w') as f:
                            failure_context = {
                                'time': fail_time,
                                'error': str(e),
                                'captcha_text': captcha_text,
                                'context': context or {}
                            }
                            json.dump(failure_context, f, indent=2)
                            
                        image_path.rename(new_path)
                        return self.solve_captcha(
                            captcha_url, cookie_name, is_math_captcha, context, retries + 1
                        )
                else:
                    # Not a valid math expression, retry
                    image_path.unlink()
                    return self.solve_captcha(
                        captcha_url, cookie_name, is_math_captcha, context, retries + 1
                    )
            else:
                # Text CAPTCHA validation
                if len(captcha_text) != 6:  # Expected length for text CAPTCHAs
                    image_path.unlink()
                    return self.solve_captcha(
                        captcha_url, cookie_name, is_math_captcha, context, retries + 1
                    )
                    
                # Cache successful solution
                self.solution_cache[cache_key] = captcha_text
                self.update_metrics(True, time.time() - start_time)
                return captcha_text
                
        except Exception as e:
            if context:
                log_error(logger, e, context)
            else:
                logger.error(f"CAPTCHA solving error: {str(e)}\n{traceback.format_exc()}")
                
            self.update_metrics(False, time.time() - start_time)
            return self.solve_captcha(
                captcha_url, cookie_name, is_math_captcha, context, retries + 1
            )