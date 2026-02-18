"""
Castos Hosting Service
======================
Adapted from clean_podcast hosting.py for newsletter-ai.
Provides Castos API integration with Selenium RPA for private feeds.
"""
import logging
import os
import requests
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime
import mimetypes
import time 
import re
from bs4 import BeautifulSoup
import shutil
from urllib.parse import urlparse
import json

# Settings equivalents using env vars
class _Settings:
    CASTOS_API_TOKEN = os.getenv("CASTOS_API_TOKEN")
    CASTOS_USERNAME = os.getenv("CASTOS_USERNAME")
    CASTOS_PASSWORD = os.getenv("CASTOS_PASSWORD")
    DEFAULT_LLM_PROVIDER = os.getenv("DEFAULT_LLM_PROVIDER", "openai")
    DEFAULT_COVER_IMAGE_PATH = os.getenv("DEFAULT_COVER_IMAGE_PATH", "")

settings = _Settings()

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.getLogger(__name__).warning("Selenium library not found. UI automation for Castos will not be available.")

logger = logging.getLogger(__name__)

class CastosUploader:
    def __init__(self, podcast_id: Optional[str] = None):
        self.podcast_id = podcast_id
        self.base_url = "https://app.castos.com/api/v2"
        if not settings.CASTOS_API_TOKEN:
            self.headers = {}
        else:
            self.headers = {"Authorization": f"Bearer {settings.CASTOS_API_TOKEN}", "Accept": "application/json"}

    def _format_episode_title(self, title: str, market: str, is_microlearning: bool = False) -> str:
        # Limpiar el título de posibles duplicados previos de (Summary), (Resumen), (Microlearning), (Síntesis) y otros idiomas
        clean_title = re.sub(r'^\((Microlearning|Summary|Resumen|Síntesis|Riassunto|Résumé|Zusammenfassung|Resumo|摘要|总结|Резюме|ملخص|सारांश|概要|まとめ|Περίληψη)\)\s*-?\s*', '', title, flags=re.IGNORECASE)
        clean_title = re.sub(r'\s*\((Summary|Resumen|Síntesis|Riassunto|Résumé|Zusammenfassung|Resumo|摘要|总结|Резюме|ملخص|सारांश|概要|まとめ|Περίληψη)\)$', '', clean_title, flags=re.IGNORECASE)
        
        if is_microlearning:
            return f"(Microlearning) {clean_title}"
        
        # Mapeo manual de traducciones para evitar llamadas al LLM
        summary_prefixes = {
            'es': '(Resumen)',
            'en': '(Summary)',
            'fr': '(Résumé)',
            'it': '(Riassunto)',
            'de': '(Zusammenfassung)',
            'pt': '(Resumo)',
            'zh': '(摘要)',
            'ru': '(Резюме)',
            'ar': '(ملخص)',
            'hi': '(सारांश)',
            'ja': '(概要)',
            'el': '(Περίληψη)'
        }
        
        lang_code = market.lower()[:2] # Tomar solo los 2 primeros caracteres si viene con dialecto
        prefix = summary_prefixes.get(lang_code, '(Summary)')
        
        return f"{prefix} {clean_title}"

    def _fetch_podcast_details(self, podcast_id: str) -> Optional[Dict[str, Any]]:
        if not self.headers or not podcast_id: return None
        try:
            response = requests.get(f"{self.base_url}/podcasts/{podcast_id}", headers=self.headers, timeout=15)
            response.raise_for_status()
            full_response_data = response.json()
            
            podcast_data = None
            if isinstance(full_response_data, dict) and full_response_data.get('success'):
                data_section = full_response_data.get("data")
                if isinstance(data_section, dict) and data_section.get("title"): # Usamos 'title' que sí existe
                    podcast_data = data_section
            
            if podcast_data:
                logger.info(f"Successfully fetched and parsed podcast details for ID {podcast_id}")
                return podcast_data
            else:
                logger.warning(f"No valid podcast data found in expected response structure for ID {podcast_id}")
                logger.critical(f"CASTOS UNHANDLED JSON (_fetch_podcast_details): {full_response_data}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching details for podcast ID {podcast_id}: {e}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON from Castos API for podcast ID {podcast_id}")
            return None

    def _set_podcast_private_via_ui(self, podcast_api_id: str, podcast_title_for_nav: str) -> bool:
        if not SELENIUM_AVAILABLE:
            logger.error("Selenium library not available. Cannot perform UI automation.")
            return False
        if not settings.CASTOS_USERNAME or not settings.CASTOS_PASSWORD:
            logger.error("Castos username or password not configured. Cannot perform UI automation.")
            return False
        
        logger.info(f"Attempting to set podcast '{podcast_title_for_nav}' to private via UI...")
        
        options = ChromeOptions()
        
        # PRODUCTION: Force Headless for Cloud Run / Docker
        # Always use --headless=new for better compatibility
        options.add_argument("--headless=new") 
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")
        
        # Opciones adicionales para evitar detección
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # SET BINARY LOCATIONS FOR DOCKER SPECIFICALLY
        chrome_bin = os.getenv("CHROME_BIN")
        if chrome_bin:
             options.binary_location = chrome_bin
        
        driver = None
        try:
            # Set Driver Path explicitly if env var exists
            driver_path = os.getenv("CHROMEDRIVER_PATH")
            if driver_path:
                service = ChromeService(executable_path=driver_path)
            else:
                service = ChromeService()
                
            driver = webdriver.Chrome(service=service, options=options)
            
            # Ocultar webdriver
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            })
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            wait = WebDriverWait(driver, 30)
            
            logger.info("Navigating to Castos login page...")
            driver.get("https://app.castos.com/login")
            
            time.sleep(3)
            logger.info("Waiting for login form...")
            
            # Intentar múltiples selectores para el email
            email_selectors = [
                (By.ID, "email"),
                (By.NAME, "email"),
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.XPATH, "//input[@type='email']")
            ]
            
            email_field = None
            for by, selector in email_selectors:
                try:
                    email_field = wait.until(EC.presence_of_element_located((by, selector)))
                    logger.info(f"Found email field with: {by}={selector}")
                    break
                except TimeoutException:
                    continue
            
            if not email_field:
                logger.error("Could not find email field")
                return False
            
            logger.info("Sending email keys...")
            email_field.send_keys(settings.CASTOS_USERNAME)
            
            logger.info("Waiting for password field...")
            password_field = wait.until(EC.presence_of_element_located((By.ID, "password")))
            password_field.send_keys(settings.CASTOS_PASSWORD)
            
            # Buscar el botón SIGN IN / LOG IN
            # Usar un wait corto para iterar rápido por los selectores
            short_wait = WebDriverWait(driver, 3) 
            
            submit_selectors = [
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.CSS_SELECTOR, "input[type='submit']"),
                (By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sign in')]"),
                (By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'login')]"),
                (By.XPATH, "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'log in')]"),
                (By.CLASS_NAME, "btn-primary"),
                (By.TAG_NAME, "button") # Fallback agresivo
            ]
            
            submit_button = None
            for by, selector in submit_selectors:
                try:
                    logger.info(f"Checking selector: {selector}")
                    submit_button = short_wait.until(EC.element_to_be_clickable((by, selector)))
                    logger.info(f"Found submit button with: {by}={selector}")
                    break
                except TimeoutException:
                    continue
            
            if not submit_button:
                logger.error("Could not find submit button")
                return False
            
            # Click con JS para evitar problemas de intercepción
            driver.execute_script("arguments[0].click();", submit_button)
            
            wait.until(lambda d: "podcasts" in d.current_url.lower() or "dashboard" in d.current_url.lower())
            logger.info("Login successful")
            time.sleep(2)
            
            # Navigation per User Instruction: "Distribution" -> "Visibility"
            distribution_url = f"https://app.castos.com/podcasts/{podcast_api_id}/edit/distribution/visibility"
            logger.info(f"Navigating to visibility page: {distribution_url}")
            driver.get(distribution_url)
            
            logger.info("Waiting for Visibility page content...")
            time.sleep(3)
            
            # Check if it is ALREADY private to avoid unnecessary clicks
            try:
                already_private = driver.find_elements(By.XPATH, "//*[contains(text(), 'This podcast is currently Private')]")
                if already_private:
                    logger.info("Podcast appears to be ALREADY PRIVATE. Skipping 'Change' sequence.")
                    return True
            except:
                pass

            logger.info("Looking for the 'Change' button...")
            change_button = None
            selectors = [
                "//button[contains(., 'Change')]",
                "//a[contains(., 'Change')]",
                "//*[@role='button'][contains(., 'Change')]",
                "//button[contains(@class, 'change') or contains(@id, 'change')]"
            ]
            
            for selector in selectors:
                try:
                    change_button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    logger.info(f"Found change button with selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if not change_button:
                logger.warning("Could not find 'Change' button. Checking if it's already private again...")
                if driver.find_elements(By.XPATH, "//*[contains(text(), 'Private')]"):
                     logger.info("Found text 'Private', assuming success.")
                     return True
                logger.error("Could not find Change button and podcast does not look private.")
                return False
            
            logger.info(f"Change button text: '{change_button.text}' - enabled: {change_button.is_enabled()}")
            driver.execute_script("arguments[0].click();", change_button)
            logger.info("Clicked 'Change' button")
            time.sleep(2)
            
            logger.info("Looking for the 'Select Private' button...")
            private_selectors = [
                "//button[contains(., 'Select Private & Continue')]",
                "//button[contains(., 'Select Private')]",
                "//button[contains(., 'Private')]",
                "//*[@role='button'][contains(., 'Private')]"
            ]
            
            select_private_button = None
            for selector in private_selectors:
                try:
                    select_private_button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    logger.info(f"Found private button with selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if not select_private_button:
                logger.error("Could not find Select Private button")
                return False
            
            logger.info(f"Private button text: '{select_private_button.text}' - enabled: {select_private_button.is_enabled()}")
            driver.execute_script("arguments[0].click();", select_private_button)
            logger.info("Clicked 'Select Private' button")
            time.sleep(2)
            
            logger.info("Looking for the 'Next' button...")
            next_selectors = [
                "//button[contains(., 'Next')]",
                "//button[contains(., 'Continue')]",
                "//*[@role='button'][contains(., 'Next')]"
            ]
            
            next_button = None
            for selector in next_selectors:
                try:
                    next_button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    logger.info(f"Found next button with selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if not next_button:
                logger.error("Could not find Next button")
                return False
            
            logger.info(f"Next button text: '{next_button.text}' - enabled: {next_button.is_enabled()}")
            driver.execute_script("arguments[0].click();", next_button)
            logger.info("Clicked 'Next' button")
            time.sleep(2)
            
            logger.info("Looking for the final confirmation button...")
            confirm_selectors = [
                "//button[contains(., 'Change Now')]",
                "//button[contains(., 'Confirm')]",
                "//button[contains(., 'Apply')]",
                "//button[contains(., 'Save')]",
                "//*[@role='button'][contains(., 'Change Now')]"
            ]
            
            confirm_change_button = None
            for selector in confirm_selectors:
                try:
                    confirm_change_button = wait.until(EC.element_to_be_clickable((By.XPATH, selector)))
                    logger.info(f"Found confirm button with selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if not confirm_change_button:
                logger.error("Could not find final confirmation button")
                return False
            
            logger.info(f"Confirm button text: '{confirm_change_button.text}' - enabled: {confirm_change_button.is_enabled()}")
            driver.execute_script("arguments[0].click();", confirm_change_button)
            logger.info("Clicked final confirmation button")
            
            logger.info("Waiting for confirmation message...")
            try:
                success_message = wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//*[contains(text(), 'updated') or contains(text(), 'Updated') or contains(text(), 'changed') or contains(text(), 'private') or contains(text(), 'Private') or contains(text(), 'success')]")
                ))
                logger.info(f"Confirmation message found: '{success_message.text}'")
            except TimeoutException:
                logger.warning("No specific confirmation message found, checking page state...")
            
            logger.info("Waiting for changes to be processed...")
            time.sleep(5)
            
            logger.info("Verifying the change by refreshing the page...")
            driver.refresh()
            time.sleep(3)
            
            try:
                private_indicators = [
                    "//*[contains(text(), 'Private')]",
                    "//*[contains(text(), 'private')]",
                    "//*[contains(@class, 'private')]",
                    "//span[contains(text(), 'Private')]",
                    "//div[contains(text(), 'Private')]"
                ]
                
                private_found = False
                for indicator in private_indicators:
                    try:
                        element = driver.find_element(By.XPATH, indicator)
                        logger.info(f"Private status confirmed - found element with text: '{element.text}'")
                        private_found = True
                        break
                    except:
                        continue
                
                if not private_found:
                    logger.warning("Could not visually confirm private status on page")
            
            except Exception as e:
                logger.warning(f"Error while verifying private status: {e}")
            
            logger.info(f"Process completed for podcast '{podcast_title_for_nav}'")
            return True
        
        except TimeoutException as e:
            logger.error(f"TimeoutException: An element was not found in time. The Castos UI may have changed. Error: {e}", exc_info=True)
            if driver:
                screenshot_path = f"error_timeout_{podcast_api_id}_{int(time.time())}.png"
                driver.save_screenshot(screenshot_path)
                logger.info(f"Screenshot saved: {screenshot_path}")
                
                try:
                    current_url = driver.current_url
                    page_source_snippet = driver.page_source[:1000] + "..." if len(driver.page_source) > 1000 else driver.page_source
                    logger.debug(f"Current URL: {current_url}")
                    logger.debug(f"Page source snippet: {page_source_snippet}")
                except:
                    pass
            return False
        
        except Exception as e:
            logger.error(f"An unexpected error occurred during UI automation: {e}", exc_info=True)
            if driver:
                screenshot_path = f"error_ui_{podcast_api_id}_{int(time.time())}.png"
                driver.save_screenshot(screenshot_path)
                logger.info(f"Screenshot saved: {screenshot_path}")
            return False
        
        finally:
            if driver:
                driver.quit()

    def _get_private_feed_via_ui(self, podcast_api_id: str) -> Optional[str]:
        if not SELENIUM_AVAILABLE:
            logger.error("Selenium library not available. Cannot perform UI automation to get feed URL.")
            return None
        
        logger.info(f"Attempting to get Generic Private Feed for podcast {podcast_api_id} via UI...")
        
        options = ChromeOptions()
        # Always use --headless=new for consistent behavior in production
        options.add_argument("--headless=new")
        
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36")
        
        # Opciones adicionales para evitar detección
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # SET BINARY LOCATIONS FOR DOCKER SPECIFICALLY
        chrome_bin = os.getenv("CHROME_BIN")
        if chrome_bin:
             options.binary_location = chrome_bin
             
        driver = None
        try:
            # Set Driver Path explicitly if env var exists
            driver_path = os.getenv("CHROMEDRIVER_PATH")
            if driver_path:
                service = ChromeService(executable_path=driver_path)
            else:
                service = ChromeService()
                
            driver = webdriver.Chrome(service=service, options=options)
            
            # Ocultar webdriver
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            })
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            wait = WebDriverWait(driver, 30)  # Aumentado a 30 segundos
            
            logger.info("Navigating to Castos login page (for feed retrieval)...")
            driver.get("https://app.castos.com/login")
            
            # Tomar screenshot para ver qué está cargando
            time.sleep(3)
            driver.save_screenshot(f"login_page_{podcast_api_id}.png")
            logger.info(f"Screenshot saved. Page title: {driver.title}")
            
            logger.info("Waiting for login form...")
            
            # Intentar múltiples selectores para el email
            email_selectors = [
                (By.ID, "email"),
                (By.NAME, "email"),
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.XPATH, "//input[@type='email']")
            ]
            
            email_field = None
            for by, selector in email_selectors:
                try:
                    email_field = wait.until(EC.presence_of_element_located((by, selector)))
                    logger.info(f"Found email field with: {by}={selector}")
                    break
                except TimeoutException:
                    continue
            
            if not email_field:
                logger.error("Could not find email field")
                return None
            
            email_field.send_keys(settings.CASTOS_USERNAME)
            driver.find_element(By.ID, "password").send_keys(settings.CASTOS_PASSWORD)
            
            # Intentar múltiples selectores para el botón submit
            submit_selectors = [
                (By.XPATH, "//button[contains(., 'SIGN IN')]"),
                (By.XPATH, "//button[normalize-space(text())='SIGN IN']"),
                (By.XPATH, "//button[contains(normalize-space(text()), 'SIGN IN')]"),
                (By.XPATH, "//button[contains(@class, 'btn') and contains(text(), 'SIGN')]"),
                (By.CSS_SELECTOR, "button[type='submit']"),
                (By.XPATH, "//button")  # Como último recurso, cualquier botón
            ]

            submit_button = None
            for by, selector in submit_selectors:
                try:
                    submit_button = wait.until(EC.element_to_be_clickable((by, selector)))
                    logger.info(f"Found submit button with: {by}={selector}, text: {submit_button.text}")
                    break
                except TimeoutException:
                    continue
            
            if not submit_button:
                logger.error("Could not find submit button")
                driver.save_screenshot(f"no_submit_button_{podcast_api_id}.png")
                return None
            
            submit_button.click()
            
            logger.info("Login form submitted, waiting for redirect...")
            wait.until(lambda d: "podcasts" in d.current_url.lower() or "dashboard" in d.current_url.lower())
            logger.info("Login successful")
            
            # Navigation per User Instruction: "Distribution" -> "Visibility"
            distribution_url = f"https://app.castos.com/podcasts/{podcast_api_id}/edit/distribution/visibility"
            logger.info(f"Navigating to visibility page: {distribution_url}")
            driver.get(distribution_url)
            
            logger.info("Waiting for Visibility page content...")
            time.sleep(3)
            
            logger.info("Looking for the 'View Generic Private Feed' button...")
            view_generic_button = None
            
            # Button selectors
            button_selectors = [
                 "//button[contains(., 'View Generic Private Feed')]",
                 "//a[contains(., 'View Generic Private Feed')]",
                 "//*[contains(text(), 'View Generic Private Feed')]",
                 "//button[contains(., 'Generic Private Feed')]"
            ]
            
            for sel in button_selectors:
                try:
                     view_generic_button = wait.until(EC.element_to_be_clickable((By.XPATH, sel)))
                     logger.info(f"Found Generic Feed button with: {sel}")
                     break
                except:
                    continue

            if not view_generic_button:
                 logger.error("Could not find 'View Generic Private Feed' button on visibility page.")
                 # Fallback: Check main distribution tab just in case? 
                 # Driver.get(... distribution) ...
                 # For now, let's assume user is right about visibility.
                 return None
            
            driver.execute_script("arguments[0].click();", view_generic_button)
            
            logger.info("Waiting for private feed modal to appear...")
            time.sleep(2)
            
            url_selectors = [
                "//input[contains(@value, 'uuid=')]",
                "//input[contains(@value, 'private')]",
                "//*[self::div or self::code or self::p or self::span][contains(text(), 'uuid=')]",
                "//textarea[contains(@value, 'uuid=')]"
            ]
            
            url_element = None
            for selector in url_selectors:
                try:
                    logger.info(f"Looking for URL with selector: {selector}")
                    url_element = wait.until(EC.presence_of_element_located((By.XPATH, selector)))
                    logger.info(f"Found URL element with selector: {selector}")
                    break
                except TimeoutException:
                    continue
            
            if not url_element:
                logger.error("Could not find URL element in modal")
                driver.save_screenshot(f"modal_url_not_found_{podcast_api_id}.png")
                return None
            
            feed_url = url_element.get_attribute('value') or url_element.text
            
            if feed_url and "uuid=" in feed_url:
                clean_feed_url = feed_url.strip()
                logger.info(f"Successfully extracted Generic Private Feed URL: {clean_feed_url}")
                return clean_feed_url
            
            logger.error("Could not extract valid feed URL from the modal window.")
            return None
        
        except Exception as e:
            logger.error(f"Error during UI automation for feed retrieval: {e}", exc_info=True)
            if driver:
                driver.save_screenshot(f"error_feed_retrieval_{podcast_api_id}.png")
            return None
        
        finally:
            if driver:
                driver.quit()
                
    def get_or_create_podcast_id_by_title(
        self,
        podcast_title_target: str,
        market_for_language: str = "en",
        private: bool = True 
    ) -> Tuple[Optional[str], Optional[str]]:
        if not self.headers: return None, None
        try:
            response = requests.get(f"{self.base_url}/podcasts", headers=self.headers, timeout=15)
            response.raise_for_status()
            podcasts_data_response = response.json().get("data", {}).get("podcast_list", {})

            for pid_str, title_from_list in podcasts_data_response.items():
                if title_from_list == podcast_title_target:
                    logger.info(f"Found existing podcast: Title '{title_from_list}', API ID {pid_str}")
                    final_feed_url = self._get_private_feed_via_ui(pid_str)
                    if final_feed_url:
                        return pid_str, final_feed_url
                    
                    logger.warning("UI automation for feed retrieval failed. Falling back to API call.")
                    details = self._fetch_podcast_details(pid_str)
                    if details:
                        return pid_str, details.get("rss_url")
                    return pid_str, None
            
            logger.info(f"Podcast '{podcast_title_target}' not found. Creating new one...")
            created_data = self.create_podcast_with_cover(
                podcast_title=podcast_title_target,
                market_for_language=market_for_language,
                private=private 
            )
            return created_data if created_data else (None, None)
        except Exception as e:
            logger.error(f"Error in get_or_create_podcast_id_by_title: {e}", exc_info=True)
            return None, None

    def create_podcast_with_cover(
        self,
        podcast_title: str,
        market_for_language: str = "en",
        private: bool = True
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Orchestrates podcast creation and sets it to private if requested.
        """
        podcast_api_id, base_feed_url = self._create_podcast_api_call(podcast_title, market_for_language, private)

        if not podcast_api_id:
            logger.error(f"Podcast creation failed for title '{podcast_title}'. Aborting.")
            return None, None

        logger.info(f"Podcast '{podcast_title}' created successfully via API with ID: {podcast_api_id}.")

        if private:
            logger.info(f"Private flag is set. Attempting to set podcast {podcast_api_id} to private via UI.")
            
            privacy_set_successfully = self._set_podcast_private_via_ui(podcast_api_id, podcast_title)

            if privacy_set_successfully:
                logger.info(f"Successfully set podcast to private. Now retrieving the private feed URL.")
                final_feed_url = self._get_private_feed_via_ui(podcast_api_id)
                return podcast_api_id, final_feed_url or base_feed_url
            else:
                logger.error(f"Failed to set podcast {podcast_api_id} to private. The podcast remains public.")
                return podcast_api_id, base_feed_url
        
        logger.info(f"Podcast {podcast_api_id} is public. Skipping privacy settings.")
        return podcast_api_id, base_feed_url
        
    def _create_podcast_api_call(self, podcast_title: str, market_for_language: str, private: bool) -> Tuple[Optional[str], Optional[str]]:
        """
        Creates a podcast via API.
        """
        url = f"{self.base_url}/podcasts"
        headers_post = self.headers.copy()
        
        # Simple fixed descriptions by language
        descriptions = {
            "es": "Tu resumen diario de noticias personalizado.",
            "en": "Your personalized daily news summary.",
        }
        podcast_description = descriptions.get(market_for_language[:2].lower(), descriptions["en"])
        
        data_payload = {
            "podcast_title": podcast_title,
            "podcast_description": podcast_description,
            "website": "https://podsummarizer.xyz/",
            "author_name": "PodSummarizer",
            "podcast_owner": "PodSummarizer",
            "owner_email": "psummarizer@gmail.com",
            "copyright": "Copyright © 2025 PodSummarizer",
            "language": market_for_language.lower()[:2],
            "feed_episodes_number": "1000"
        }
        try:
            logger.info(f"Creating podcast with title '{podcast_title}'...")
            response = requests.post(url, headers=headers_post, json=data_payload, timeout=30)
            response.raise_for_status()
            response_data = response.json()
            podcast_info = response_data.get("data", {}).get("podcast", {})
            podcast_id = str(podcast_info.get("id"))
            base_feed_url = podcast_info.get("feed_url")
            if not podcast_id:
                logger.error("Podcast creation succeeded but no ID was returned.")
                return None, None
            logger.info(f"Podcast created with ID: {podcast_id}")
            return podcast_id, base_feed_url
        except requests.exceptions.RequestException as e:
            logger.error(f"Error in API call for podcast creation: {e}")
            return None, None
    
    def upload_episode(
        self, podcast_name: str, episode_title: str, episode_description: str,
        audio_file_path: str, market: str,
        episode_image_path: Optional[str] = None,
        is_microlearning: bool = False
    ) -> Optional[Tuple[str, str]]:
        if not self.podcast_id or not self.headers:
            logger.error("Cannot upload episode: Missing podcast_id or authorization headers.")
            return None
        
        audio_path = Path(audio_file_path)
        if not audio_path.exists():
            logger.error(f"Cannot upload episode: Audio file not found at {audio_file_path}")
            return None

        formatted_title = self._format_episode_title(episode_title, market, is_microlearning=is_microlearning)
        url = f"{self.base_url}/podcasts/{self.podcast_id}/episodes"
        payload = {
            "post_title": formatted_title,
            "post_content": episode_description,
            "private": "true"
        }
        
        image_file_handle = None
        try:
            with open(audio_path, 'rb') as audio_file:
                files: Dict[str, Any] = {
                    'episode_file': (
                        audio_path.name,
                        audio_file,
                        mimetypes.guess_type(audio_path.name)[0] or 'audio/mpeg'
                    )
                }
                
                if episode_image_path and Path(episode_image_path).exists():
                    img_path_obj = Path(episode_image_path)
                    image_file_handle = open(img_path_obj, 'rb')
                    files['post_image'] = (
                        img_path_obj.name,
                        image_file_handle,
                        mimetypes.guess_type(img_path_obj.name)[0] or 'image/jpeg'
                    )

                headers_upload = self.headers.copy()
                headers_upload.pop('Content-Type', None)
                
                logger.info(f"Uploading episode '{formatted_title}' to Castos podcast ID {self.podcast_id}...")
                response = requests.post(url, headers=headers_upload, data=payload, files=files, timeout=900)
                response.raise_for_status()
                
                response_data = response.json()
                
                episode_obj = response_data.get("episode", {})
                file_obj = response_data.get("file", {})

                share_url = episode_obj.get("guid")
                direct_download_url = file_obj.get("file_path")

                if share_url and direct_download_url:
                    logger.info(f"Castos episode uploaded. Share Link (guid): {share_url}, Direct Audio URL: {direct_download_url}")
                    return share_url, direct_download_url
                else:
                    logger.error(f"Castos upload succeeded, but could not find 'guid' or 'file_path' in response. Full response: {response_data}")
                    return None

        except requests.exceptions.HTTPError as e:
            error_details = "No response body."
            try:
                error_details = e.response.json()
            except Exception:
                error_details = e.response.text
            logger.error(f"Castos episode upload failed with HTTP error {e.response.status_code}: {error_details}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Castos episode upload failed with an unexpected error: {e}", exc_info=True)
            return None
        finally:
            if image_file_handle:
                image_file_handle.close()
                
    def get_and_download_castos_assets(self, share_url: str, temp_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
        """
        Descarga un asset de audio desde una URL directa de Castos.
        """
        logger.info(f"Attempting to download direct audio asset from Castos URL: {share_url}")

        try:
            path = urlparse(share_url).path
            if not (path and path.lower().endswith(('.mp3', '.m4a', '.wav', '.aac', '.ogg'))):
                 logger.warning(f"URL '{share_url}' does not appear to be a direct audio link. Proceeding anyway, but it may fail.")

        except Exception as e:
            logger.error(f"Could not parse the provided URL '{share_url}': {e}")
            return None, None

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            with requests.get(share_url, stream=True, headers=headers, timeout=600) as r:
                r.raise_for_status()
                
                try:
                    audio_filename = Path(urlparse(share_url).path).name
                    if not audio_filename or '.' not in audio_filename:
                        audio_filename = f"episode_audio_{int(time.time())}.mp3"
                except:
                    audio_filename = f"episode_audio_{int(time.time())}.mp3"
                
                local_audio_path = temp_dir / audio_filename
                
                with open(local_audio_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                logger.info(f"Successfully downloaded audio directly to: {local_audio_path}")
                return local_audio_path, None
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"Direct Castos audio URL not found (404): {share_url}")
            else:
                logger.error(f"HTTP error during direct download from {share_url}: {e}")
            return None, None
        except Exception as e:
            logger.error(f"An unexpected error occurred during direct download: {e}", exc_info=True)
            return None, None