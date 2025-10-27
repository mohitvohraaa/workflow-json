"""
OpenArt ComfyUI Workflow Scraper
Downloads and consolidates 2,000+ workflow JSON files from OpenArt
"""

import os
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Set
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

# Configuration
CONFIG = {
    'base_url': 'https://openart.ai/workflows/all',
    'target_count': 2000,
    'download_folder': './workflows',
    'merged_file': './workflows_merged.json',
    'scroll_pause': 2,  # seconds between scrolls
    'page_load_timeout': 30,  # seconds
    'headless': False,  # Set to True for headless mode
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OpenArtScraper:
    def __init__(self, config: Dict):
        self.config = config
        self.download_folder = Path(config['download_folder'])
        self.download_folder.mkdir(exist_ok=True)
        self.driver = None
        self.workflow_urls: Set[str] = set()
        self.downloaded_count = 0
        self.failed_downloads: List[str] = []
        
    def setup_driver(self):
        """Initialize Selenium WebDriver with Chrome"""
        chrome_options = Options()
        
        # Set download preferences
        prefs = {
            "download.default_directory": str(self.download_folder.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        if self.config['headless']:
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(30)
        logger.info("WebDriver initialized successfully")
        
    def scroll_and_collect_urls(self):
        """Scroll through the page and collect workflow URLs"""
        logger.info(f"Opening {self.config['base_url']}")
        self.driver.get(self.config['base_url'])
        time.sleep(5)  # Initial page load
        
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        no_new_content_count = 0
        max_no_new_attempts = 5
        
        while len(self.workflow_urls) < self.config['target_count']:
            # Find all workflow links on the current page
            try:
                # Look for workflow links - adjust selector based on actual page structure
                workflow_elements = self.driver.find_elements(
                    By.CSS_SELECTOR, 
                    'a[href*="/workflows/"]'
                )
                
                for element in workflow_elements:
                    try:
                        url = element.get_attribute('href')
                        if url and '/workflows/' in url and url not in self.workflow_urls:
                            # Filter out the base /workflows/all URL
                            if url != self.config['base_url'] and not url.endswith('/workflows/all'):
                                self.workflow_urls.add(url)
                    except StaleElementReferenceException:
                        continue
                
                logger.info(f"Collected {len(self.workflow_urls)} unique workflow URLs so far...")
                
            except Exception as e:
                logger.error(f"Error finding workflow elements: {e}")
            
            # Scroll down
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.config['scroll_pause'])
            
            # Check if new content loaded
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                no_new_content_count += 1
                if no_new_content_count >= max_no_new_attempts:
                    logger.warning(f"No new content after {max_no_new_attempts} attempts. Stopping scroll.")
                    break
            else:
                no_new_content_count = 0
                last_height = new_height
        
        logger.info(f"Total unique workflow URLs collected: {len(self.workflow_urls)}")
        return list(self.workflow_urls)
    
    def download_workflow(self, url: str, index: int):
        """Navigate to workflow page and download JSON"""
        try:
            logger.info(f"[{index}/{len(self.workflow_urls)}] Processing: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, self.config['page_load_timeout']).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)  # Additional wait for dynamic content
            
            # Try multiple selectors for download button
            download_button = None
            selectors = [
                "//button[contains(text(), 'Download')]",
                "//button[contains(text(), 'download')]",
                "//a[contains(text(), 'Download')]",
                "//button[contains(@class, 'download')]",
                "//a[contains(@href, '.json')]",
                "//button[.//span[contains(text(), 'Download')]]"
            ]
            
            for selector in selectors:
                try:
                    download_button = self.driver.find_element(By.XPATH, selector)
                    if download_button:
                        break
                except NoSuchElementException:
                    continue
            
            if not download_button:
                logger.warning(f"Download button not found for {url}")
                self.failed_downloads.append(url)
                return False
            
            # Get initial file count
            initial_files = set(os.listdir(self.download_folder))
            
            # Click download button
            download_button.click()
            time.sleep(3)  # Wait for download to start
            
            # Wait for new file to appear
            max_wait = 30
            elapsed = 0
            while elapsed < max_wait:
                current_files = set(os.listdir(self.download_folder))
                new_files = current_files - initial_files
                
                # Check for completed downloads (no .crdownload or .tmp files)
                completed_files = [f for f in new_files if not f.endswith(('.crdownload', '.tmp'))]
                
                if completed_files:
                    # Rename file to workflow_index.json
                    old_path = self.download_folder / completed_files[0]
                    new_path = self.download_folder / f"workflow_{index}.json"
                    
                    # If file already exists, add timestamp
                    if new_path.exists():
                        new_path = self.download_folder / f"workflow_{index}_{int(time.time())}.json"
                    
                    os.rename(old_path, new_path)
                    logger.info(f"Downloaded and saved as {new_path.name}")
                    self.downloaded_count += 1
                    return True
                
                time.sleep(1)
                elapsed += 1
            
            logger.warning(f"Download timeout for {url}")
            self.failed_downloads.append(url)
            return False
            
        except TimeoutException:
            logger.error(f"Page load timeout for {url}")
            self.failed_downloads.append(url)
            return False
        except Exception as e:
            logger.error(f"Error downloading workflow from {url}: {e}")
            self.failed_downloads.append(url)
            return False
    
    def merge_json_files(self):
        """Combine all downloaded JSON files into a single file"""
        logger.info("Starting JSON file consolidation...")
        
        merged_data = []
        json_files = list(self.download_folder.glob("workflow_*.json"))
        
        logger.info(f"Found {len(json_files)} JSON files to merge")
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    merged_data.append({
                        'filename': json_file.name,
                        'workflow': data
                    })
            except Exception as e:
                logger.error(f"Error reading {json_file.name}: {e}")
        
        # Save merged file
        merged_path = Path(self.config['merged_file'])
        with open(merged_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Merged {len(merged_data)} workflows into {merged_path}")
        return len(merged_data)
    
    def run(self):
        """Main execution flow"""
        try:
            logger.info("Starting OpenArt workflow scraper...")
            
            # Setup driver
            self.setup_driver()
            
            # Collect workflow URLs
            logger.info("Phase 1: Collecting workflow URLs...")
            workflow_urls = self.scroll_and_collect_urls()
            
            if len(workflow_urls) < self.config['target_count']:
                logger.warning(
                    f"Only found {len(workflow_urls)} workflows, "
                    f"less than target of {self.config['target_count']}"
                )
            
            # Download each workflow
            logger.info("Phase 2: Downloading individual workflows...")
            for idx, url in enumerate(workflow_urls, 1):
                self.download_workflow(url, idx)
                
                # Periodic progress update
                if idx % 50 == 0:
                    logger.info(f"Progress: {idx}/{len(workflow_urls)} - "
                              f"Success: {self.downloaded_count}, "
                              f"Failed: {len(self.failed_downloads)}")
            
            # Merge all JSON files
            logger.info("Phase 3: Merging JSON files...")
            merged_count = self.merge_json_files()
            
            # Final summary
            logger.info("\n" + "="*60)
            logger.info("SCRAPING COMPLETE")
            logger.info("="*60)
            logger.info(f"Total URLs collected: {len(workflow_urls)}")
            logger.info(f"Successfully downloaded: {self.downloaded_count}")
            logger.info(f"Failed downloads: {len(self.failed_downloads)}")
            logger.info(f"Merged workflows: {merged_count}")
            logger.info(f"Merged file location: {self.config['merged_file']}")
            
            if self.failed_downloads:
                logger.info("\nFailed URLs:")
                for url in self.failed_downloads[:10]:  # Show first 10
                    logger.info(f"  - {url}")
                if len(self.failed_downloads) > 10:
                    logger.info(f"  ... and {len(self.failed_downloads) - 10} more")
            
        except KeyboardInterrupt:
            logger.info("\nScraping interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed")


def main():
    """Entry point"""
    scraper = OpenArtScraper(CONFIG)
    scraper.run()


if __name__ == "__main__":
    main()