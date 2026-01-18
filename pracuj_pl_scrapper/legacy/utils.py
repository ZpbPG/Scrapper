import threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.page_load_strategy = "eager"
chrome_options.add_experimental_option("prefs", {
    "profile.managed_default_content_settings.images": 2,
    "profile.managed_default_content_settings.stylesheets": 2,
    "profile.managed_default_content_settings.fonts": 2,
})
chrome_options.add_argument("--disable-search-engine-choice-screen")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-features=VizDisplayCompositor")
chrome_options.add_argument("--log-level=3")
chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

thread_local = threading.local()

def get_driver():
    """Get a WebDriver instance for the current thread."""
    if not hasattr(thread_local, "driver"):
        thread_local.driver = webdriver.Chrome(options=chrome_options)
    return thread_local.driver

def quit_drivers():
    """Quit all WebDriver instances at the end of scraping."""
    if hasattr(thread_local, "driver"):
        thread_local.driver.quit()
