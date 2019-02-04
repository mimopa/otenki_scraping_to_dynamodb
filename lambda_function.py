from selenium import webdriver

def lambda_handler(event, context):
    options = webdriver.ChromeOptions()

    options.binary_location = "./bin/headless-chromium"

    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--single-process")
    
    driver = webdriver.Chrome(
        "./bin/chromedriver",
        chrome_options=options)
    driver.get("https://www.google.co.jp")
    title = driver.title
    print(title)
    driver.close()
    return title