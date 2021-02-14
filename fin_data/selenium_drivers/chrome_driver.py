from typing import List

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement


class ChromeDriver:
    def __init__(self, path="chromedriver.exe"):
        self.options = webdriver.ChromeOptions()
        self.path = path

    def __enter__(self):
        self.driver = webdriver.Chrome(
            executable_path=self.path, options=self.options
        )
        # self.driver.implicitly_wait(2)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.driver.close()

    @staticmethod
    def get_text_from_element(elem: WebElement) -> str:
        return elem.text

    @staticmethod
    def get_text_from_elements(elems: List[WebElement]) -> List[str]:
        return [elem.text for elem in elems]

    @staticmethod
    def get_attribute_from_elements(
        elems: List[WebElement], attribute: str
    ) -> List[str]:
        return [elem.get_attribute(attribute) for elem in elems]
