import pandas
import time
from urllib.parse import quote_plus, urlencode

from bs4 import BeautifulSoup
from google_comments.base import SpiderMixin
from selenium.webdriver.common.by import By

from google_comments import get_selenium_browser_instance


class GoogleSearch(SpiderMixin):
    # collected_search = []

    def __init__(self, output_folder=None):
        self.driver = None
        self.collected_search = pandas.DataFrame(data, columns=['title', 'link', 'url', 'address', 'telephone'])
        super().__init__(output_folder=output_folder)
    
    def click_consent(self):
        """Function that clicks on the cookie 
        consent button"""
        script = """
        const buttonElement = document.querySelector('.KxvlWc')
        const xpath = '//div[contains(text(), "Tout accepter")][@role="none"]'
        const el = document.evaluate(xpath, buttonElement, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
        el && el.click()
        """
        try:
            self.driver.execute_script(script)
        except:
            return False
        else:
            time.sleep(5)
            return True

    def before_launch(self):
        pass

    def start_spider(self, search):
        self.before_launch()    
        self.driver = get_selenium_browser_instance()

        search = quote_plus(search)
        query = urlencode({'q': search})
        self.driver.get(f'https://www.google.com/search?{query}')
        self.click_consent()

        element = self.driver.find_element(By.TAG_NAME, 'body')
        html = element.get_attribute('innerHTML')

        soup = BeautifulSoup(html, 'html.parser')
        elements = soup.find_all('div', {'class': 'TzHB6b'})

        contact_infos = self.driver.execute_script(
            """
            function evaluateXpath(xpath) {
                return document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
            }

            function getText(el) {
                return el && el.textContent
            }

            const addressElement = evaluateXpath('//a[contains(@href, "/maps/place/")]')
            const telephone = getText(evaluateXpath('//span[contains(@aria-label, "Appeler le")]'))

            return {
                url: addressElement && addressElement.href,
                address: getText(addressElement),
                telephone
            }
            """
        )

        data = []
        for element in elements:
            item = {'title': None: 'link': None} | contact_infos
            h3_tag = element.find('h3')
            if h3_tag is not None:
                title = h3_tag.text
                item['title'] = title
            
                link_tag = element.find('a', {'jsname': 'UWckNb'})
                if link_tag is not None:
                    link = link_tag.attrs.get('href')
                    item['link'] = link
                self.collected_search.append(item)
        
        df = pandas.DataFrame(data)

        time.sleep(1)

        # text_area = self.driver.execute_script(
        #     """
        #     return document.querySelector('textarea[type="search"]')
        #     """
        # )
        # if text_area is not None:
        #     pass

        time.sleep(10)
        self.driver.quit()


s = GoogleSearch()
s.start_spider('Centre Commercial NICETOILE')
