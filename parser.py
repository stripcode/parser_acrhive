from urllib.request import urlopen
import lxml.html as html
import logging
import csv
from concurrent.futures import ThreadPoolExecutor

host = r'https://www.example.ru';

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s  %(message)s')


def get_regions(url):
    logging.info('Попытка получить регионы области по url={}'.format(url))
    root = html.parse(urlopen(url)).getroot()
    return root.cssselect('table.col_list a:not(.disable)')


def get_cities_elements(element):
    logging.info('Попытка получить города региона "{}" по url={}'.format(element.text, element.get('href')))
    root = html.parse(urlopen(host + element.get('href'))).getroot()
    return root.cssselect('table.col_list a:not(.disable)')


def get_companies_elements(element):
    logging.info('Попытка получить компании города "{}" по url={}'.format(element.text, element.get('href')))
    root = html.parse(urlopen(host + element.get('href') + r'&sort=houses&order=desc&page=1&limit=2000')).getroot()
    return root.cssselect('div.grid tbody a')


def analize_company(url):
    link = url.replace(r'profile', r'profile/profile')
    logging.info('Попытка получить компанию по url={}'.format(link))
    root = html.parse(urlopen(host + link, timeout=10)).getroot()
    location = root.cssselect('.location a')
    obl = location[1].text
    region = location[2].text
    city = location[3].text
    r = root.cssselect('table.col_list tr:not(.left) td:last-child > span')
    name = r[0].text
    ur_address = r[6].text
    fact_address = r[6].text
    post_address = r[8].text
    email = r[16].text
    site = r[17].text
    houses = r[20].text
    shape = r[21].text
    return [obl, region, city, name, email, site, houses, shape, ur_address, fact_address, post_address]


def process_obl(reg):
    logging.info('Попытка обработать область "{}" по ссылке {} '.format(reg.text, reg.get('href')))
    companies = set()
    for region in get_regions(host + reg.get('href')):
        for city_el in get_cities_elements(region):
            for company_el in get_companies_elements(city_el):
                if company_el.text not in companies:
                    companies.add(company_el.get('href'))
                    logging.info('Количество компаний для обработки: "{}" '.format(len(companies)))

    with open(reg.text + '.csv', 'w') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=';')
        for url in companies:
            try:
                row = analize_company(url)
                spamwriter.writerow(row)
            except Exception:
                logging.error('Невозможно получить данные по url={}'.format(url))


def get_obls():
    root = html.parse(urlopen(r'https://www.example.ru/mymanager')).getroot()
    regions = root.cssselect('table.col_list a:not(.disable)')
    return [r for r in regions if r.text != 'Прочие организации']


def test():
    for reg in get_obls():
        process_obl(reg)



if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=10) as executor:
        for reg in get_obls():
            future = executor.submit(process_obl, reg)
    logging.info('Парсинг завершен')
