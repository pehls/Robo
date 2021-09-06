import os
import time
import requests
import ndjson
import pandas as pd
import json
import io
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
from bs4 import BeautifulSoup
from pandas.io.json import json_normalize
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

## Declaração das variáveis para envio ao bigquery
key_path = "C:/Robo/FinanceBOT.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

data_e_hora_atuais = datetime.now()
data_e_hora_em_texto = data_e_hora_atuais.strftime('%d%m%Y_%H%M')
os.chdir('C:/Robo/Dados')

dados_totais = {'imgs': [], 'links':[], 'text':[], 'stars':[], 'price':[], 'economy':[]}
dados_totais = pd.DataFrame(data=dados_totais)

# Aqui, uso o webdriver do chrome
browser = webdriver.Chrome()

# O send keys "toma o controle" do Chrome para envio de comandos
body = browser.find_element_by_tag_name("body")
body.send_keys(Keys.CONTROL + 't')

# O browser.get envia o site para acessar
browser.get("https://www.amazon.com.br/ref=nav_logo")
time.sleep(2)

# Procuramos através do inspecionar a text box para pesquisar, e pegando o "full path" através do botão direito na aba de inspeção do botão para o search na amazon, com o evento de click
search_box = browser.find_element_by_id("twotabsearchtextbox")
search_box.send_keys("fralda RN")
browser.find_element_by_xpath('//*[@id="nav-search-submit-button"]').click()
time.sleep(2)

browser.find_element_by_class_name('a-button-dropdown').click()
time.sleep(2)

browser.find_element_by_id('s-result-sort-select_1').click()
time.sleep(2)

n_paginas = int(BeautifulSoup(browser.find_element_by_class_name('a-pagination').get_attribute("innerHTML"), "html.parser").findAll(class_='a-disabled')[1].text)
time.sleep(4)

for execucoes in range(1,n_paginas):
    lista_resultados = browser.find_element_by_class_name('s-main-slot')
    time.sleep(2)
    html = lista_resultados.get_attribute("innerHTML")
    time.sleep(2)
    soup = BeautifulSoup(html, "html.parser")
    time.sleep(2)
    imgs = []
    links = []
    text = []
    stars = []
    price = []
    economy = []
    fretes = []   
    ## No geral, uso as classes (class_=) ou então os elementos (como 'span', 'div') ou o próprio texto (como em economy abaixo)
    for div in soup.findAll('div'):
        index = (div.get('data-index'))
        if (not(index is None)):
            if(int(index) in range(0,24)):
                imgs.append('' if (div.findAll(class_='s-image')== []) else (div.findAll(class_='s-image'))[0].get('src'))
                links.append('' if (div.findAll(class_='a-link-normal')== []) else 'https://www.amazon.com.br/'+(div.findAll(class_='a-link-normal'))[0].get('href'))
                text.append('' if (div.findAll(class_='a-text-normal') == []) else (div.findAll(class_='a-text-normal')[0].find('span').text))
                stars.append('' if (div.findAll(class_='a-icon-alt') == []) else div.findAll(class_='a-icon-alt')[0].text)
                price.append('' if (div.findAll(class_='a-price') == []) else (div.findAll(class_='a-price')[0].find('span').text))
                economy.append(('' if (div.findAll(class_='a-color-secondary')) == [] else div.findAll(class_='a-color-secondary')[0].text if 'Economize' in div.findAll(class_='a-color-secondary')[0].text else ''))
                frete = ''
                for span in div.findAll('span'):
                    aria_label = span.get('aria-label')
                    if not(aria_label == None):
                        if("frete" in aria_label.lower()):
                            frete = (aria_label)
                fretes.append(frete)
    ## No fim, crio um DF com os itens selecionados, concateno com o já existente
    dados = {'imgs': imgs, 'links':links, 'text':text, 'stars':stars, 'price':price, 'economy':economy, 'frete':fretes}
    dados = pd.DataFrame(data=dados)
    dados_totais = pd.concat([dados,dados_totais], ignore_index=True)
    time.sleep(2)
    browser.find_element_by_class_name('a-last').click()
    time.sleep(10)
    

## E abaixo, envio ao bigquery ou salvo em csv / comentado
dados_totais.to_csv("C:/Robo/Dados/fraldas_amazon_"+data_e_hora_em_texto+".csv", index=False)
dados_totais.to_gbq(destination_table='priceanalysis.fraldas_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)

import os
import time
import requests
import ndjson
import pandas as pd
import json
import io
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
from bs4 import BeautifulSoup
from pandas.io.json import json_normalize
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
key_path = "C:/Robo/FinanceBOT.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
data_e_hora_atuais = datetime.now()
data_e_hora_em_texto = data_e_hora_atuais.strftime('%d%m%Y_%H%M')
os.chdir('C:/Robo/Dados')

dados_totais = {'imgs': [], 'links':[], 'text':[], 'stars':[], 'price':[], 'economy':[]}
dados_totais = pd.DataFrame(data=dados_totais)

browser = webdriver.Chrome()

body = browser.find_element_by_tag_name("body")
body.send_keys(Keys.CONTROL + 't')
browser.get("https://lista.mercadolivre.com.br/higiene-e-cuidados-com-bebe-fraldas-descartaveis/fralda-rn#D[A:fralda%20rn,on]")
time.sleep(2)
itens_atuais = 48
total_itens = int(browser.find_element_by_class_name('ui-search-search-result__quantity-results').text.split(' ')[0].replace('.',''))
try:
    while (itens_atuais <= total_itens):

        lista_resultados = browser.find_element_by_class_name('ui-search-results')
        time.sleep(2)
        html = lista_resultados.get_attribute("innerHTML")
        time.sleep(2)
        soup = BeautifulSoup(html, "html.parser")
        time.sleep(2)
        imgs = []
        links = []
        text = []
        stars = []
        price = []
        economy = []
        fretes = []  
        for div in soup.findAll('div'):
            if (not(div.findAll(class_='ui-search-result__image') == [])):
                links.append(div.find(class_='ui-search-result__image').find(class_='ui-search-link').get('href'))
                imgs.append(div.find(class_='ui-search-result__image').find(class_='ui-search-result-image__element').get('src'))
                text.append(div.find(class_='ui-search-result__image').find(class_='ui-search-result-image__element').get('alt'))
                stars.append('')
                economy.append('')
                fretes.append('')
                cents = div.find(class_="ui-search-price__second-line").find(class_='price-tag-cents')
                if (cents==None):
                    cents="00"
                else:
                    cents = cents.text
                price.append((float(div.find(class_="ui-search-price__second-line").find(class_='price-tag-fraction').text.replace(".","") + "." + cents)))
        dados = {'imgs': imgs, 'links':links, 'text':text, 'stars':stars, 'price':price, 'economy':economy, 'frete':fretes}
        dados = pd.DataFrame(data=dados)
        dados = dados.dropna()
        dados_totais = pd.concat([dados,dados_totais], ignore_index=True)
        time.sleep(10)
        browser.find_element_by_class_name('andes-pagination__button.andes-pagination__button--next').click()
        time.sleep(2)
        itens_atuais = int(browser.current_url.split('Desde_')[1].split('_')[0])
except:
    print('error')
    data_e_hora_atuais = datetime.now()
    data_e_hora_em_texto = data_e_hora_atuais.strftime('%d%m%Y_%H%M')
    dados_totais.to_gbq(destination_table='priceanalysis.fralda_mercadolivre_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)
dados_totais.to_gbq(destination_table='priceanalysis.fralda_mercadolivre_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)
    