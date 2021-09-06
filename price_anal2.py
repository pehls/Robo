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

key_path = "G:/Meu Drive/Python Workspace/dados/FinanceBOT.json"

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
try:
    browser.get("https://lista.mercadolivre.com.br/bebes/roupas/kit-body-bebe_CustoFrete_Gratis_OrderId_PRICE*DESC_EnvioEspecial_Normalmente_ITEM*CONDITION_2230284")
    time.sleep(2)
    itens_atuais = 48
    total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))

    while (itens_atuais <= total_itens):

        lista_resultados = browser.find_element_by_id('searchResults')
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
            for viewer in (div.findAll(class_='images-viewer')):
                links.append(viewer.get('item-url'))
                img = (viewer.findAll('img')[0].get('src')) if (viewer.findAll('img')[0].get('data-src') == None) else (viewer.findAll('img')[0].get('data-src'))
                imgs.append(img)
                text.append(viewer.findAll('img')[0].get('alt'))
                stars.append('')
                economy.append('')
                fretes.append('Frete grátis')
                price.append(np.nan if (div.findAll(class_='price__fraction') == []) else (div.findAll(class_='price__fraction')[0].text+",00"))
        dados = {'imgs': imgs, 'links':links, 'text':text, 'stars':stars, 'price':price, 'economy':economy, 'frete':fretes}
        dados = pd.DataFrame(data=dados)
        dados = dados.dropna()
        dados_totais = pd.concat([dados,dados_totais], ignore_index=True)
        time.sleep(2)
        browser.find_element_by_class_name('andes-pagination__button--next').click()
        time.sleep(2)
        itens_atuais = int(browser.current_url.split('Desde_')[1].split('_')[0])
        total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))
except:
    print('error')
    dados_totais.to_gbq(destination_table='priceanalysis.kitbodybebe_mercadolivre_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)
    

try:    
    browser.get("https://lista.mercadolivre.com.br/bebes/roupas/kit-body-bebe_CustoFrete_Gratis_OrderId_PRICE*ASC_EnvioEspecial_Normalmente_ITEM*CONDITION_2230284")
    time.sleep(2)
    itens_atuais = 48
    total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))

    while (itens_atuais <= total_itens):

        lista_resultados = browser.find_element_by_id('searchResults')
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
            for viewer in (div.findAll(class_='images-viewer')):
                links.append(viewer.get('item-url'))
                img = (viewer.findAll('img')[0].get('src')) if (viewer.findAll('img')[0].get('data-src') == None) else (viewer.findAll('img')[0].get('data-src'))
                imgs.append(img)
                text.append(viewer.findAll('img')[0].get('alt'))
                stars.append('')
                economy.append('')
                fretes.append('Frete grátis')
                price.append(np.nan if (div.findAll(class_='price__fraction') == []) else (div.findAll(class_='price__fraction')[0].text+",00"))
        dados = {'imgs': imgs, 'links':links, 'text':text, 'stars':stars, 'price':price, 'economy':economy, 'frete':fretes}
        dados = pd.DataFrame(data=dados)
        dados = dados.dropna()
        dados_totais = pd.concat([dados,dados_totais], ignore_index=True)
        time.sleep(2)
        browser.find_element_by_class_name('andes-pagination__button--next').click()
        time.sleep(2)
        itens_atuais = int(browser.current_url.split('Desde_')[1].split('_')[0])
        total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))
except:
    print('error')
    dados_totais.to_gbq(destination_table='priceanalysis.kitbodybebe_mercadolivre_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)

key_path = "G:/Meu Drive/Python Workspace/dados/FinanceBOT.json"

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

try:

    produto = "babador"
    browser.get("https://lista.mercadolivre.com.br/bebes/"+produto+"_CustoFrete_Gratis_OrderId_PRICE*ASC_EnvioEspecial_Normalmente_ITEM*CONDITION_2230284")
    time.sleep(2)
    itens_atuais = 48
    total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))

    while (itens_atuais <= total_itens):

        lista_resultados = browser.find_element_by_id('searchResults')
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
            for viewer in (div.findAll(class_='images-viewer')):
                links.append(viewer.get('item-url'))
                img = (viewer.findAll('img')[0].get('src')) if (viewer.findAll('img')[0].get('data-src') == None) else (viewer.findAll('img')[0].get('data-src'))
                imgs.append(img)
                text.append(viewer.findAll('img')[0].get('alt'))
                stars.append('')
                economy.append('')
                fretes.append('Frete grátis')
                price.append(np.nan if (div.findAll(class_='price__fraction') == []) else (div.findAll(class_='price__fraction')[0].text+",00"))
        dados = {'imgs': imgs, 'links':links, 'text':text, 'stars':stars, 'price':price, 'economy':economy, 'frete':fretes}
        dados = pd.DataFrame(data=dados)
        dados = dados.dropna()
        dados_totais = pd.concat([dados,dados_totais], ignore_index=True)
        time.sleep(2)
        browser.find_element_by_class_name('andes-pagination__button--next').click()
        time.sleep(2)
        itens_atuais = int(browser.current_url.split('Desde_')[1].split('_')[0])
        total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))
except:
    print('error')
    dados_totais.to_gbq(destination_table='priceanalysis.'+produto+'_mercadolivre_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)

try:
    body = browser.find_element_by_tag_name("body")
    body.send_keys(Keys.CONTROL + 't')
    produto = "babador"
    browser.get("https://lista.mercadolivre.com.br/bebes/"+produto+"_CustoFrete_Gratis_OrderId_PRICE*DESC_EnvioEspecial_Normalmente_ITEM*CONDITION_2230284")
    time.sleep(2)
    itens_atuais = 48
    total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))

    while (itens_atuais <= total_itens):

        lista_resultados = browser.find_element_by_id('searchResults')
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
            for viewer in (div.findAll(class_='images-viewer')):
                links.append(viewer.get('item-url'))
                img = (viewer.findAll('img')[0].get('src')) if (viewer.findAll('img')[0].get('data-src') == None) else (viewer.findAll('img')[0].get('data-src'))
                imgs.append(img)
                text.append(viewer.findAll('img')[0].get('alt'))
                stars.append('')
                economy.append('')
                fretes.append('Frete grátis')
                price.append(np.nan if (div.findAll(class_='price__fraction') == []) else (div.findAll(class_='price__fraction')[0].text+",00"))
        dados = {'imgs': imgs, 'links':links, 'text':text, 'stars':stars, 'price':price, 'economy':economy, 'frete':fretes}
        dados = pd.DataFrame(data=dados)
        dados = dados.dropna()
        dados_totais = pd.concat([dados,dados_totais], ignore_index=True)
        time.sleep(2)
        browser.find_element_by_class_name('andes-pagination__button--next').click()
        time.sleep(2)
        itens_atuais = int(browser.current_url.split('Desde_')[1].split('_')[0])
        total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))
except:
    print('error')
    dados_totais.to_gbq(destination_table='priceanalysis.'+produto+'_mercadolivre_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)
    
key_path = "G:/Meu Drive/Python Workspace/dados/FinanceBOT.json"

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

try:
    produto = "microfibra"
    browser.get("https://lista.mercadolivre.com.br/bebes/"+produto+"_CustoFrete_Gratis_OrderId_PRICE*ASC_EnvioEspecial_Normalmente_ITEM*CONDITION_2230284")
    time.sleep(2)
    itens_atuais = 48
    total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))

    while (itens_atuais <= total_itens):

        lista_resultados = browser.find_element_by_id('searchResults')
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
            for viewer in (div.findAll(class_='images-viewer')):
                links.append(viewer.get('item-url'))
                img = (viewer.findAll('img')[0].get('src')) if (viewer.findAll('img')[0].get('data-src') == None) else (viewer.findAll('img')[0].get('data-src'))
                imgs.append(img)
                text.append(viewer.findAll('img')[0].get('alt'))
                stars.append('')
                economy.append('')
                fretes.append('Frete grátis')
                price.append(np.nan if (div.findAll(class_='price__fraction') == []) else (div.findAll(class_='price__fraction')[0].text+",00"))
        dados = {'imgs': imgs, 'links':links, 'text':text, 'stars':stars, 'price':price, 'economy':economy, 'frete':fretes}
        dados = pd.DataFrame(data=dados)
        dados = dados.dropna()
        dados_totais = pd.concat([dados,dados_totais], ignore_index=True)
        time.sleep(2)
        browser.find_element_by_class_name('andes-pagination__button--next').click()
        time.sleep(2)
        itens_atuais = int(browser.current_url.split('Desde_')[1].split('_')[0])
        total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))
except:
    print('error')
    dados_totais.to_gbq(destination_table='priceanalysis.'+produto+'_mercadolivre_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)

try:
    body = browser.find_element_by_tag_name("body")
    body.send_keys(Keys.CONTROL + 't')
    produto = "microfibra"
    browser.get("https://lista.mercadolivre.com.br/bebes/"+produto+"_CustoFrete_Gratis_OrderId_PRICE*DESC_EnvioEspecial_Normalmente_ITEM*CONDITION_2230284")
    time.sleep(2)
    itens_atuais = 48
    total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))

    while (itens_atuais <= total_itens):

        lista_resultados = browser.find_element_by_id('searchResults')
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
            for viewer in (div.findAll(class_='images-viewer')):
                links.append(viewer.get('item-url'))
                img = (viewer.findAll('img')[0].get('src')) if (viewer.findAll('img')[0].get('data-src') == None) else (viewer.findAll('img')[0].get('data-src'))
                imgs.append(img)
                text.append(viewer.findAll('img')[0].get('alt'))
                stars.append('')
                economy.append('')
                fretes.append('Frete grátis')
                price.append(np.nan if (div.findAll(class_='price__fraction') == []) else (div.findAll(class_='price__fraction')[0].text+",00"))
        dados = {'imgs': imgs, 'links':links, 'text':text, 'stars':stars, 'price':price, 'economy':economy, 'frete':fretes}
        dados = pd.DataFrame(data=dados)
        dados = dados.dropna()
        dados_totais = pd.concat([dados,dados_totais], ignore_index=True)
        time.sleep(2)
        browser.find_element_by_class_name('andes-pagination__button--next').click()
        time.sleep(2)
        itens_atuais = int(browser.current_url.split('Desde_')[1].split('_')[0])
        total_itens = int(browser.find_element_by_class_name('quantity-results').text.split(' ')[0].replace('.',''))
except:
    print('error')
    dados_totais.to_gbq(destination_table='priceanalysis.'+produto+'_mercadolivre_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)
browser.close()