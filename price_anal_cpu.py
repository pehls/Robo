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

browser.get("https://www.amazon.com.br/ref=nav_logo")
time.sleep(2)

produto = "cpu"

search_box = browser.find_element_by_id("twotabsearchtextbox")
search_box.send_keys(produto)
browser.find_element_by_xpath('//*[@id="nav-search"]/form/div[2]/div/input').click()
time.sleep(2)

browser.find_element_by_class_name('a-button-dropdown').click()
time.sleep(2)

browser.find_element_by_id('s-result-sort-select_1').click()
time.sleep(2)

n_paginas = int(BeautifulSoup(browser.find_element_by_class_name('a-pagination').get_attribute("innerHTML"), "html.parser").findAll(class_='a-disabled')[1].text)
time.sleep(2)

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

page = 0
while(True):
    n_paginas = int(BeautifulSoup(browser.find_element_by_class_name('a-pagination').get_attribute("innerHTML"), "html.parser").findAll(class_='a-disabled')[1].text)
    time.sleep(2)
    try:
        for execucoes in range(1,n_paginas):
            for div in soup.findAll('div'):
                index = (div.get('data-index'))
                if (page == 0):
                    if (not(index is None)):
                        if(int(index) in range(2,25)):
                            imgs.append((div.findAll(class_='s-image'))[0].get('src'))
                            links.append('https://www.amazon.com.br/'+(div.findAll(class_='a-link-normal'))[0].get('href'))
                            text.append((div.findAll(class_='a-text-normal')[0].find('span').text))
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
                            page = page + 1
                            time.sleep(2)
                            browser.find_element_by_class_name('a-last').click()
                            dados_totais = {'imgs': imgs, 
                                            'links':links, 
                                            'text':text, 
                                            'stars':stars, 
                                            'price':price, 
                                            'economy':economy, 
                                            'frete':fretes}
                            dados_totais = pd.DataFrame(data=dados_totais)
                            dados_totais['Data'] = data_e_hora_atuais.strftime('%d/%m/%Y')
                            dados_totais['Hora'] = data_e_hora_atuais.strftime('%H:%M:%S')
                            dados_totais['local'] = 'Amazon'
                            #dados_totais.to_csv(produto+data_e_hora_em_texto+".csv", index=False)
                            dados_totais.to_gbq(destination_table='PCsprice.'+produto+'_main_amazon_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)
                else:
                     if (not(index is None)):
                        if(int(index) in range(1,24)):
                            imgs.append((div.findAll(class_='s-image'))[0].get('src'))
                            links.append('https://www.amazon.com.br/'+(div.findAll(class_='a-link-normal'))[0].get('href'))
                            text.append((div.findAll(class_='a-text-normal')[0].find('span').text))
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
                            time.sleep(2)
                            browser.find_element_by_class_name('a-last').click()
                            dados_totais = {'imgs': imgs, 
                                            'links':links, 
                                            'text':text, 
                                            'stars':stars, 
                                            'price':price, 
                                            'economy':economy, 
                                            'frete':fretes}
                            dados_totais = pd.DataFrame(data=dados_totais)
                            dados_totais['Data'] = data_e_hora_atuais.strftime('%d/%m/%Y')
                            dados_totais['Hora'] = data_e_hora_atuais.strftime('%H:%M:%S')
                            dados_totais['local'] = 'Amazon'
                            #dados_totais.to_csv(produto+data_e_hora_em_texto+".csv", index=False)
                            dados_totais.to_gbq(destination_table='PCsprice.'+produto+'_main_amazon_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)
                            page = page + 1
    except:
        print('erro')
        browser.get("https://www.amazon.com.br/ref=nav_logo")
        time.sleep(2)

        produto = "cpu"

        search_box = browser.find_element_by_id("twotabsearchtextbox")
        search_box.send_keys(produto)
        browser.find_element_by_xpath('//*[@id="nav-search"]/form/div[2]/div/input').click()
        time.sleep(2)

        browser.find_element_by_class_name('a-button-dropdown').click()
        time.sleep(2)

        browser.find_element_by_id('s-result-sort-select_2').click()
        time.sleep(2)

        n_paginas = int(BeautifulSoup(browser.find_element_by_class_name('a-pagination').get_attribute("innerHTML"), "html.parser").findAll(class_='a-disabled')[1].text)
        time.sleep(2)

        lista_resultados = browser.find_element_by_class_name('s-main-slot')
        time.sleep(2)
        html = lista_resultados.get_attribute("innerHTML")
        time.sleep(2)
        soup = BeautifulSoup(html, "html.parser")
        time.sleep(2)

        page = 0

        n_paginas = int(BeautifulSoup(browser.find_element_by_class_name('a-pagination').get_attribute("innerHTML"), "html.parser").findAll(class_='a-disabled')[1].text)
        time.sleep(2)

        for execucoes in range(1,n_paginas):
            for div in soup.findAll('div'):
                index = (div.get('data-index'))
                if (page == 0):
                    if (not(index is None)):
                        if(int(index) in range(2,25)):
                            imgs.append((div.findAll(class_='s-image'))[0].get('src'))
                            links.append('https://www.amazon.com.br/'+(div.findAll(class_='a-link-normal'))[0].get('href'))
                            text.append((div.findAll(class_='a-text-normal')[0].find('span').text))
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
                            page = page + 1
                            time.sleep(2)
                            browser.find_element_by_class_name('a-last').click()
                else:
                     if (not(index is None)):
                        if(int(index) in range(1,24)):
                            imgs.append((div.findAll(class_='s-image'))[0].get('src'))
                            links.append('https://www.amazon.com.br/'+(div.findAll(class_='a-link-normal'))[0].get('href'))
                            text.append((div.findAll(class_='a-text-normal')[0].find('span').text))
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
                            time.sleep(2)
                            browser.find_element_by_class_name('a-last').click()
                             page = page + 1
    finally:
        print('finally')
        break
dados_totais = {'imgs': imgs, 
                'links':links, 
                'text':text, 
                'stars':stars, 
                'price':price, 
                'economy':economy, 
                'frete':fretes}
dados_totais = pd.DataFrame(data=dados_totais)
dados_totais['Data'] = data_e_hora_atuais.strftime('%d/%m/%Y')
dados_totais['Hora'] = data_e_hora_atuais.strftime('%H:%M:%S')
dados_totais['local'] = 'Amazon'
#dados_totais.to_csv(produto+data_e_hora_em_texto+".csv", index=False)
dados_totais.to_gbq(destination_table='PCsprice.'+produto+'_main_amazon_'+data_e_hora_em_texto, if_exists='append', credentials=credentials)

Nome_da_marca = []
Série = []
Cor = []
Formato = []
Altura_do_produto = []
Largura_do_produto = []
Marca_do_processador = []
Tipo_de_processador = []
Velocidade_do_processador = []
Tamanho_da_memória = []
Tecnologia_da_memória = []
Tipo_de_Memória = []
Tamanho_do_HD = []
Tecnologia_do_HD = []
Interface_do_HD = []
Modelo_de_placa_de_vídeo = []
Marca_do_chipset_de_vídeo = []
Memória_de_vídeo = []
Tipo_de_conexão = []
Tecnologia_de_conexão = []
Plataforma_de_hardware = []
Sistema_operacional = []
Links_2 = []
for link in links:
    # Abre uma nova aba e vai para o site do SO
    browser.get(link)
    time.sleep(3)
    try:
        lista_resultados = browser.find_element_by_class_name('section, techD')
        time.sleep(2)

        html = lista_resultados.get_attribute("innerHTML")
        time.sleep(2)

        soup = BeautifulSoup(html, "html.parser")
        time.sleep(2)



        for line in (soup.findAll('tr')):
            Links_2.append(link)
            Nome_da_marca.append(line.findAll('td')[1].text if 'Nome da marca' in (line.findAll('td')[0].text) else '')
            Série.append(line.findAll('td')[1].text if 'Série' in (line.findAll('td')[0].text) else '')
            Cor.append(line.findAll('td')[1].text if 'Cor' in (line.findAll('td')[0].text) else '')
            Formato.append(line.findAll('td')[1].text if 'Formato' in (line.findAll('td')[0].text) else '')
            Altura_do_produto.append(line.findAll('td')[1].text if 'Altura do produto' in (line.findAll('td')[0].text) else '')
            Largura_do_produto.append(line.findAll('td')[1].text if 'Largura do produto' in (line.findAll('td')[0].text) else '')
            Marca_do_processador.append(line.findAll('td')[1].text if 'Marca do processador' in (line.findAll('td')[0].text) else '')
            Tipo_de_processador.append(line.findAll('td')[1].text if 'Tipo de processador' in (line.findAll('td')[0].text) else '')
            Velocidade_do_processador.append(line.findAll('td')[1].text if 'Velocidade do processador' in (line.findAll('td')[0].text) else '')
            Tamanho_da_memória.append(line.findAll('td')[1].text if 'Tamanho da memória' in (line.findAll('td')[0].text) else '')
            Tecnologia_da_memória.append(line.findAll('td')[1].text if 'Tecnologia da memória' in (line.findAll('td')[0].text) else '')
            Tipo_de_Memória.append(line.findAll('td')[1].text if 'Tipo de Memória' in (line.findAll('td')[0].text) else '')
            Tamanho_do_HD.append(line.findAll('td')[1].text if 'Tamanho do HD' in (line.findAll('td')[0].text) else '')
            Tecnologia_do_HD.append(line.findAll('td')[1].text if 'Tecnologia do HD' in (line.findAll('td')[0].text) else '')
            Interface_do_HD.append(line.findAll('td')[1].text if 'Interface do HD' in (line.findAll('td')[0].text) else '')
            Modelo_de_placa_de_vídeo.append(line.findAll('td')[1].text if 'Modelo de placa de vídeo' in (line.findAll('td')[0].text) else '')
            Marca_do_chipset_de_vídeo.append(line.findAll('td')[1].text if 'Marca do chipset de vídeo' in (line.findAll('td')[0].text) else '')
            Memória_de_vídeo.append(line.findAll('td')[1].text if 'Memória de vídeo' in (line.findAll('td')[0].text) else '')
            Tipo_de_conexão.append(line.findAll('td')[1].text if 'Tipo de conexão' in (line.findAll('td')[0].text) else '')
            Tecnologia_de_conexão.append(line.findAll('td')[1].text if 'Tecnologia de conexão' in (line.findAll('td')[0].text) else '')
            Plataforma_de_hardware.append(line.findAll('td')[1].text if 'Plataforma de hardware' in (line.findAll('td')[0].text) else '')
            Sistema_operacional.append(line.findAll('td')[1].text if 'Sistema operacional' in (line.findAll('td')[0].text) else '')
    except:
        Links_2.append(link)
        Nome_da_marca.append( '')
        Série.append( '')
        Cor.append('')
        Formato.append( '')
        Altura_do_produto.append( '')
        Largura_do_produto.append( '')
        Marca_do_processador.append( '')
        Tipo_de_processador.append('')
        Velocidade_do_processador.append('')
        Tamanho_da_memória.append('')
        Tecnologia_da_memória.append( '')
        Tipo_de_Memória.append('')
        Tamanho_do_HD.append( '')
        Tecnologia_do_HD.append('')
        Interface_do_HD.append( '')
        Modelo_de_placa_de_vídeo.append('')
        Marca_do_chipset_de_vídeo.append('')
        Memória_de_vídeo.append( '')
        Tipo_de_conexão.append( '')
        Tecnologia_de_conexão.append( '')
        Plataforma_de_hardware.append( '')
        Sistema_operacional.append( '')
        pass
dados_totais = {'Nome_da_marca':Nome_da_marca,
            'Serie':Série,
            'link':Links_2,
            'Cor':Cor,
            'Formato':Formato,
            'Altura_do_produto':Altura_do_produto,
            'Largura_do_produto':Largura_do_produto,
            'Marca_do_processador':Marca_do_processador,
            'Tipo_de_processador':Tipo_de_processador,
            'Velocidade_do_processador':Velocidade_do_processador,
            'Tamanho_da_memoria':Tamanho_da_memória,
            'Tecnologia_da_memoria':Tecnologia_da_memória,
            'Tipo_de_Memoria':Tipo_de_Memória,
            'Tamanho_do_HD':Tamanho_do_HD,
            'Tecnologia_do_HD':Tecnologia_do_HD,
            'Interface_do_HD':Interface_do_HD,
            'Modelo_de_placa_de_video':Modelo_de_placa_de_vídeo,
            'Marca_do_chipset_de_video':Marca_do_chipset_de_vídeo,
            'Memoria_de_video':Memória_de_vídeo,
            'Tipo_de_conexao':Tipo_de_conexão,
            'Tecnologia_de_conexao':Tecnologia_de_conexão,
            'Plataforma_de_hardware':Plataforma_de_hardware,
            'Sistema_operacional':Sistema_operacional,
           }
dados_totais = pd.DataFrame(data=dados_totais)
dados_totais['Data'] = data_e_hora_atuais.strftime('%d/%m/%Y')
dados_totais['Hora'] = data_e_hora_atuais.strftime('%H:%M:%S')
dados_totais['local'] = 'Amazon'
#dados_totais.to_csv(produto+data_e_hora_em_texto+".csv", index=False)
dados_totais.to_gbq(destination_table='PCsprice.'+produto+'_desc_amazon_'+data_e_hora_em_texto, if_exists='replace', credentials=credentials)
browser.close()
