import requests
import lxml.html as html
import re
import os
import json
import pandas as pd
import yfinance as yf


def get_main_urls(ticker):

    with open('./json-files/ticker-data.json') as json_file:
        ticker_data = json.load(json_file)
    with open('./json-files/exchange-codes.json') as json_file:
        exchange_code = json.load(json_file)

    # ticker_data = {'NVDA': ['NVIDIA Corporation', 'NASDAQ'],
    #                'DOCN': ['DigitalOcean Holdings Inc', 'NYSE'],
    #                'NFLX': ['Netflix Inc', 'NASDAQ'],
    #                'AMZN': ['Amazon.com, Inc.', 'NASDAQ'],
    #                'TSLA': ['Tesla Inc', 'NASDAQ']}
    # exchange_code = {'NASDAQ': 'xnas',
    #                  'NYSE': 'xnys'}

    HOME_URL = 'https://www.morningstar.com'
    stock_url = HOME_URL + '/stocks/' + \
        exchange_code[ticker_data[ticker][1]] + '/' + ticker.lower()
    quote_url = stock_url + '/quote'  # Where key ratios are
    # valuation_url = stock_url + '/valuation'
    return quote_url


def get_current_stock_price(ticker):
    stock_info = yf.Ticker(ticker).info
    # stock_info.keys() for other properties you can explore
    market_price = stock_info['regularMarketPrice']
    previous_close_price = stock_info['regularMarketPreviousClose']
    print(ticker + ' stock market price ', market_price)
    print(ticker + ' stock previous close price ', previous_close_price)
    return market_price


def get_full_key_ratio_data_url(quote_url):
    # XPATH expression after pushing on key ratio button (chrome): $x('//div[@class="sal-full-key-ratios"]/a/@href').map(x => x.value)
    # XPATH expression to id value to be pasted in next url: $x('/html/body/script[1]/text()').map(x => x.wholeText)
    response = requests.get(quote_url)
    if response.status_code == 200:
        home = response.content.decode('utf-8')
        parsed = html.fromstring(home)
        text_to_look_id = parsed.xpath('/html/body/script[1]/text()')[0]
    #     print(text_to_look_id)
        id_to_full_data = re.findall(
            '(?<=byId:{").*', text_to_look_id)[0].split('":')[0]
        # print(id_to_full_data)
        full_key_ratio_url = 'https://financials.morningstar.com/ratios/r.html?t=' + \
            id_to_full_data + '&culture=en&platform=sal'
    return full_key_ratio_url


def get_valuation_tab_url(full_key_ratio_url):
    # XPATH expression $x('//*[contains(text(),"Valuation")]/@href').map(x => x.value)
    response = requests.get(full_key_ratio_url)
    if response.status_code == 200:
        home = response.content.decode('utf-8')
        parsed = html.fromstring(home)
        valuation_tab_url = parsed.xpath(
            '//*[contains(text(),"Valuation")]/@href')[0]
        valuation_tab_url = 'https://' + valuation_tab_url.split('//')[-1]
    return valuation_tab_url


def get_valuation_ratio_urls(valuation_tab_url):
    # XPATH expression $x('//script[@language="javascript"]/text()').map(x => x.wholeText)
    response = requests.get(valuation_tab_url)
    home = response.content.decode('utf-8')
    parsed = html.fromstring(home)
    text_to_look_url = parsed.xpath(
        '//script[@language="javascript"]/text()')[0]
    # Current valuation url
    current_valuation_url = 'https:' + \
        re.findall('(?<=urlstr = ").*',
                   text_to_look_url)[0].split('"+sAds;')[0] + '&adsFlag=true'
    # Forward valuation url
    forward_valuation_url = 'https:' + \
        re.findall('(?<=urlstr = ").*',
                   text_to_look_url)[1].split('"+sAds;')[0] + '&adsFlag=true'
    # History valuation url
    history_valuation_url = 'https://financials.morningstar.com/valuate/valuation-history' + \
        re.findall('(?<="//financials.morningstar.com/valuate/valuation-history).*',
                   text_to_look_url)[0].split('"+historyType,')[0] + 'price-earnings'
    return current_valuation_url, forward_valuation_url, history_valuation_url


def download_key_ratios_csv(full_key_ratio_url):
    response = requests.get(full_key_ratio_url)
    home = response.content.decode('utf-8')
    parsed = html.fromstring(home)
    text_to_look_url = parsed.xpath(
        '/html/body/div[1]/div[3]/script/text()')[0]
    url_to_csv = 'https:' + \
        re.findall('(?<=urlstr = ").*',
                   text_to_look_url)[2].split('"+orderby;')[0] + 'asc'
    headers = {
        'Referer': full_key_ratio_url}

    r = requests.get(url_to_csv, headers=headers)
    print(r.status_code)
    csv = r.content

    # Saving data into a local folder or database
    path = './datasets/' + ticker + '/'
    if not os.path.exists(path):
        os.makedirs(path)
    filename = ticker + '-key-ratios.csv'
    with open(path+filename, "wb") as file:
        file.write(csv)


def download_financial_reports_csv(ticker, full_key_ratio_url, report_type):
    with open('./json-files/ticker-data.json') as json_file:
        ticker_data = json.load(json_file)
    with open('./json-files/exchange-codes.json') as json_file:
        exchange_code = json.load(json_file)

    # 'Report_Type : is = Income Statement, cf = Cash Flow, bs = Balance Sheet
    # 'Period: 12 for annual reporting, 3 for quarterly reporting
    # 'Data_Type : A means as reported, R as restated
    # 'Order: asc or desc (ascending or descending)
    # 'Years: 5 or 10 are the only two values supported
    # 'Unit : 1 = None 2 = Thousands 3 = Millions 4 = Billions
    stock_exchange = exchange_code[ticker_data[ticker][1]].upper()  # "XMIL"
    stock_name = ticker.upper()  # "PIRC"

    ms_report_type = report_type  # "bs"
    ms_period = '3'
    ms_data_type = "A"
    ms_years = '5'
    ms_unit = '3'
    # url = 'http://financials.morningstar.com/ajax/ReportProcess4CSV.html?&t=' + stock_exchange +':' + stock_name + '&region=ita&culture=en-US&cur=&reportType=' + ms_report_type + '&period=' + ms_period + '&dataType=A&order=asc&columnYear=5&curYearPart=1st5year&rounding=3&view=raw&r=305280&denominatorView=raw&number=' + ms_unit

    url_to_csv = "https://financials.morningstar.com/ajax/ReportProcess4CSV.html?" + "t=" + stock_exchange + ":" + stock_name + "&reportType=" + \
        ms_report_type + "&period=" + ms_period + "&dataType=" + ms_data_type + \
        "&order=asc" + "&columnYear=" + ms_years + "&number=" + ms_unit

    headers = {
        'Referer': full_key_ratio_url}

    r = requests.get(url_to_csv, headers=headers)
    print(r.status_code)
    csv = r.content

    # Saving data into a local folder or database
    path = './datasets/' + ticker + '/'
    if not os.path.exists(path):
        os.makedirs(path)
    if ms_report_type == "bs":
        name = 'balance-sheet'
    elif ms_report_type == 'is':
        name = 'income-statement'
    else:
        name = 'cash-flow'
    filename = ticker + '-' + name + '.csv'
    with open(path+filename, "wb") as file:
        file.write(csv)


def save_html_table(table_name, url, ticker):
    r = requests.get(url, headers={})
    print(r.status_code)
    html_content = r.content

    # Saving data into a local folder or database
    path = './datasets/' + ticker + '/'
    if not os.path.exists(path):
        os.makedirs(path)
    filename = ticker + '-' + table_name + '.html'
    with open(path+filename, "wb") as file:
        file.write(html_content)


def convert_current_valuation_from_html_to_csv(ticker):
    path = './datasets/' + ticker + '/'
    filename = ticker + '-current-valuation.html'
    filepath = path + filename
    df = pd.read_html(filepath, index_col=0)[0]
    idxs = ['Price/Earnings', 'Price/Book', 'Price/Sales',
            'Price/Cash Flow', 'Dividend Yield %']
    df = df.loc[idxs].dropna(axis=1)
    # Saving data into a local folder or database
    filename_csv = filename.replace('html', 'csv')
    df.to_csv(path+filename_csv)


def convert_history_valuation_from_html_to_csv(ticker):
    path = './datasets/' + ticker + '/'
    filename = ticker + '-history-valuation.html'
    filepath = path + filename
    df = pd.read_html(filepath, index_col=0)[0]
    # Setting needed indexes
    df = df.iloc[1::3]
    df.set_axis(['Price/Earnings', 'Price/Book', 'Price/Sales',
                 'Price/Cash Flow'], axis=0, inplace=True)
    # Setting columns
    html_file = open(filepath, "r").read()
    items = re.findall(r'"(.*?)"', html_file)[2:24:2]
    df.set_axis(items, axis=1, inplace=True)
    # Saving data into a local folder or database
    filename_csv = filename.replace('html', 'csv')
    df.to_csv(path+filename_csv)


def get_financials_tab_url(full_key_ratio_url):
    # XPATH expression $x('//*[contains(text(),"Financials")]/@href').map(x => x.value)
    response = requests.get(full_key_ratio_url)
    if response.status_code == 200:
        home = response.content.decode('utf-8')
        parsed = html.fromstring(home)
        financials_tab_url = parsed.xpath(
            '//*[contains(text(),"Financials")]/@href')[0]
        financials_tab_url = 'https://' + financials_tab_url.split('//')[-1]
    return financials_tab_url


def get_financials_urls(financials_tab_url):
    # XPATH expression $x('//ul[@class="r_snav"]/li/a/@href').map(x => x.value)
    response = requests.get(financials_tab_url)
    home = response.content.decode('utf-8')
    parsed = html.fromstring(home)
    financials_urls = parsed.xpath('//ul[@class="r_snav"]/li/a/@href')
    income_statement_url = financials_urls[0].replace('http', 'https')
    balance_sheet_url = financials_urls[1].replace('http', 'https')
    cash_flow_url = financials_urls[2].replace('http', 'https')
    return income_statement_url, balance_sheet_url, cash_flow_url


def save_current_key_ratios_to_csv(ticker):
    path = './datasets/' + ticker + '/'
    filename = ticker + '-key-ratios.csv'
    filepath = path + filename

    # Liquidity, efficiency and  profitability ratios
    # --------------------------------------------------
    df = pd.read_csv(filepath, sep=',', skiprows=[
                     0, 1], index_col=0, on_bad_lines='skip')

    # Liquidity: Cash ratio
    # ---------------------
    filename = ticker + '-balance-sheet.csv'
    filepath = path + filename
    df2 = pd.read_csv(filepath, sep=',', skiprows=[
                      0, 2, 3, 4], index_col=0, on_bad_lines='skip')

    # Liquidity Ratios
    # ----------------
    # Current Ratio
    current_ratio = df.loc['Current Ratio', 'TTM']
    # Quick Ratio
    quick_ratio = df.loc['Quick Ratio', 'TTM']

    # Cash Ratio
    total_cash = df2.loc['Total cash'][-1]
    total_current_liabilities = df2.loc['Total current liabilities'][-1]
    cash_ratio = round(total_cash / total_current_liabilities, 2)

    # Solvency Ratios
    debt_equity = df.loc['Debt/Equity', 'TTM']

    # Efficiency Ratios
    # -----------------
    # Inventory Turnover  (TTM)
    inventory_turnover = df.loc['Inventory Turnover', 'TTM']
    # Days Inventory (TTM)
    days_inventory = df.loc['Days Inventory', 'TTM']
    # Assets Turnover (TTM)
    assets_turnover = df.loc['Asset Turnover', 'TTM']

    # Profitability Ratios
    # --------------------
    # ROE (Return on Equity)
    roe_ratio = df.loc['Return on Equity %', 'TTM']
    # Net Margin
    net_margin_ratio = df.loc['Net Margin %', 'TTM']

    # Valuation Ratios
    # ----------------
    # path = './datasets/' + ticker + '/'
    filename = ticker + '-current-valuation.csv'
    filepath = path + filename
    df1 = pd.read_csv(filepath, index_col=0)

    # Valuation Ratios
    # ----------------
    # PER (Price Earning Ratio)
    per_ratio = df1.loc['Price/Earnings'][0]
    # PCF (Price to Cash Flow)
    pcf_ratio = df1.loc['Price/Cash Flow'][0]
    # PS (Price to Sales Ratio)
    ps_ratio = df1.loc['Price/Sales'][0]
    # PBV (Price to Book Value)
    pbv_ratio = df1.loc['Price/Book'][0]
    ratios_dict = {'Current Ratio': current_ratio, 'Quick Ratio': quick_ratio, 'Cash Ratio': cash_ratio, 'Debt/Equity': debt_equity, 'Inventory Turnover': inventory_turnover, 'Days Inventory': days_inventory,
                   'Asset Turnover': assets_turnover, 'Return on Equity %': roe_ratio, 'Net Margin %': net_margin_ratio, 'Price/Earnings': per_ratio, 'Price/Cash Flow': pcf_ratio, 'Price/Sales': ps_ratio, 'Price/Book': pbv_ratio}
    # print(ratios_dict.items())
    # print(type(ratios_dict))
    ratios_dict_df = pd.DataFrame.from_dict(
        data=ratios_dict, orient='index', columns=[ticker])
    ratios_dict_df.to_csv(path + ticker + '-current-key-ratios.csv')


def save_current_past_valuation_ratios_to_csv(ticker):
    # Valuation Ratios
    # ----------------
    path = './datasets/' + ticker + '/'
    filename = ticker + '-current-valuation.csv'
    filepath = path + filename
    df1 = pd.read_csv(filepath, index_col=0)
    # PER (Price Earning Ratio)
    per_ratio = df1.loc['Price/Earnings'][0]
    per_ratio_5year = df1.loc['Price/Earnings'][-1]
    # PCF (Price to Cash Flow)
    pcf_ratio = df1.loc['Price/Cash Flow'][0]
    pcf_ratio_5year = df1.loc['Price/Cash Flow'][-1]
    # PS (Price to Sales Ratio)
    ps_ratio = df1.loc['Price/Sales'][0]
    ps_ratio_5year = df1.loc['Price/Sales'][-1]
    # PBV (Price to Book Value)
    pbv_ratio = df1.loc['Price/Book'][0]
    pbv_ratio_5year = df1.loc['Price/Book'][-1]
    ratios_dict = {'Price/Earnings': [per_ratio, per_ratio_5year], 'Price/Cash Flow': [pcf_ratio, pcf_ratio_5year],
                   'Price/Sales': [ps_ratio, ps_ratio_5year], 'Price/Book': [pbv_ratio, pbv_ratio_5year]}
    # print(ratios_dict.items())
    # print(type(ratios_dict))
    ratios_dict_df = pd.DataFrame.from_dict(
        data=ratios_dict, orient='index', columns=[ticker, '5 years'])
    ratios_dict_df.to_csv(path + ticker + '-current-past-valuation-ratios.csv')


def main(ticker):
    # ticker = 'AMD'
    quote_url = get_main_urls(ticker)
    # current_stock_price = get_current_stock_price(ticker)
    # --------------------
    # Get key-ratios-csv
    # --------------------
    full_key_ratio_url = get_full_key_ratio_data_url(quote_url)
    download_key_ratios_csv(full_key_ratio_url)

    download_financial_reports_csv(ticker, full_key_ratio_url, 'bs')
    download_financial_reports_csv(ticker, full_key_ratio_url, 'is')
    download_financial_reports_csv(ticker, full_key_ratio_url, 'cf')

    valuation_tab_url = get_valuation_tab_url(full_key_ratio_url)
    current_valuation_url, forward_valuation_url, history_valuation_url = get_valuation_ratio_urls(
        valuation_tab_url)
    print(current_valuation_url, forward_valuation_url, history_valuation_url)
    save_html_table('current-valuation', current_valuation_url, ticker)
    save_html_table('forward-valuation', forward_valuation_url, ticker)
    save_html_table('history-valuation', history_valuation_url, ticker)
    convert_current_valuation_from_html_to_csv(ticker)
    convert_history_valuation_from_html_to_csv(ticker)
    save_current_key_ratios_to_csv(ticker)
    save_current_past_valuation_ratios_to_csv(ticker)
    # --------------------
    # Get financials-csv
    # --------------------
    financials_tab_url = get_financials_tab_url(full_key_ratio_url)
    income_statement_url, balance_sheet_url, cash_flow_url = get_financials_urls(
        financials_tab_url)


if __name__ == '__main__':
    # ticker = 'AMD'
    tickers = ['NVDA', 'DOCN', 'NFLX', 'TSLA', 'INTC', 'AMD', 'TWTR', 'FB']
    for ticker in tickers:
        main(ticker)
