import time
from yahoo_fin.stock_info import *
import json
import numpy_financial as npf
import pandas as pd
import numpy
import traceback
import csv
import random
import stopit
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import queue
import threading
import socket
import requests

RATE_OF_RETURN = .15
MARGIN_OF_ERROR = .5
POLLING_RETRIES = 4
MAX_THREADS = 20
TIMEOUT = 20
# FIXME make this more mutable
TICKERS_CSV = './tickers.12.8.21.csv'
# TICKERS_CSV = './tickers_small.csv'

print(f'Local IP: {socket.gethostbyname(socket.gethostname())}')
ip = requests.get('https://api.ipify.org').content.decode('utf8')
print('Public IP: {}'.format(ip))

print('Getting tickers')
df = pd.read_csv(TICKERS_CSV).iloc[:,0:2]
ticker_count = len(df)

csvfile = open('outputs/good.csv', 'w')
csvwriter = csv.writer(csvfile)
columns=['ticker', 'current_EPS', 'growth_rate', 'PE', 'future_EPS', 'unadjusted_price', 'present_value', 'calculated_value', 'previous_close', 'good']
csvwriter.writerow(columns)

logging.basicConfig(level=logging.INFO, format='%(asctime)-5s :: %(relativeCreated)-4d :: %(threadName)-10s :: %(levelname)-5s: %(message)s')
filehandler = logging.FileHandler('./outputs/log.csv', mode='w')
log = logging.getLogger('stock-eval')
log.addHandler(filehandler)

log.debug('Tickers:')
log.debug(df)

@stopit.threading_timeoutable(default=('didnt finish', None, None))
def poll_api(ticker):
    info = get_analysts_info(ticker)
    time.sleep(random.random()*2)

    try:
        quote = get_quote_table(ticker)
        time.sleep(random.random()*2)
    except Exception:
        log.error(f'{ticker}: something wrong with get_quote_table, cannot process so skipping')
        log.error(traceback.format_exc())
        raise Exception
        return

    try:    
        stats = get_stats_valuation(ticker)
        time.sleep(random.random()*2)
    except IndexError:
        log.warning(f'{ticker}: cant get stats table, will try to use data from quote table')
        stats = None
    
    return info, stats, quote

def writer(q):
    log.info('writer started')

    csvfile = open('outputs/good.csv', 'w')
    csvwriter = csv.writer(csvfile)
    columns=['ticker', 'current_EPS', 'growth_rate', 'PE', 'future_EPS', 'unadjusted_price', 'present_value', 'calculated_value', 'previous_close', 'good']
    csvwriter.writerow(columns)
    count = 0

    while 1:
        m = q.get()
        if m == 'kill':
            kill = True
            log.info('writer killed')
            break
        csvwriter.writerow(m)
        csvfile.flush()
        count += 1

        if count == ticker_count:
            log.info('writer done')
            kill=True
        
    if kill:
        csvfile.close()
        q.task_done()

                

def get_info(ticker, name, q):
    log.info(f"Getting analysis for ticker {ticker}: {name}")

    # try to get data, retry x amount of times
    for i in range(POLLING_RETRIES):
        try:
            info, stats, quote = poll_api(ticker, timeout=TIMEOUT)
            if info == 'didnt finish':
                log.info(f"{i}: DIDNT FINISH LOADING IN TIME")
                # ran out of retries, failed
                if i == POLLING_RETRIES:
                    return
                # retry
                continue
        except ValueError:
            log.info(f"{ticker}: Error getting analyist info, most likely not available")
            return
        break

    if float(info['Earnings Estimate'].iloc[0,3]) == 0:
        log.info("No analysts avilable, skipping")
        return
    else:
        log.debug(f"Num analysts: {float(info['Earnings Estimate'].iloc[0,3])}")

    data = {}

    # col should be "current year"
    data['current_EPS'] = float(info['Earnings Estimate'].iloc[1,3])

    try:
        data['growth_rate'] = float(info['Growth Estimates'].loc[4, ticker].replace('%', ''))/100
    except AttributeError:
        log.warning("NOTE: can't find 5 year per annum growth rate, will try next year growth rate")   
    try:
        data['growth_rate'] = float(info['Growth Estimates'].loc[3, ticker].replace('%', ''))/100
    except AttributeError:
        log.warning("NOTE: can't find 5 year per annum growth rate, will try next year growth rate")
        data['growth_rate'] = float(info['Growth Estimates'].loc[2, ticker].replace('%', ''))/100

    # TODO: add a check to make sure nothing is null and error if so

    if stats is not None:
        data['PE_trailing'] = float(stats.iloc[2,1])    
    else:
        data['PE_trailing'] = numpy.nan
    
    if numpy.isnan(data['PE_trailing']):
        log.warning("NOTE: can't find PE trailing, will just use the growth percentage * 200")
        data['PE_trailing'] = data['growth_rate']*200

    data['future_EPS'] = data["current_EPS"]* ((1+data["growth_rate"])**10)
    data['unadjusted_price'] = data["future_EPS"]* data["PE_trailing"] 
    data['present_value'] = npf.pv(RATE_OF_RETURN, 10, 0, -1*data['unadjusted_price'])
    data['calc_value'] = data['present_value']*MARGIN_OF_ERROR
    data['previous_close'] = quote['Previous Close']
    
    good = False
    if(data['previous_close'] < data['calc_value']) and data['calc_value'] > 0:
        log.info("THIS IS A GOOD ONE!!!!!")
        good = True
        # csvwriter.writerow([ticker, data['current_EPS'], data['growth_rate'], data['PE_trailing'], data['future_EPS'], data['unadjusted_price'], data['present_value'], data['calc_value'], data['previous_close'], True])
    
    res = [ticker, data['current_EPS'], data['growth_rate'], data['PE_trailing'], data['future_EPS'], data['unadjusted_price'], data['present_value'], data['calc_value'], data['previous_close'], good]
    log.info(res)
    q.put(res)

    log.info("the data I used:")
    log.info(json.dumps(data, indent=4))
    

def lambda_function(event, context):
    # start the writer thread
    t_start = time.time()
    q = queue.Queue()
    q_thread = threading.Thread(target=writer, args=(q,), daemon=True).start()

    executer = ThreadPoolExecutor(max_workers=MAX_THREADS, thread_name_prefix='tkr_')
    log.info(f"TICKER COUNT: {ticker_count}")
        # print(f"{df.iloc[i,0]} {df.iloc[i,1]}")
    futures_to_ticker = {executer.submit(get_info, df.iloc[i,0], df.iloc[i,1], q, ): df.iloc[i,0] for i in range(ticker_count)}

    doneCount = 0
    for future in as_completed(futures_to_ticker):
        log.info(f'future {futures_to_ticker[future]} completed {doneCount + 1}/{ticker_count}')
        doneCount += 1
        if doneCount == ticker_count:
            executer.shutdown(wait=True)
            log.info('executer shutdown complete')
            q.put('kill')

    t_end = time.time()
    log.info(f"okay, i think everything's done :) completed in {t_end-t_start}ms")

if __name__ == '__main__':
    lambda_function(None, None)
