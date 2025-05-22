import pandas as pd
import yfinance as yf
import time

input_file = '../csv/Forbes_Global.csv'

try:
    df = pd.read_csv(input_file, encoding='utf-8-sig')
except UnicodeDecodeError:
    df = pd.read_csv(input_file, encoding='latin1')

if 'Name' not in df.columns:
    raise ValueError("can't find 'Name'")

results = []
not_found = []

for name in df['Name']:
    try:
        search_results = yf.Search(name)
        if search_results.quotes and len(search_results.quotes) > 0:
            found = False
            for quote in search_results.quotes:
                ticker = quote.get('symbol')
                shortname = quote.get('shortname')
                exchange = quote.get('exchange')
                if ticker and shortname and exchange:
                    if exchange in ['NYQ', 'NMS', 'NGM', 'NCM', 'NYS', 'NSC', 'NGS', 'NAS']:
                        results.append({'Name': name, 'Ticker': ticker, 'Found_Name': shortname, 'Exchange': exchange})
                        found = True
                        print(f"{name}'s ticker is {ticker}")
                        break
            if not found:
                print(f"{name} can't find U.S. stock exchange code")
                not_found.append({'Name': name})
        else:
            print(f"can't find ticker for {name}")
            not_found.append({'Name': name})
    except Exception as e:
        print(f"error searching {name}: {e}")
        not_found.append({'Name': name})
        continue
    time.sleep(0.5)

output_df = pd.DataFrame(results)
not_found_df = pd.DataFrame(not_found)

output_file = '../csv/Ticker.csv'
not_found_file = '../csv/not_found.csv'

output_df.to_csv(output_file, index=False)
not_found_df.to_csv(not_found_file, index=False)

print(f"完成！結果已儲存到 {output_file} 和 {not_found_file}")
