import pandas as pd
import yfinance as yf
import time

sector_map = {
    "Technology": "科技",#1
    "Financial Services": "金融服務",#2
    "Healthcare": "醫療保健",#0
    "Consumer Cyclical": "非必需消費品",#3
    "Communication Services": "通訊服務",#1
    "Energy": "能源",#4
    "Industrials": "工業",
    "Consumer Defensive": "必需消費品",#3
    "Real Estate": "房地產",#5
    "Utilities": "公用事業",#0
    "Basic Materials": "基礎材料"#0
}

industry_map = {
    # 材料類
    "agricultural-inputs": "農業投入",
    "aluminum": "鋁業",
    "building-materials": "建材",
    "chemicals": "化學品",
    "coking-coal": "焦煤",
    "copper": "銅業",
    "gold": "黃金",
    "lumber-wood-production": "木材與木製品",
    "other-industrial-metals-mining": "其他工業金屬與採礦",
    "other-precious-metals-mining": "其他貴金屬與採礦",
    "paper-paper-products": "紙與紙製品",
    "silver": "白銀",
    "specialty-chemicals": "特殊化學品",
    "steel": "鋼鐵",

    # 傳媒與通訊
    "advertising-agencies": "廣告代理",
    "broadcasting": "廣播",
    "electronic-gaming-multimedia": "電子遊戲與多媒體",
    "entertainment": "娛樂",
    "internet-content-information": "網路內容與資訊",
    "publishing": "出版",
    "telecom-services": "電信服務",

    # 非必需消費品
    "apparel-manufacturing": "服裝製造",
    "apparel-retail": "服裝零售",
    "auto-manufacturers": "汽車製造商",
    "auto-parts": "汽車零件",
    "auto-truck-dealerships": "汽車與卡車經銷商",
    "department-stores": "百貨公司",
    "footwear-accessories": "鞋類與配件",
    "furnishings-fixtures-appliances": "家具與家電",
    "gambling": "博弈業",
    "home-improvement-retail": "家居裝修零售",
    "internet-retail": "網路零售",
    "leisure": "休閒",
    "lodging": "住宿",
    "luxury-goods": "奢侈品",
    "packaging-containers": "包裝與容器",
    "personal-services": "個人服務",
    "recreational-vehicles": "休閒車輛",
    "residential-construction": "住宅建設",
    "resorts-casinos": "渡假村與賭場",
    "restaurants": "餐廳",
    "specialty-retail": "專門零售",
    "textile-manufacturing": "紡織品製造",
    "travel-services": "旅遊服務",

    # 必需消費品
    "beverages-brewers": "啤酒飲料",
    "beverages-non-alcoholic": "非酒精飲料",
    "beverages-wineries-distilleries": "酒莊與酒廠",
    "confectioners": "糖果製造",
    "discount-stores": "折扣商店",
    "education-training-services": "教育與培訓服務",
    "farm-products": "農產品",
    "food-distribution": "食品分銷",
    "grocery-stores": "雜貨店",
    "household-personal-products": "家庭與個人用品",
    "packaged-foods": "包裝食品",
    "tobacco": "菸草",

    # 能源
    "oil-gas-drilling": "油氣鑽探",
    "oil-gas-e-p": "油氣勘探與生產",
    "oil-gas-equipment-services": "油氣設備與服務",
    "oil-gas-integrated": "綜合油氣",
    "oil-gas-midstream": "油氣中游",
    "oil-gas-refining-marketing": "油氣煉製與行銷",
    "thermal-coal": "熱煤",
    "uranium": "鈾業",

    # 金融
    "asset-management": "資產管理",
    "banks-diversified": "多元化銀行",
    "banks-regional": "區域銀行",
    "capital-markets": "資本市場",
    "credit-services": "信用服務",
    "financial-conglomerates": "金融集團",
    "financial-data-stock-exchanges": "金融數據與交易所",
    "insurance-brokers": "保險經紀",
    "insurance-diversified": "多元化保險",
    "insurance-life": "人壽保險",
    "insurance-property-casualty": "財產與意外保險",
    "insurance-reinsurance": "再保險",
    "insurance-specialty": "專業保險",
    "mortgage-finance": "房貸金融",
    "shell-companies": "空殼公司",

    # 醫療健康
    "biotechnology": "生物科技",
    "diagnostics-research": "診斷與研究",
    "drug-manufacturers-general": "藥品製造商—綜合",
    "drug-manufacturers-specialty-generic": "藥品製造商—專利與學名藥",
    "health-information-services": "健康資訊服務",
    "healthcare-plans": "健康保險計劃",
    "medical-care-facilities": "醫療機構",
    "medical-devices": "醫療設備",
    "medical-distribution": "醫療分銷",
    "medical-instruments-supplies": "醫療器材與耗材",
    "pharmaceutical-retailers": "藥局零售",

    # 工業
    "aerospace-defense": "航太與國防",
    "airlines": "航空公司",
    "airports-air-services": "機場與航空服務",
    "building-products-equipment": "建築產品與設備",
    "business-equipment-supplies": "商業設備與用品",
    "conglomerates": "企業集團",
    "consulting-services": "顧問服務",
    "electrical-equipment-parts": "電機設備與零件",
    "engineering-construction": "工程與建設",
    "farm-heavy-construction-machinery": "農業與重型建機",
    "industrial-distribution": "工業分銷",
    "infrastructure-operations": "基礎建設營運",
    "integrated-freight-logistics": "綜合貨運與物流",
    "marine-shipping": "海運",
    "metal-fabrication": "金屬製造",
    "pollution-treatment-controls": "污染處理與控制",
    "railroads": "鐵路",
    "rental-leasing-services": "租賃服務",
    "security-protection-services": "保全與防護服務",
    "specialty-business-services": "特殊商業服務",
    "specialty-industrial-machinery": "特殊工業機械",
    "staffing-employment-services": "人力派遣與職業介紹",
    "tools-accessories": "工具與配件",
    "trucking": "卡車運輸",
    "waste-management": "廢棄物管理",

    # 房地產
    "real-estate-development": "房地產開發",
    "real-estate-diversified": "多元化房地產",
    "real-estate-services": "房地產服務",
    "reit-diversified": "不動產投資信託—綜合",
    "reit-healthcare-facilities": "不動產投資信託—醫療",
    "reit-hotel-motel": "不動產投資信託—飯店",
    "reit-industrial": "不動產投資信託—工業",
    "reit-mortgage": "不動產投資信託—房貸",
    "reit-office": "不動產投資信託—辦公",
    "reit-residential": "不動產投資信託—住宅",
    "reit-retail": "不動產投資信託—零售",
    "reit-specialty": "不動產投資信託—特殊",

    # 資訊科技
    "communication-equipment": "通訊設備",
    "computer-hardware": "電腦硬體",
    "consumer-electronics": "消費性電子",
    "electronic-components": "電子零組件",
    "electronics-computer-distribution": "電子與電腦分銷",
    "information-technology-services": "資訊科技服務",
    "scientific-technical-instruments": "科學與技術儀器",
    "semiconductor-equipment-materials": "半導體設備與材料",
    "semiconductors": "半導體",
    "software-application": "應用軟體",
    "software-infrastructure": "基礎軟體",
    "solar": "太陽能",

    # 公用事業
    "utilities-diversified": "綜合公用事業",
    "utilities-independent-power-producers": "獨立電力生產商",
    "utilities-regulated-electric": "公用事業—電力",
    "utilities-regulated-gas": "公用事業—天然氣",
    "utilities-regulated-water": "公用事業—自來水",
    "utilities-renewable": "再生能源"
}

input_file = '../csv/few_reports.csv'
df = pd.read_csv(input_file)

df["Sector"] = ""
df["Industry"] = ""

for i, row in df.iterrows():
    ticker_symbol = str(row["Ticker"]).strip()
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        df.at[i, "Sector"] = info.get("sector", "N/A/N/A/N/A/N/A/N/A/N/A/")
        df.at[i, "Industry"] = info.get("industry", "N/A/N/A/N/A/N/A/N/A/N/A/")
        print(f"{i}.{ticker_symbol}")
    except Exception as e:
        df.at[i, "Sector"] = "N/A"
        df.at[i, "Industry"] = "N/A"
        print(f"{i}.{ticker_symbol} error {e}")
    time.sleep(1)

df["Industry"] = (
    df["Industry"]
    .str.lower()
    .str.replace(" & ", "-", regex=False)
    .str.replace("—", "-", regex=False)
    .str.replace("&", "-", regex=False)
    .str.replace(" - ", "-", regex=False)
    .str.replace(" ", "-", regex=False)
    .str.replace(" — ", "-", regex=False)
)

df["Sector"] = df["Sector"].map(sector_map).fillna("未知")
df["Industry"] = df["Industry"].map(industry_map).fillna("未知")

output_file = '../csv/few_reports_with_sector.csv'
df.to_csv(output_file, index=False)