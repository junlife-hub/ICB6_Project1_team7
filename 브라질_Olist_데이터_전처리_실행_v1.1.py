import pandas as pd
import os

# 📂 경로 설정
RAW_PATH = r"C:\Users\ehdwn\Desktop\업로드 필요\OneDrive\Study\Fastcamp\ICB6\T_Choi\Procjet1\Brazilian_e-commerce\dataset"
OUTPUT_PATH = r"C:\Users\ehdwn\Desktop\업로드 필요\OneDrive\Study\Fastcamp\ICB6\T_Choi\Procjet1\Brazilian_e-commerce\공통데이터전처리\processed(v1.1)"

if not os.path.exists(OUTPUT_PATH):
    os.makedirs(OUTPUT_PATH)

# 매핑 데이터 정의
category_map_ko = {
    'health_beauty': '헬스/뷰티',
    'computers_accessories': '컴퓨터/주변기기',
    'auto': '자동차용품',
    'bed_bath_table': '침구/생활',
    'furniture_decor': '가구/인테리어',
    'sports_leisure': '스포츠/레저',
    'baby': '유아용품',
    'housewares': '주방용품',
    'watches_gifts': '시계/선물',
    'telephony': '통신기기',
    'toys': '완구/교구',
    'cool_stuff': '아이디어상품',
    'perfumery': '향수',
    'garden_tools': '가드닝/공구',
    'pet_shop': '반려동물용품',
    'electronics': '가전',
    'construction_tools_lights': '건축자재/조명',
    'luggage_accessories': '가방/액세서리',
    'others': '기타'
}

state_map_ko = {
    'AC': '아크리', 'AL': '알라고아스', 'AM': '아마조나스', 'AP': '아마파',
    'BA': '바이아', 'CE': '세아라', 'DF': '연방특구', 'ES': '에스피리투산투',
    'GO': '고이아스', 'MA': '마라냥', 'MG': '미나스제라이스', 'MS': '마투그로수두술',
    'MT': '마투그로수', 'PA': '파라', 'PB': '파라이바', 'PE': '페르남부쿠',
    'PI': '피아우이', 'PR': '파라나', 'RJ': '리우데자네이루', 'RN': '리오그란데도노르테',
    'RO': '혼도니아', 'RR': '로라이마', 'RS': '리오그란데도술', 'SC': '산타카타리나',
    'SE': '세르지피', 'SP': '상파울루', 'TO': '토칸칭스'
}

# 1. olist_orders_dataset.csv
print("Processing orders...")
orders = pd.read_csv(os.path.join(RAW_PATH, "olist_orders_dataset.csv"))
date_cols = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']
for col in date_cols:
    orders[col] = pd.to_datetime(orders[col])

orders['order_id'] = orders['order_id'].str.strip()
orders['customer_id'] = orders['customer_id'].str.strip()

# 결측 처리 및 플래그 부여
orders.loc[orders['order_delivered_customer_date'].isnull(), 'order_delivered_customer_date_flag'] = '미배송/취소(' + orders['order_status'] + ')'

# 파생 변수
orders['actual_delivery_time'] = (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).dt.total_seconds() / 86400
orders['delivery_delay_time'] = (orders['order_delivered_customer_date'] - orders['order_estimated_delivery_date']).dt.total_seconds() / 86400

orders.to_csv(os.path.join(OUTPUT_PATH, "proc_olist_orders_dataset.csv"), index=False)

# 2. olist_order_items_dataset.csv
print("Processing order items...")
items = pd.read_csv(os.path.join(RAW_PATH, "olist_order_items_dataset.csv"))
items['shipping_limit_date'] = pd.to_datetime(items['shipping_limit_date'])
for col in ['order_id', 'product_id', 'seller_id']:
    items[col] = items[col].str.strip()
items.to_csv(os.path.join(OUTPUT_PATH, "proc_olist_order_items_dataset.csv"), index=False)

# 3. olist_order_reviews_dataset.csv
print("Processing reviews...")
reviews = pd.read_csv(os.path.join(RAW_PATH, "olist_order_reviews_dataset.csv"))
for col in ['review_creation_date', 'review_answer_timestamp']:
    reviews[col] = pd.to_datetime(reviews[col])
reviews['order_id'] = reviews['order_id'].str.strip()
reviews['review_comment_title'] = reviews['review_comment_title'].fillna('')
reviews['review_comment_message'] = reviews['review_comment_message'].fillna('')

reviews = reviews.sort_values('review_answer_timestamp', ascending=False)
reviews = reviews.drop_duplicates(subset='order_id', keep='first')
reviews.to_csv(os.path.join(OUTPUT_PATH, "proc_olist_order_reviews_dataset.csv"), index=False)

# 4. olist_products_dataset.csv
print("Processing products...")
products = pd.read_csv(os.path.join(RAW_PATH, "olist_products_dataset.csv"))
products['product_id'] = products['product_id'].str.strip()
products['product_category_name'] = products['product_category_name'].str.strip().fillna('others')

# 영문 매핑
translation = pd.read_csv(os.path.join(RAW_PATH, "product_category_name_translation.csv"), encoding='utf-8-sig')
products = products.merge(translation, on='product_category_name', how='left')
products['product_category_name_english'] = products['product_category_name_english'].fillna('others')

# 한글 매핑
products['product_category_name_korean'] = products['product_category_name_english'].map(category_map_ko).fillna('기타')
products.to_csv(os.path.join(OUTPUT_PATH, "proc_olist_products_dataset.csv"), index=False)

# 5. olist_geolocation_dataset.csv
print("Processing geolocation...")
geo = pd.read_csv(os.path.join(RAW_PATH, "olist_geolocation_dataset.csv"))
geo['geolocation_zip_code_prefix'] = geo['geolocation_zip_code_prefix'].astype(str)
geo_grouped = geo.groupby('geolocation_zip_code_prefix').agg({
    'geolocation_lat': 'mean',
    'geolocation_lng': 'mean',
    'geolocation_city': 'first',
    'geolocation_state': 'first'
}).reset_index()

geo_grouped['geolocation_state_korean'] = geo_grouped['geolocation_state'].map(state_map_ko)
geo_grouped.to_csv(os.path.join(OUTPUT_PATH, "proc_olist_geolocation_dataset.csv"), index=False)

# 6. olist_customers_dataset.csv
print("Processing customers...")
customers = pd.read_csv(os.path.join(RAW_PATH, "olist_customers_dataset.csv"))
for col in ['customer_id', 'customer_unique_id']:
    customers[col] = customers[col].str.strip()
customers['customer_zip_code_prefix'] = customers['customer_zip_code_prefix'].astype(str)
customers['customer_state_korean'] = customers['customer_state'].map(state_map_ko)
customers.to_csv(os.path.join(OUTPUT_PATH, "proc_olist_customers_dataset.csv"), index=False)

# 7. olist_sellers_dataset.csv
print("Processing sellers...")
sellers = pd.read_csv(os.path.join(RAW_PATH, "olist_sellers_dataset.csv"))
sellers['seller_id'] = sellers['seller_id'].str.strip()
sellers['seller_zip_code_prefix'] = sellers['seller_zip_code_prefix'].astype(str)
sellers['seller_state_korean'] = sellers['seller_state'].map(state_map_ko)
sellers.to_csv(os.path.join(OUTPUT_PATH, "proc_olist_sellers_dataset.csv"), index=False)

print("Done!")
