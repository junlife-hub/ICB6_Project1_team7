import pandas as pd
import numpy as np
import os

# 1. 경로 설정 (Path Configuration)
PROCESSED_DATA_PATH = r'C:\Users\ehdwn\Desktop\업로드 필요\OneDrive\Study\Fastcamp\ICB6\T_Choi\Procjet1\Brazilian_e-commerce\공통데이터전처리\processed'
INTEGRATED_RESULT_PATH = r'C:\Users\ehdwn\Desktop\업로드 필요\OneDrive\Study\Fastcamp\ICB6\T_Choi\Procjet1\Brazilian_e-commerce\공통데이터전처리\integrated'

# 출력 폴더 생성
if not os.path.exists(INTEGRATED_RESULT_PATH):
    os.makedirs(INTEGRATED_RESULT_PATH)
    print(f"Created directory: {INTEGRATED_RESULT_PATH}")

def load_processed(filename):
    path = os.path.join(PROCESSED_DATA_PATH, f"proc_{filename}")
    return pd.read_csv(path)

def save_integrated(df, filename):
    path = os.path.join(INTEGRATED_RESULT_PATH, f"integrated_{filename}")
    df.to_csv(path, index=False)
    print(f"Saved: {path}")

print("Starting data integration...")

# 데이터 로드
orders = load_processed('olist_orders_dataset.csv')
customers = load_processed('olist_customers_dataset.csv')
reviews = load_processed('olist_order_reviews_dataset.csv')
payments = load_processed('olist_order_payments_dataset.csv')
order_items = load_processed('olist_order_items_dataset.csv')
products = load_processed('olist_products_dataset.csv')
sellers = load_processed('olist_sellers_dataset.csv')
geo = load_processed('olist_geolocation_dataset.csv')

# --- 1. [테이블 A] 주문 마스터 통합 (integrated_orders_master) ---
print("Creating integrated_orders_master...")

# 결제 데이터 Group By
payments_agg = payments.groupby('order_id')['payment_value'].sum().reset_index()
payments_agg.rename(columns={'payment_value': 'total_payment_value'}, inplace=True)

# 통합 시작
orders_master = orders.merge(customers, on='customer_id', how='left')
orders_master = orders_master.merge(reviews, on='order_id', how='left')
orders_master = orders_master.merge(payments_agg, on='order_id', how='left')

save_integrated(orders_master, 'orders_master.csv')


# --- 2. [테이블 B] 판매 상세 마스터 통합 (integrated_sales_item_master) ---
print("Creating integrated_sales_item_master...")

# 중복 방지를 위한 unique 체크 (이미 전처리에서 처리했으나 확인 차원)
# products와 sellers가 unique한지 확인해서 merge
sales_item_master = order_items.merge(products, on='product_id', how='left')
sales_item_master = sales_item_master.merge(sellers, on='seller_id', how='left')

save_integrated(sales_item_master, 'sales_item_master.csv')


# --- 3. [테이블 C] 물류 배송 경로 통합 (integrated_logistics_master) ---
print("Creating integrated_logistics_master...")

# Table B 기반
logistics_master = sales_item_master.copy()

# 1. orders를 붙여 customer_zip_code_prefix 확보
# 필요한 컬럼만 선택해서 merge
orders_subset = orders[['order_id', 'customer_id']]
customers_subset = customers[['customer_id', 'customer_zip_code_prefix']]
order_customer_zip = orders_subset.merge(customers_subset, on='customer_id', how='left')

logistics_master = logistics_master.merge(order_customer_zip[['order_id', 'customer_zip_code_prefix']], on='order_id', how='left')

# 2. geolocation 결합
# 우편번호 컬럼들이 string 타입인지 확인 (전처리에서 처리됨)
geo['geolocation_zip_code_prefix'] = geo['geolocation_zip_code_prefix'].astype(str)
logistics_master['seller_zip_code_prefix'] = logistics_master['seller_zip_code_prefix'].astype(str)
logistics_master['customer_zip_code_prefix'] = logistics_master['customer_zip_code_prefix'].astype(str)

# 1회차: 판매자 위치
logistics_master = logistics_master.merge(
    geo[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']].rename(
        columns={'geolocation_lat': 'seller_lat', 'geolocation_lng': 'seller_lng', 'geolocation_zip_code_prefix': 'seller_zip_code_prefix'}
    ),
    on='seller_zip_code_prefix',
    how='left'
)

# 2회차: 구매자 위치
logistics_master = logistics_master.merge(
    geo[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']].rename(
        columns={'geolocation_lat': 'customer_lat', 'geolocation_lng': 'customer_lng', 'geolocation_zip_code_prefix': 'customer_zip_code_prefix'}
    ),
    on='customer_zip_code_prefix',
    how='left'
)

save_integrated(logistics_master, 'logistics_master.csv')


# --- 4. 통합 결과 검증 (Sanity Check) ---
print("\n--- 4. Data Integration Sanity Check ---")

def run_check():
    print(f"Table A rows: {len(orders_master)} (Expect: {len(orders)})")
    if len(orders_master) == len(orders): print("  - Row count match: OK")
    else: print(f"  - Row count mismatch: FAILED ({len(orders_master)} vs {len(orders)})")

    print(f"Table B rows: {len(sales_item_master)} (Expect: {len(order_items)})")
    if len(sales_item_master) == len(order_items): print("  - Row count match: OK")
    else: print(f"  - Row count mismatch: FAILED ({len(sales_item_master)} vs {len(order_items)})")

    print(f"Table C rows: {len(logistics_master)} (Expect: {len(sales_item_master)})")
    if len(logistics_master) == len(sales_item_master): print("  - Row count match: OK")
    else: print(f"  - Row count mismatch: FAILED ({len(logistics_master)} vs {len(sales_item_master)})")

    # Payment Sum Check
    orig_payment_sum = payments['payment_value'].sum()
    integrated_payment_sum = orders_master['total_payment_value'].sum()
    print(f"Payment Sum Check: Original={orig_payment_sum:.2f}, Integrated={integrated_payment_sum:.2f}")
    if abs(orig_payment_sum - integrated_payment_sum) < 0.01: print("  - Sum match: OK")
    else: print("  - Sum mismatch: FAILED")

    # Item Price Sum Check
    orig_price_sum = order_items['price'].sum()
    integrated_price_sum = sales_item_master['price'].sum()
    print(f"Price Sum Check: Original={orig_price_sum:.2f}, Integrated={integrated_price_sum:.2f}")
    if abs(orig_price_sum - integrated_price_sum) < 0.01: print("  - Sum match: OK")
    else: print("  - Sum mismatch: FAILED")

run_check()

print("\nData integration completed.")
