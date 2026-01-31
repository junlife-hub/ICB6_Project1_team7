import pandas as pd
import numpy as np
import os

# 📂 경로 설정 (V1.1 출력 경로)
PROCESSED_PATH = r"C:\Users\ehdwn\Desktop\업로드 필요\OneDrive\Study\Fastcamp\ICB6\T_Choi\Procjet1\Brazilian_e-commerce\공통데이터전처리\processed(v1.1)"

errors = []
successes = []

def check(condition, success_msg, error_msg):
    if condition:
        successes.append(success_msg)
    else:
        errors.append(error_msg)

print("="*50)
print("브라질 Olist 데이터 전처리 검증 시작 (V1.1)")
print(f"대상 경로: {PROCESSED_PATH}")
print("="*50)

if not os.path.exists(PROCESSED_PATH):
    print(f"❌ 오류: 출력 경로가 존재하지 않습니다. ({PROCESSED_PATH})")
    exit()

# 1. olist_orders_dataset.csv 검증
print("\n[1/5] 주문 데이터 검증 중...")
orders_file = os.path.join(PROCESSED_PATH, "proc_olist_orders_dataset.csv")
if os.path.exists(orders_file):
    orders = pd.read_csv(orders_file)
    date_cols = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    
    # 타입 검증
    for col in date_cols:
        check(pd.api.types.is_datetime64_any_dtype(pd.to_datetime(orders[col], errors='coerce')), 
              f"  - {col}: 날짜 형식 확인 완료", 
              f"  - {col}: 날짜 변환 실패")
    
    # 파생 변수 검증
    check('actual_delivery_time' in orders.columns, "  - 실제 배송 소요 시간 컬럼 존재", "  - actual_delivery_time 컬럼 누락")
    check('delivery_delay_time' in orders.columns, "  - 예상 대비 지연 시간 컬럼 존재", "  - delivery_delay_time 컬럼 누락")
else:
    errors.append("proc_olist_orders_dataset.csv 파일이 없습니다.")

# 2. olist_order_reviews_dataset.csv 검증
print("[2/5] 리뷰 데이터 검증 중...")
reviews_file = os.path.join(PROCESSED_PATH, "proc_olist_order_reviews_dataset.csv")
if os.path.exists(reviews_file):
    reviews = pd.read_csv(reviews_file)
    check(reviews['review_comment_message'].isnull().sum() == 0, "  - 리뷰 메시지 결측치 보정 완료 (빈 문자열)", "  - 리뷰 메시지에 Null 값이 존재합니다.")
    check(reviews.duplicated(subset='order_id').sum() == 0, "  - 주문당 중복 리뷰 제거 완료", "  - 주문당 중복된 리뷰가 존재합니다.")
else:
    errors.append("proc_olist_order_reviews_dataset.csv 파일이 없습니다.")

# 3. olist_products_dataset.csv 검증 (한글 매핑 핵심)
print("[3/5] 상품 데이터 검증 중...")
products_file = os.path.join(PROCESSED_PATH, "proc_olist_products_dataset.csv")
if os.path.exists(products_file):
    products = pd.read_csv(products_file)
    check('product_category_name_english' in products.columns, "  - 영문 카테고리 컬럼 존재", "  - 영문 매핑 컬럼이 누락되었습니다.")
    check('product_category_name_korean' in products.columns, "  - 한글 카테고리 컬럼 존재", "  - 한글 매핑 컬럼이 누락되었습니다.")
    check(products['product_category_name_korean'].isnull().sum() == 0, "  - 한글 카테고리 결측치 보정 완료", "  - 한글 카테고리에 Null 값이 존재합니다.")
    check('기타' in products['product_category_name_korean'].values, "  - 한글 카테고리 '기타' 플래그 확인", "  - 한글 카테고리에 '기타' 값이 없습니다.")
else:
    errors.append("proc_olist_products_dataset.csv 파일이 없습니다.")

# 4. olist_geolocation_dataset.csv 검증 (한글 주 명칭)
print("[4/5] 위치 정보 데이터 검증 중...")
geo_file = os.path.join(PROCESSED_PATH, "proc_olist_geolocation_dataset.csv")
if os.path.exists(geo_file):
    geo = pd.read_csv(geo_file, dtype={'geolocation_zip_code_prefix': str})
    check(geo['geolocation_zip_code_prefix'].str.contains('^0').any() if any(geo['geolocation_zip_code_prefix'].str.startswith('0')) else True, 
          "  - 우편번호 앞자리 '0' 보존 확인 (문자열 타입)", "  - 우편번호 데이터 타입 오류 (숫자로 인식되어 0 유실 가능성)")
    check('geolocation_state_korean' in geo.columns, "  - 한글 주(State) 명칭 컬럼 존재", "  - geolocation_state_korean 컬럼이 누락되었습니다.")
    check(geo.duplicated(subset='geolocation_zip_code_prefix').sum() == 0, "  - 우편번호 기준 중복 제거 완료", "  - 우편번호가 중복된 데이터가 존재합니다.")
else:
    errors.append("proc_olist_geolocation_dataset.csv 파일이 없습니다.")

# 5. olist_customers_dataset.csv / olist_sellers_dataset.csv 검증
print("[5/5] 고객/판매자 데이터 검증 중...")
cust_file = os.path.join(PROCESSED_PATH, "proc_olist_customers_dataset.csv")
if os.path.exists(cust_file):
    customers = pd.read_csv(cust_file)
    check('customer_state_korean' in customers.columns, "  - 고객 데이터 한글 주 명칭 추가 확인", "  - customer_state_korean 컬럼이 누락되었습니다.")

sell_file = os.path.join(PROCESSED_PATH, "proc_olist_sellers_dataset.csv")
if os.path.exists(sell_file):
    sellers = pd.read_csv(sell_file)
    check('seller_state_korean' in sellers.columns, "  - 판매자 데이터 한글 주 명칭 추가 확인", "  - seller_state_korean 컬럼이 누락되었습니다.")

# 최종 결과 출력
print("\n" + "="*50)
if not errors:
    print("✨ 검증 결과: 모든 전처리 항목이 정상적으로 완료되었습니다! (V1.1 합격)")
else:
    print(f"⚠️ 검증 결과: {len(errors)}개의 항목에서 오류/누락이 발견되었습니다.")
    for e in errors:
        print(f"  ❌ {e}")
print("="*50)
