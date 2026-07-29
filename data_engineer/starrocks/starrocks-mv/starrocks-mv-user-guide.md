# StarRocks Materialized View 사용자 가이드 (v4.1.1 기준)

> **대상 독자**: Data Platform에서 쿼리를 실행하는 분석가·엔지니어, MV 생성 권한이 있는 팀
> **환경**: base 테이블은 Iceberg(external catalog), MV는 StarRocks 4.1.1에서 생성·관리
> **문서 성격**: 사용 가이드. 내부 동작 원리는 별도 문서 *StarRocks MV 엔지니어링 심화 가이드* 참고.
> 예시의 `iceberg_cat.sales.orders` 등은 예시 이름이며, 실제 catalog/스키마 이름으로 바꿔 사용한다.

---

## 목차

1. [MV를 왜 쓰는가 — 3분 요약](#1-mv를-왜-쓰는가--3분-요약)
2. [만들기 전에: 기존 MV부터 확인](#2-만들기-전에-기존-mv부터-확인)
3. [MV 생성하기 — 상황별 레시피](#3-mv-생성하기--상황별-레시피)
4. [쿼리는 어떻게 쓰나 — Query Rewrite 이해하기](#4-쿼리는-어떻게-쓰나--query-rewrite-이해하기)
5. [신선도(Freshness) 이해하기](#5-신선도freshness-이해하기)
6. [검증 시나리오](#6-검증-시나리오)
7. [주의사항과 흔한 실수](#7-주의사항과-흔한-실수)
8. [FAQ](#8-faq)

---

## 1. MV를 왜 쓰는가 — 3분 요약

**문제**: 대시보드·리포트·정기 분석 쿼리는 같은 조인/집계를 하루에도 수십~수백 번 반복한다. 매번 Iceberg 원천(S3의 Parquet)을 읽고 같은 계산을 다시 하면, 응답은 느리고 클러스터 컴퓨팅은 낭비된다.

**해법**: Materialized View는 그 계산 결과를 미리 만들어 StarRocks 내부에 저장해두고 주기적으로 갱신한다. 효과는 두 가지다.

- **빠르다**: 원격 스토리지 스캔 + 조인/집계 대신, 이미 계산된 결과를 로컬에서 읽는다.
- **싸다**: 계산을 "쿼리마다"가 아니라 "refresh 주기마다 변경분에 대해서만" 수행한다.

**그리고 StarRocks MV의 결정적 장점 — 쿼리를 바꿀 필요가 없다.** 여러분이 base 테이블을 조회하면, 옵티마이저가 조건에 맞는 MV가 있는지 판단해 자동으로 MV를 읽도록 플랜을 바꾼다(transparent query rewrite). 대시보드의 SQL, BI 도구의 쿼리를 수정하지 않아도 가속이 적용된다. 별도 집계 테이블 + Airflow 파이프라인을 직접 유지하는 방식과 비교하면, 갱신 스케줄·증분 처리·쿼리 라우팅을 전부 엔진이 대신해 주는 셈이다.

### 언제 쓰면 좋은가

- 같은 집계/조인 패턴의 쿼리가 **반복적으로** 실행된다 (대시보드, 정기 리포트, 서빙 API)
- 쿼리가 느린 이유가 **대량 스캔 + 무거운 집계/조인**이다
- 데이터가 **몇 분~몇 시간 지연되어도 괜찮다** (실시간 정합성이 필수가 아니다)

### 언제 쓰지 말아야 하는가 (안티패턴)

- **일회성 탐색 쿼리**: 재사용이 없으면 refresh 비용만 낭비된다. 그냥 base를 조회한다.
- **매번 조건이 완전히 다른 ad-hoc 분석**: rewrite가 걸릴 공통 패턴이 없다.
- **실시간 정합성이 필수인 조회**: MV는 최종적 일관성이다 (5장). base를 직접 조회한다.
- **이미 충분히 빠른 쿼리**: Data Cache 히트로 해결되는 수준이면 MV까지 갈 필요 없다.
- **거의 안 바뀌는 소형 테이블 조회**: 이득이 미미하다.

> 판단이 애매하면 플랫폼 팀에 슬로우 쿼리와 실행 빈도를 공유해 상담한다. MV는 "만들수록 좋은 것"이 아니라 refresh 컴퓨팅·저장소·옵티마이저 부하를 소모하는 자원이다.

---

## 2. 만들기 전에: 기존 MV부터 확인

**같은 목적의 MV가 이미 있는지 반드시 먼저 확인한다.** 중복 MV는 refresh 비용을 이중으로 쓰고, 옵티마이저의 후보 탐색도 느리게 만든다.

```sql
-- 1) 데이터베이스의 MV 목록과 상태 확인
SHOW MATERIALIZED VIEWS FROM dw;

-- 이름 패턴으로 검색
SHOW MATERIALIZED VIEWS FROM dw WHERE NAME LIKE 'mv_sales%';

-- 2) 정의 쿼리까지 확인 (내가 만들려는 것과 겹치는지 판단)
SELECT *
FROM information_schema.materialized_views
WHERE table_name LIKE 'mv_sales%';
```

- DataHub에서 base 테이블을 검색하면 해당 테이블 위에 구축된 MV 리니지를 확인할 수 있다. *(플랫폼에서 MV 리니지 노출을 준비 중이며, 적용 전까지는 위 SQL로 확인한다.)*
- 비슷한 MV가 이미 있다면: 그대로 활용하거나, 부족한 부분을 플랫폼 팀과 논의한다. 기존 MV 위에 nested MV를 얹는 방법도 있다 (플랫폼 팀 협의 대상).

---

## 3. MV 생성하기 — 상황별 레시피

### 3.0 공통 규칙 (플랫폼 컨벤션 — 제안, 팀 확정 후 갱신)

- **이름**: `mv_<도메인>_<집계단위>_<목적>` — 예: `mv_sales_daily_store_agg`
- **필수**: `COMMENT`에 목적·오너 팀·문의 채널 기재, freshness 계약(`mv_rewrite_staleness_second`) 명시
- **refresh 주기**: 사용자 요구 신선도보다 짧게, 단 서빙 피크 시간대는 피해서 `START` 시각 조정
- Iceberg base의 MV는 **변경 즉시 자동 갱신이 되지 않는다.** 스케줄(`EVERY`) 또는 수동 refresh만 가능하므로, "refresh 주기 = 신선도 하한"이다.

### 3.1 기본형: 일 단위 집계 MV

가장 흔한 케이스. 일별·매장별 매출 집계를 반복 조회하는 대시보드를 가속한다.

```sql
CREATE MATERIALIZED VIEW dw.mv_sales_daily_store_agg
COMMENT "일별/매장별 매출 집계. 오너: 커머스데이터팀(#commerce-data). 신선도 계약: 최대 1시간"
PARTITION BY date_trunc('day', order_dt)     -- base(일 파티션)와 매핑 → 변경된 날짜만 재계산
DISTRIBUTED BY HASH(store_id)
ORDER BY (store_id, order_dt)                -- 자주 쓰는 필터 컬럼을 정렬 키로
REFRESH ASYNC START ('2026-07-20 03:00:00') EVERY (INTERVAL 1 HOUR)
PROPERTIES (
    "partition_refresh_number" = "2",              -- 한 번에 2개 파티션씩 갱신
    "mv_rewrite_staleness_second" = "3600"         -- 1시간 이내 지연은 rewrite 허용 (freshness 계약)
)
AS
SELECT
    order_dt,
    store_id,
    sum(amount)        AS total_amount,
    count(*)           AS order_cnt,
    count(DISTINCT customer_id) AS customer_cnt
FROM iceberg_cat.sales.orders
GROUP BY order_dt, store_id;
```

포인트:

- `PARTITION BY`를 base의 파티션 컬럼과 맞추면(동일 단위 또는 `date_trunc`로 상위 단위) **변경된 파티션만 갱신**된다. 이게 없으면 매번 전체 재계산이 될 수 있다.
- **필터로 쓸 컬럼은 반드시 SELECT에 포함한다.** `WHERE store_id = ...` 조회를 rewrite하려면 `store_id`가 MV 출력에 있어야 한다.
- 집계 결과에 **원하는 상세 단위의 컬럼을 모두 남겨라.** 일/매장 단위로 만들어두면 월 단위, 전 매장 합계 쿼리도 자동으로 rewrite된다(상위 집계로 롤업). 반대로 너무 뭉쳐서 만들면 세밀한 쿼리에는 못 쓴다.

### 3.2 조인 가속 MV

팩트-디멘전 조인이 반복되는 경우, 조인 결과 자체를 MV로 만든다.

```sql
CREATE MATERIALIZED VIEW dw.mv_orders_with_store
COMMENT "주문-매장 조인 평면화. 오너: 커머스데이터팀"
PARTITION BY date_trunc('day', o.order_dt)
DISTRIBUTED BY HASH(o.order_id)
REFRESH ASYNC EVERY (INTERVAL 2 HOUR)
PROPERTIES ("mv_rewrite_staleness_second" = "7200")
AS
SELECT
    o.order_id, o.order_dt, o.amount, o.customer_id,
    s.store_id, s.store_name, s.region, s.store_type
FROM iceberg_cat.sales.orders o
JOIN iceberg_cat.master.stores s ON o.store_id = s.store_id;
```

이후 이 조인을 포함하는 쿼리(추가 필터·집계가 붙어도)는 rewrite 후보가 된다. 조인 키 관계가 명확한 정규형 조인일수록 rewrite 성공률이 높다.

### 3.3 count(distinct) 가속 MV

정확한 distinct 카운트는 비싸다. bitmap 집계 상태를 MV에 저장하면 `count(distinct)` 쿼리가 자동으로 rewrite된다.

```sql
CREATE MATERIALIZED VIEW dw.mv_dau_bitmap
COMMENT "일별 활성 사용자 bitmap. count(distinct user_id) 가속용"
PARTITION BY date_trunc('day', event_dt)
DISTRIBUTED BY HASH(event_dt)
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
AS
SELECT
    event_dt,
    channel,
    bitmap_union(to_bitmap(user_id)) AS user_bitmap   -- user_id는 정수형이어야 함
FROM iceberg_cat.log.app_events
GROUP BY event_dt, channel;
```

```sql
-- 사용자는 평소처럼 쓴다. 아래 쿼리가 자동으로 위 MV로 rewrite된다.
SELECT event_dt, count(DISTINCT user_id)
FROM iceberg_cat.log.app_events
WHERE event_dt >= '2026-07-01'
GROUP BY event_dt;
```

### 3.4 핫 구간만 유지하는 MV (TTL + Union rewrite)

전 기간을 MV로 들고 있기엔 저장 비용이 크고, 실제 조회의 대부분은 최근 구간인 경우.

```sql
CREATE MATERIALIZED VIEW dw.mv_sales_daily_hot
COMMENT "최근 3개월만 유지. 이전 구간 쿼리는 자동으로 base와 UNION 처리됨"
PARTITION BY date_trunc('day', order_dt)
DISTRIBUTED BY HASH(store_id)
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
PROPERTIES (
    "partition_ttl" = "3 MONTH"                      -- 3개월 지난 MV 파티션 자동 삭제
)
AS
SELECT order_dt, store_id, sum(amount) AS total_amount, count(*) AS order_cnt
FROM iceberg_cat.sales.orders
GROUP BY order_dt, store_id;
```

전체 기간을 조회하는 쿼리는 옵티마이저가 "최근 3개월 = MV, 그 이전 = base 직접"의 UNION 플랜으로 자동 조립한다. 사용자는 아무것도 신경 쓸 필요 없다.

### 3.5 refresh_mode 선택 (4.1 신기능 — 기본은 그대로 두면 된다)

4.1부터 MV 속성에 `refresh_mode`가 생겼다. **일반 사용자는 기본값(PCT)을 그대로 쓰면 된다.**

| | `pct` (기본) | `incremental` |
|---|---|---|
| 갱신 방식 | 변경된 파티션을 통째로 재계산 | 마지막 반영 이후 추가분(delta)만 계산 |
| query rewrite | 참여 — 쿼리 자동 가속 | **미참여 (4.1 현재)** — MV를 직접 조회해야 함 |
| 조건 | 제한 없음 | Iceberg **append-only** 테이블 한정, delete/컴팩션 발생 시 refresh 실패 |
| 용도 | 대시보드/리포트 가속 (대부분의 경우) | 저비용 고빈도 갱신이 필요한 파생 서빙 테이블 |

`incremental`은 대상 테이블의 컴팩션 정책과 충돌할 수 있어 **플랫폼 팀 협의 후에만 사용한다.** "쿼리가 자동으로 빨라지길" 원한다면 반드시 기본값(PCT)이어야 한다.

---

## 4. 쿼리는 어떻게 쓰나 — Query Rewrite 이해하기

### 4.1 원칙: base 테이블을 그대로 조회한다

MV를 쓰기 위해 쿼리를 바꿀 필요가 없다. **base 테이블에 대한 쿼리를 그대로 실행하면**, 옵티마이저가 사용할 수 있는 MV가 있는지 판단해 자동으로 바꿔치기한다.

```sql
-- 3.1의 mv_sales_daily_store_agg가 있는 상태에서, 아래 쿼리들은 전부 자동 rewrite 후보다.

-- (a) MV 정의와 거의 같은 쿼리 → 직접 매칭
SELECT order_dt, store_id, sum(amount)
FROM iceberg_cat.sales.orders
GROUP BY order_dt, store_id;

-- (b) 더 좁은 조건 → MV 위에 필터를 얹어서 rewrite (predicate 보상)
SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE store_id = 1024 AND order_dt >= '2026-07-01'
GROUP BY order_dt;

-- (c) 더 큰 집계 단위 → 일 단위 MV를 월 단위로 재집계해서 rewrite (rollup)
SELECT date_trunc('month', order_dt) AS ym, sum(amount)
FROM iceberg_cat.sales.orders
GROUP BY date_trunc('month', order_dt);
```

핵심 직관: **"내 쿼리 결과를 MV 결과로부터 계산해낼 수 있는가"** 를 옵티마이저가 판정한다. 텍스트가 같을 필요는 없고, 논리적으로 유도 가능하면 된다.

### 4.2 rewrite가 안 걸리는 대표 패턴

```sql
-- (x1) MV에 없는 컬럼을 요구
SELECT order_dt, coupon_id, sum(amount)          -- coupon_id가 MV 출력에 없음
FROM iceberg_cat.sales.orders
GROUP BY order_dt, coupon_id;
-- → MV로 유도 불가. base 스캔. 해당 패턴이 반복되면 MV 정의에 컬럼 추가를 검토.

-- (x2) MV보다 세밀한 집계 단위를 요구
SELECT order_dt, store_id, customer_id, sum(amount)   -- MV는 일/매장 단위까지만 있음
FROM iceberg_cat.sales.orders
GROUP BY order_dt, store_id, customer_id;
-- → 롤업은 위로만 가능하다. 아래로(더 세밀하게)는 불가.

-- (x3) 재집계 불가능한 집계의 rollup
-- 일 단위 MV의 count(DISTINCT customer_id)를 월 단위로 합산할 수는 없다 (중복 제거가 깨짐).
-- → distinct의 단위 간 롤업이 필요하면 3.3처럼 bitmap MV로 설계한다.

-- (x4) 비결정적 함수 포함
SELECT store_id, sum(amount)
FROM iceberg_cat.sales.orders
WHERE order_dt = current_date()                  -- 비결정적 함수
GROUP BY store_id;
-- → rewrite 불성립 가능. 날짜를 리터럴/파라미터로 바인딩해서 실행한다.
```

### 4.3 rewrite 여부를 직접 확인하는 법

```sql
EXPLAIN
SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01'
GROUP BY order_dt;
```

플랜의 스캔 노드에서 `TABLE:` 값을 확인한다. **MV 이름(`mv_sales_daily_store_agg`)이 보이면 rewrite된 것**이고, base 테이블 이름이 보이면 rewrite되지 않은 것이다.

rewrite가 안 되는 이유를 알고 싶으면:

```sql
TRACE REASON MV
SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01'
GROUP BY order_dt;
```

후보 MV별 탈락 사유(정의 불일치, 신선도 미달, INACTIVE 등)가 출력된다. 더 상세한 로그는 `TRACE LOGS MV <query>`.

### 4.4 MV를 직접 조회해야 하는 경우

`refresh_mode = incremental`인 MV(3.5절)는 4.1 현재 rewrite에 참여하지 않으므로 테이블처럼 직접 조회한다.

```sql
SELECT * FROM dw.mv_event_hourly_agg WHERE event_hour >= '2026-07-17 00:00:00';
```

일반(PCT) MV도 직접 조회할 수 있지만, 특별한 이유가 없으면 base를 조회하고 rewrite에 맡기는 것을 권장한다 — MV 구조가 바뀌어도 쿼리가 깨지지 않는다.

---

## 5. 신선도(Freshness) 이해하기

**MV의 데이터는 실시간이 아니다.** Iceberg base의 변경은 다음 refresh 때 반영되므로, 최악의 경우 "refresh 주기 + refresh 소요 시간"만큼 뒤처질 수 있다.

각 MV에는 신선도 계약이 명시되어 있다:

- **refresh 주기**: `SHOW MATERIALIZED VIEWS`에서 확인 (예: 1시간 간격)
- **staleness 허용치**: 속성 `mv_rewrite_staleness_second`. 이 시간 이내의 지연은 "괜찮은 것"으로 간주하고 rewrite가 계속 동작한다. COMMENT에도 사람이 읽을 수 있게 병기한다.

마지막 갱신 시각 확인:

```sql
SHOW MATERIALIZED VIEWS FROM dw WHERE NAME = 'mv_sales_daily_store_agg';
-- last_refresh 관련 필드에서 마지막 성공 시각과 상태를 확인한다.
```

**정합성이 그 순간 반드시 정확해야 하는 조회**(정산 마감, 감사 등)라면 MV rewrite를 끄고 base를 직접 읽는다:

```sql
SET enable_materialized_view_rewrite = false;   -- 현재 세션에서만 rewrite 비활성
SELECT ... FROM iceberg_cat.sales.orders ...;
SET enable_materialized_view_rewrite = true;
```

---

## 6. 검증 시나리오

MV를 만들었으면 "잘 만들어졌다"를 검증하고 나서 사용자에게 안내한다. 아래 시나리오는 **신규 MV 배포 전 필수 체크**이자, 운영 중 이상 징후 시의 점검 절차다.

### 6.1 데이터 정합성 검증 — MV 결과 = base 직접 계산 결과인가

**중요한 함정부터**: base 테이블을 조회해서 비교하려고 하면, 그 비교 쿼리 자체가 MV로 rewrite되어 "MV를 MV와 비교"하게 된다. **비교용 base 쿼리는 반드시 rewrite를 끄고 실행한다.**

```sql
-- STEP 0. 비교 직전에 refresh를 완료시켜 시점을 고정한다 (동기 모드로 완료 대기)
REFRESH MATERIALIZED VIEW dw.mv_sales_daily_store_agg WITH SYNC MODE;
-- 검증 구간 동안 base에 적재가 없는 시간대를 골라 수행한다.

-- STEP 1. 총량 비교 (빠른 스모크 테스트)
-- (a) MV 직접 조회
SELECT count(*) AS row_cnt, sum(total_amount) AS amt, sum(order_cnt) AS cnt
FROM dw.mv_sales_daily_store_agg
WHERE order_dt BETWEEN '2026-07-01' AND '2026-07-15';

-- (b) base 직접 계산 (rewrite 끄고!)
SET enable_materialized_view_rewrite = false;
SELECT count(*) AS row_cnt, sum(amount) AS amt, count(*) AS cnt   -- MV 정의와 동일한 집계로
FROM (
    SELECT order_dt, store_id, sum(amount) AS amount, count(*) AS cnt2
    FROM iceberg_cat.sales.orders
    WHERE order_dt BETWEEN '2026-07-01' AND '2026-07-15'
    GROUP BY order_dt, store_id
) t;
SET enable_materialized_view_rewrite = true;
```

```sql
-- STEP 2. 행 단위 비교 (총량이 일치해도 상쇄 오류가 있을 수 있다)
SET enable_materialized_view_rewrite = false;

WITH base_agg AS (
    SELECT order_dt, store_id, sum(amount) AS total_amount, count(*) AS order_cnt
    FROM iceberg_cat.sales.orders
    WHERE order_dt BETWEEN '2026-07-01' AND '2026-07-15'
    GROUP BY order_dt, store_id
),
mv_agg AS (
    SELECT order_dt, store_id, total_amount, order_cnt
    FROM dw.mv_sales_daily_store_agg
    WHERE order_dt BETWEEN '2026-07-01' AND '2026-07-15'
)
SELECT
    coalesce(b.order_dt, m.order_dt)   AS order_dt,
    coalesce(b.store_id, m.store_id)   AS store_id,
    b.total_amount AS base_amt, m.total_amount AS mv_amt,
    b.order_cnt    AS base_cnt, m.order_cnt    AS mv_cnt
FROM base_agg b
FULL OUTER JOIN mv_agg m
  ON b.order_dt = m.order_dt AND b.store_id = m.store_id
WHERE b.order_dt IS NULL            -- MV에만 있는 행
   OR m.order_dt IS NULL            -- base에만 있는 행
   OR b.total_amount <> m.total_amount
   OR b.order_cnt    <> m.order_cnt;
-- 결과가 0행이면 정합. 1행이라도 나오면 배포 중단하고 원인 분석.

SET enable_materialized_view_rewrite = true;
```

체크 포인트:

- 비교 구간을 **최근 파티션 + 과거 파티션 양쪽에서 샘플링**한다 (증분 refresh가 과거 구간을 빠뜨리는 결함을 잡기 위해).
- 금액류가 FLOAT/DOUBLE이면 미세한 합산 차이가 날 수 있다. DECIMAL 기반으로 비교하거나 허용 오차를 정의한다.
- 검증 중 base에 적재가 들어오면 정상 상황에서도 차이가 난다. **정합성 실패 판정 전에 base 최신 커밋 시각과 MV last_refresh 시각을 먼저 대조**한다 (6.4).

### 6.2 Query rewrite 동작 검증 — 쿼리가 실제로 빨라지는가

```sql
-- (1) 대표 쿼리 3~5개를 정해 EXPLAIN으로 rewrite 성립을 확인한다
EXPLAIN SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01'
GROUP BY order_dt;
-- 기대: 스캔 노드의 TABLE: 에 mv_sales_daily_store_agg 가 표시됨

-- (2) rewrite가 성립하지 않으면 사유를 확인한다
TRACE REASON MV SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01'
GROUP BY order_dt;
```

```sql
-- (3) 성능 이득을 정량화한다: 같은 쿼리를 rewrite on/off로 각각 실행해 비교
SET enable_materialized_view_rewrite = false;
SELECT /* bench: mv_off */ order_dt, sum(amount) FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01' GROUP BY order_dt;

SET enable_materialized_view_rewrite = true;
SELECT /* bench: mv_on */ order_dt, sum(amount) FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01' GROUP BY order_dt;
-- audit log / query profile에서 두 실행의 지연시간·스캔 행수·스캔 바이트를 비교해 기록한다.
```

합격 기준(제안): 대표 쿼리 전부 rewrite 성립 + 지연시간/스캔량이 의미 있게 감소. rewrite가 안 되는 대표 쿼리가 있으면 MV 정의를 수정하고 재검증한다. **"MV는 갱신되는데 아무 쿼리도 rewrite되지 않는 상태"가 가장 흔한 낭비 유형이다.**

### 6.3 Refresh 동작 검증 — 갱신이 제때, 증분으로 도는가

```sql
-- (1) refresh 상태와 마지막 성공 시각
SHOW MATERIALIZED VIEWS FROM dw WHERE NAME = 'mv_sales_daily_store_agg';
-- 확인: is_active = true, 마지막 refresh 상태 SUCCESS, 시각이 주기와 맞는지

-- (2) 개별 실행 이력과 상세
SELECT *
FROM information_schema.task_runs
WHERE task_name LIKE '%mv_sales_daily_store_agg%'
ORDER BY create_time DESC
LIMIT 10;
-- 확인: state = SUCCESS 연속 여부, 소요 시간 추이,
--        EXTRA_MESSAGE의 갱신 대상 파티션 목록
```

**증분 동작 확인 시나리오**: 특정 일자 파티션에만 소량 데이터를 적재(또는 적재를 관찰)한 뒤 refresh를 트리거하고, `EXTRA_MESSAGE`에서 **해당 파티션만** 갱신 대상에 잡혔는지 확인한다. 매번 전체 파티션이 잡힌다면 파티션 매핑이 깨진 것이다 — MV의 `PARTITION BY`와 base 파티션 정의(transform 포함)를 재점검하고 플랫폼 팀에 문의한다.

`refresh_mode = incremental` MV의 추가 시나리오: (a) append만 있는 정상 상황에서 refresh 성공과 처리량 확인, (b) 컴팩션·DELETE 발생 직후 refresh가 **실패하는지**(설계된 동작), 실패 메시지와 복구 절차를 기록해 둔다.

### 6.4 신선도 검증 — 계약대로 최신인가

```sql
-- (1) base의 마지막 변경 시각 (Iceberg snapshot 이력)
SELECT * FROM iceberg_cat.sales."orders$snapshots"    -- 미지원 시 Spark 등에서 snapshot 이력 조회
ORDER BY committed_at DESC LIMIT 5;

-- (2) MV의 마지막 refresh 성공 시각
SHOW MATERIALIZED VIEWS FROM dw WHERE NAME = 'mv_sales_daily_store_agg';
```

`(base 마지막 커밋 시각) - (MV 마지막 refresh 시각)`이 계약(주기 + staleness)을 초과하는 상태가 반복되면 refresh 실패·지연을 의심한다 (6.3으로). 신선도 검증은 배포 후 **최소 2~3 refresh 주기 동안 관찰**한 뒤 완료로 판정한다.

### 6.5 장애·변경 시나리오 테스트 (배포 전 1회 수행 권장)

| 시나리오 | 방법 | 기대 동작 |
|---|---|---|
| base 스키마 변경 | (개발 환경에서) base 컬럼 변경 후 MV 상태 확인 | MV가 INACTIVE로 전환됨을 확인하고, `ALTER MATERIALIZED VIEW ... ACTIVE` 복구 절차를 문서화 |
| refresh 실패 | 타임아웃을 짧게 걸어 의도적 실패 유발 | task_runs에 FAILED 기록, 알림 채널로 통지되는지 확인 |
| stale 상태의 rewrite | base 적재 직후 (refresh 전) 쿼리 EXPLAIN | staleness 이내면 MV rewrite 유지, 초과면 base 스캔으로 전환되는지 확인 |
| 대량 backfill | 과거 파티션 다수를 재적재 후 refresh | 배치 분할(partition_refresh_number)로 안정적으로 처리되는지, 서빙 영향 확인 |
| TTL 경계 (3.4 유형만) | TTL 이전 구간을 포함한 쿼리 EXPLAIN | MV + base의 UNION 플랜 생성 확인 |

### 6.6 배포 전 체크리스트 (요약)

- [ ] 기존 MV 중복 확인 완료 (2장)
- [ ] 네이밍·COMMENT·staleness 계약 명시 (3.0)
- [ ] 정합성: 총량 + 행 단위 비교 0건 차이 (6.1)
- [ ] 대표 쿼리 rewrite 성립 + 성능 이득 정량 기록 (6.2)
- [ ] 증분 refresh 동작 확인 — 변경 파티션만 갱신 (6.3)
- [ ] 2~3 주기 신선도 관찰 완료 (6.4)
- [ ] INACTIVE·실패 대응 절차 확인 (6.5)
- [ ] 사용자 공지: 대상 테이블, 신선도 계약, 문의 채널

---

## 7. 주의사항과 흔한 실수

| 하지 말 것 | 이유 / 대신 할 것 |
|---|---|
| 확인 없이 비슷한 MV 신규 생성 | refresh 비용 이중 지출 + 옵티마이저 부하. 2장 절차로 기존 MV 먼저 확인 |
| 필터 컬럼을 SELECT에서 누락 | 그 컬럼으로 필터하는 쿼리가 rewrite 안 됨. 조회 패턴의 컬럼을 출력에 포함 |
| `current_date()` 등으로 조건 작성 | rewrite 불성립 가능. 날짜는 리터럴/파라미터로 바인딩 |
| 파티션 지정 없는 수동 `REFRESH ... FORCE` | 전체 파티션 재계산으로 클러스터에 부하 폭탄. `PARTITION START/END`로 범위 지정 |
| 신선도 확인 없이 "MV 값이 틀리다" 판단 | 대부분 refresh 지연이다. 6.4로 신선도 먼저 대조 후 6.1 정합성 검증 |
| rewrite 기대하면서 `refresh_mode=incremental` 선택 | 4.1 현재 rewrite 미참여. 가속 목적이면 기본(PCT) 유지 |
| base 스키마를 통보 없이 변경 | 그 위의 MV가 전부 INACTIVE로 빠진다. 변경 전 MV 오너/플랫폼 팀에 사전 공유 |
| MV 위에 무분별한 nested MV | 갱신 체인이 길어져 지연·비용 증가. 플랫폼 팀 협의 후 설계 |

**문의가 필요한 상황**: rewrite가 안 되는 이유를 TRACE로도 모르겠을 때, refresh가 반복 실패할 때, `incremental` 모드를 쓰고 싶을 때, MV 정의 변경/삭제가 필요할 때 → 플랫폼 팀 채널로 문의한다. *(채널명·담당은 팀 확정 후 기입)*

---

## 8. FAQ

**Q. MV를 쓰면 쿼리를 바꿔야 하나요?**
아니요. base 테이블을 그대로 조회하면 자동으로 적용됩니다. 유일한 예외는 `incremental` 모드 MV(직접 조회 전용)입니다.

**Q. 내 쿼리가 MV를 타는지 어떻게 확인하나요?**
`EXPLAIN`의 스캔 노드에 MV 이름이 보이면 적용된 것입니다. 안 보이면 `TRACE REASON MV`로 사유를 확인하세요 (4.3절).

**Q. MV 데이터가 실시간인가요?**
아니요. refresh 주기만큼 지연될 수 있습니다. 각 MV의 신선도 계약(주기 + staleness)은 COMMENT와 `SHOW MATERIALIZED VIEWS`로 확인합니다 (5장).

**Q. base와 값이 다릅니다.**
먼저 신선도를 대조하세요 (6.4). refresh 지연이 원인인 경우가 대부분입니다. 신선도가 정상인데 다르면 6.1 절차로 정합성을 검증하고 플랫폼 팀에 결과와 함께 문의하세요.

**Q. MV를 직접 조회해도 되나요?**
됩니다만, 일반 MV는 base 조회 + 자동 rewrite를 권장합니다. MV 정의가 바뀌어도 쿼리가 깨지지 않습니다.

**Q. MV 생성 권한은 어떻게 받나요? / 만든 MV는 언제 정리되나요?**
생성은 대상 테이블 SELECT 권한 + 플랫폼 팀 승인 절차를 따릅니다. 일정 기간 rewrite hit가 없는 MV는 오너 통보 후 정리 대상이 됩니다. *(승인 절차·정리 기준은 팀 확정 후 기입)*

---

*작성: 2026-07. StarRocks 4.1.1 기준. 내부 동작·설계 원리는 「StarRocks MV 엔지니어링 심화 가이드」 참고. 예시 테이블명은 실제 환경에 맞게 치환할 것. "(팀 확정 후 기입)" 표기 항목은 플랫폼 정책 확정 시 갱신한다.*





Spark 심화 — 1부: 배치 실행 모델
■ 1. 실행 계층 (질문 오면 이 순서로)
사용자 코드 → 논리 플랜 → 최적화 논리 플랜 → 물리 플랜 → RDD DAG → Stage → Task

Catalyst 4단계: Analysis(스키마·컬럼 해석) → Logical Optimization(predicate pushdown, projection pruning, constant folding) → Physical Planning(조인 전략 선택 등 후보 생성 후 비용 비교) → Code Generation(Tungsten이 JVM 바이트코드 생성)
Logical Optimization : 읽는 양과 계산하는 양을 줄이는 과정


Whole-Stage CodeGen: 연산자 체인을 하나의 함수로 합쳐 가상 함수 호출·중간 객체 생성 제거


UDF가 최적화 장벽인 이유: Catalyst가 내부를 못 봐서 pushdown·codegen이 끊김. PySpark UDF는 더 나쁨 — JVM↔Python 직렬화 왕복 발생 → Pandas UDF(Arrow 기반)로 완화


■ 2. Job/Stage/Task 경계
Action 하나 = Job 하나 (count, write, collect)


Shuffle = Stage 경계. Stage는 앞이 전부 끝나야 다음 시작 (배리어)


Task = 파티션 하나 처리 단위. Stage의 태스크 수 = 그 스테이지 파티션 수


앵커: “느린 태스크 하나가 스테이지 전체를 붙잡는다”


■ 3. AQE (Spark 3+, 실무에서 제일 자주 언급)
런타임 통계로 플랜을 다시 짜는 기능. 세 가지:
파티션 병합(coalesce shuffle partitions): shuffle 후 작은 파티션들을 합쳐 태스크 수 최적화 → spark.sql.shuffle.partitions를 크게 잡아도 안전해짐


Skew Join 처리: 큰 파티션을 감지해 자동 분할


조인 전략 전환: 실제 크기를 보고 Sort-Merge → Broadcast로 변경


한계: 파티션 경계에서만 개입 — 소스 읽기 단계의 스큐나 극단적 편중은 못 잡음 → Salting


■ 4. 조인 전략 정리
전략
조건
비용
위험
Broadcast Hash
한쪽 < 임계(10MB 기본)
shuffle 없음, 최선
Driver OOM
Sort-Merge
대용량×대용량
shuffle + 정렬
skew 취약
Shuffle Hash
한쪽이 중간 크기
정렬 없음
executor 메모리
Bucketed Join
양쪽 같은 키로 버킷팅 저장
shuffle 생략
사전 설계 필요

■ 5. 메모리 모델
Executor JVM = Reserved(300MB) + Unified(Execution ⟷ Storage) + User
+ (JVM 밖) memoryOverhead: off-heap, Python 프로세스, 네이티브

Execution ⟷ Storage 동적 경계: 캐시가 실행 메모리를 뺏길 수 있음(evict), 반대는 제한적


Spill: 부족하면 디스크로 — 죽진 않지만 급격히 느려짐. Spark UI의 Spill 지표가 튜닝 신호


OOM 두 종류 구분 (핵심): JVM heap 부족(스택트레이스 남음) vs 컨테이너 OOMKilled(exit 137, 로그 없음) — 후자는 overhead 부족


■ 6. 파티션 관리
repartition(n) = 셔플 O, 균등 / coalesce(n) = 셔플 X, 불균등 가능(줄일 때만)


repartition(col) = 키 기준 재분배 (쓰기 전 파일 수 제어에 사용)


읽기 파티션 수 = 입력 파일·블록 수에 좌우 → small file이 많으면 태스크 폭발


쓰기 파티션 수 = 출력 파일 수 (Iceberg 적재 시 파일 크기 결정 — 내 Mini-Batch 경험)


■ 7. 진단 순서 (암기)
Stage 소요시간 → Task duration 분포(median vs max) → Shuffle read/write 크기 → Spill → GC time → Input rows

Spark 심화 — 2부: Structured Streaming
■ 1. 핵심 추상화: “무한히 증가하는 테이블”
스트림을 끝없이 행이 추가되는 테이블로 보고, 쿼리를 그 테이블에 반복 실행. 그래서 배치와 같은 DataFrame API를 씀 — 이게 Spark 스트리밍의 최대 강점(배치·스트리밍 로직 통합)
■ 2. 실행 모델 — Micro-Batch
Trigger 발동 → 소스에서 처리할 offset 범위 결정 → WAL에 기록
→ 배치 실행 → 싱크에 쓰기 → 커밋 로그 기록 → 반복

Trigger 종류 (실무 선택 지점):
ProcessingTime("5 minutes") — 주기 실행


Trigger.Once / Trigger.AvailableNow (권장) — 있는 데이터 다 처리하고 종료. 스트리밍 코드로 배치처럼 운영 — 내 Mini-Batch + CronJob 구조와 사실상 같은 목적


Trigger.Continuous — 실험적, 밀리초 지연 (실무 거의 미사용)


핵심: 트리거 간격 = 파일 생성 빈도 = 저장 레이아웃 (내 30배 개선의 본질)


■ 3. 체크포인트 구조 (장애 복구의 실체)
checkpointLocation/
├── offsets/     ← 배치별 처리 예정 offset (WAL, 실행 전 기록)
├── commits/     ← 완료된 배치 기록
├── state/       ← 상태 저장소 스냅샷
└── metadata/    ← 쿼리 ID

복구 로직: offsets에는 있는데 commits에 없는 배치 = 미완료 → 재처리


결과적 exactly-once 조건: 소스가 재생 가능(Kafka) + 싱크가 멱등. 둘 중 하나라도 없으면 안 됨


주의: 체크포인트는 쿼리와 강결합 — 로직을 크게 바꾸면 호환 안 될 수 있음. 삭제하면 처음부터 재처리


■ 4. State Store (상태 기반 연산)
집계·조인·dedup·mapGroupsWithState는 상태를 유지 — 기본 HDFSBackedStateStore(메모리+체크포인트), RocksDB 백엔드 권장(대용량 상태 시 GC 압박 회피)


상태 무한 증가가 최대 리스크 → Watermark로 정리


■ 5. Watermark — 정확성과 지연의 trade-off
.withWatermark("event_ts", "10 minutes")

의미: “현재까지 본 최대 event_ts - 10분”보다 오래된 데이터는 늦은 것으로 간주하고 버림


두 가지 역할: ① late data 처리 기준 ② 상태·윈도우 정리 기준(이게 없으면 상태 무한 증가)


차량 도메인 핵심 판단: 단절 후 재연결로 late data가 상시 → watermark를 짧게 잡으면 데이터 손실, 길게 잡으면 상태 증가 + 결과 지연. “얼마나 늦은 데이터까지 정확성에 포함할 것인가”의 비즈니스 판단


■ 6. Output Mode
Append — 확정된 행만 추가 (watermark 필요한 집계에서). 파일 싱크는 이것만 지원


Update — 변경된 행만 출력 (집계 중간 결과 갱신)


Complete — 전체 결과 테이블 재출력 (상태 무한 증가, 소규모만)


■ 7. Kafka 소스 실무 옵션
startingOffsets: latest / earliest / 특정 offset JSON


maxOffsetsPerTrigger — 배치당 최대 레코드 수. backpressure 제어 + 첫 실행 시 폭주 방지 (재시작 후 lag이 클 때 필수)


failOnDataLoss — retention으로 offset 소실 시 실패 여부


Kafka 파티션 = Spark 태스크 1:1 매핑 (기본)


■ 8. foreachBatch — 실무의 만능 열쇠
def upsert(batch_df, batch_id):
    batch_df.createOrReplaceTempView("s")
    spark.sql("MERGE INTO target t USING s ON ... ")

query.writeStream.foreachBatch(upsert).start()

각 마이크로배치를 일반 DataFrame처럼 다룸 → MERGE INTO, 멀티 싱크 쓰기, 배치 API 재사용이 가능해짐


Iceberg/Delta upsert의 표준 경로 — 스트리밍 API가 직접 지원 안 하는 연산을 여기서 처리


주의: batch_id로 멱등성 직접 보장해야 함 (재시도 시 같은 batch_id 재실행 가능)


■ 9. 배치 vs 스트리밍 선택 논리 (내 서사)
“실시간이 가능해서가 아니라 필요해서 선택한다. 목적이 분석·ML 기반 저장이면 분 단위 지연은 허용 가능하고, 그 대가로 파일 효율과 운영 단순성을 얻는다. 우리는 상시 구동 스트리밍 대신 CronJob 배치를 택했는데, 요구 지연이 분 단위였고 리소스·운영 면에서 단순했기 때문이다. 지금이라면 Trigger.AvailableNow로 스트리밍 코드를 배치처럼 운영하는 방식도 고려할 것 — offset 관리를 체크포인트에 맡기면서 주기 실행의 장점을 얻으니까.”

Spark Connect — 현재 회사 이야기용
■ 1. 문제: 기존 Spark의 강결합
전통 구조에서 Driver가 곧 애플리케이션 — 사용자 코드와 Spark 실행 엔진이 같은 JVM에 존재


그래서: 클라이언트가 무거움(전체 Spark 의존성 필요), 버전 강결합(클라이언트 Spark 버전 = 클러스터 버전), 멀티테넌시 어려움, 사용자 코드가 Driver를 죽이면 전체 실패, IDE/노트북에서 원격 개발 불편


■ 2. Spark Connect의 구조 (3.4+)
클라이언트 (경량 라이브러리)
  ↓ 미해결 논리 플랜을 Protobuf로 직렬화
  ↓ gRPC 전송
Spark Connect Server (Driver 내)
  ↓ 플랜 해석 → Catalyst 최적화 → 실행
  ↓ 결과를 Arrow 포맷으로 스트리밍 반환
클라이언트

핵심: DataFrame API가 “실행”이 아니라 “플랜 생성”이 됨. 클라이언트는 플랜만 만들어 보내고, 실행은 서버가


전송 포맷: Protobuf(플랜) + Arrow(결과) — 둘 다 언어 중립적이라 클라이언트를 여러 언어로 만들 수 있음


■ 3. 얻는 것
클라이언트 경량화 — JVM·Spark 전체 없이 얇은 라이브러리만. 노트북·IDE·앱에서 직접 연결


버전 분리 — 클러스터를 업그레이드해도 클라이언트 코드 유지 (안정적 API 계약)


멀티테넌시 — 하나의 서버가 여러 세션을 격리 처리. 사용자별로 Spark 클러스터를 띄우지 않아도 됨


안정성 — 사용자 코드가 Driver JVM 밖에 있어 크래시 격리


언어 확장 — Go, Rust 등 클라이언트 등장


■ 4. 제약 (정직하게 말할 부분)
RDD API 미지원 — DataFrame/SQL 중심 (대부분 문제 없음)


SparkContext 직접 접근, 일부 내부 API 불가


커스텀 UDF는 클라이언트↔서버 간 코드 배포 이슈가 있음


성숙도가 계속 올라가는 중 — 버전별 지원 범위 확인 필요


■ 5. “런타임 제공자” 서사로 착지 (면접 발화)
“현재 회사에서는 사용자에게 Spark를 Spark Connect 방식으로 제공하고 있습니다. 기존 방식은 Driver가 곧 애플리케이션이라 사용자마다 클러스터를 띄우거나 무거운 클라이언트를 배포해야 하는데, Connect는 클라이언트가 논리 플랜만 gRPC로 보내고 실행은 서버가 담당하는 구조여서 — 클라이언트가 가벼워지고, 클러스터 버전과 분리되고, 하나의 서버가 여러 세션을 격리 처리할 수 있습니다. 플랫폼 관점에서 이게 중요한 이유는 런타임을 제품처럼 제공할 수 있다는 점이라고 봅니다. 사용자는 접속만 하면 되고, 버전 업그레이드나 리소스 배분 같은 복잡성은 플랫폼이 흡수하니까요. 제가 지금 다루는 문제가 정확히 그 지점 — Runtime 영역에서 사용자별 격리와 리소스 배분을 어떻게 설계할 것인가입니다. StarRocks에서 Resource Group으로 워크로드를 격리하는 것과 같은 고민이 Spark 쪽에도 있는 거고요.”
앵커 3개:
“DataFrame이 실행이 아니라 플랜 생성이 된다” (Connect의 본질)


“클라이언트-클러스터 버전 분리 = 플랫폼이 업그레이드 자유를 얻는다”


“런타임을 제품으로 제공한다” (제공자 정체성으로 착지)


Kafka → Iceberg Spark 적재 — 실무 논점 정리

■ 1. 읽기 단계 (Kafka)
① Offset 관리 — 배치 방식의 핵심 결정
스트리밍이면 체크포인트가 자동 관리하지만, 배치 CronJob은 offset을 스스로 관리해야 함


선택지: 
컨슈머 그룹 offset 활용
외부 저장소에 기록
startingOffsets~endingOffsets 범위 명시


중요: 배치 시작 시점에 읽을 범위를 고정해야 함 — 안 그러면 실행 중 계속 유입되는 데이터로 배치 경계가 모호해짐


② 배치 크기 제어
Kafka 파티션 = Spark 태스크 1:1 → 파티션 수가 병렬성 상한


5분치가 얼마나 되는지 예측하고, 급증 시(재시작 후 lag 큼) 한 배치가 감당 못 할 양이 들어오는 걸 방지 — maxOffsetsPerTrigger 또는 offset 범위 제한


내 케이스: 평균 50만 건/5분, burst 시 그 이상 → 첫 실행이나 장애 복구 시 폭주 방지가 실무 포인트


③ 역직렬화
Avro + Schema Registry면 스키마 조회 후 디코딩. 스키마 캐싱으로 매 레코드 조회 방지


실패 레코드 처리: 전체 실패 vs DLQ 격리 — 정상은 흘리고 불량만 분리



■ 2. 변환 단계 (여기가 CDC 특유)
① Dedup이 필수 (MERGE 전제 조건)
ROW_NUMBER() OVER (PARTITION BY pk ORDER BY event_ts DESC, ord DESC) = 1

MERGE INTO는 source 키당 1건을 전제 → 위반 시 런타임 에러


정렬 기준의 함정: timestamp만으로는 같은 밀리초 내 순서 불명 → Debezium ord 값을 밀리초 뒤에 붙여 보강 (내 실사례)


이 dedup 자체가 shuffle을 유발하는 무거운 연산 — 파티션 수 관리 필요


② 타입·스키마 정합 (내 실제 트러블슈팅)
epoch Long → Timestamp 명시 캐스팅 (밀리초 정밀도 유지)


Nested StructType의 nullable 속성을 기존 스키마에 정렬


“Schema Evolution은 메타데이터의 일, 이건 데이터의 일” — 쓰기 경로에서 해결


③ op 타입별 분기
c/u는 upsert, d는 delete 또는 tombstone → 삭제를 물리 삭제할지 soft delete할지가 다운스트림 계약



■ 3. 쓰기 단계 (가장 중요)
① MERGE INTO 최적화
ON 조건에 파티션 컬럼 포함 → 프루닝 유도, 없으면 매 배치가 테이블 전체 스캔


Late data 방어: WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE — 늦게 온 오래된 이벤트가 최신을 덮지 않게


CoW vs MoR 선택 → 변경 빈도 높으면 MoR + compaction


② 파일 레이아웃 제어 (앞서 정리한 “충분조건”)
쓰기 전 repartition으로 태스크 수 조정 → 파일 수 결정


write.target-file-size-bytes 설정


파티션 걸친 배치면 파일 수 = 파티션 수 × 태스크 수로 곱셈 → 파티션 컬럼 기준 repartition 고려


③ 멱등성
MERGE 기반이라 재실행해도 결과 동일 (자연키 upsert)


배치 단위 재시도 시 같은 offset 범위를 다시 읽어도 안전



■ 4. 운영 단계
커밋 충돌: Compaction과 겹치면 OCC 재시도 → 실행 시각 오프셋 분리 (01~03분 / 31~33분)


유지보수 3종: Compaction 30분, Rewrite Manifest 1시간, Expire Snapshots 2주


모니터링: Consumer Lag 추세, 배치 소요시간, 커밋당 파일 수·크기, Compaction 성공률


실패 처리: CronJob 실패 시 다음 주기가 밀린 offset부터 처리 — 자동 catch-up 구조



■ 5. 왜 Structured Streaming이 아니라 CronJob 배치였나 (논리 정리)
핵심 프레임: “요구가 정한 것이지 기술이 정한 게 아니다”
① 요구 지연이 분 단위였다
Lakehouse의 목적이 분석·ML 기반 저장이지 실시간 서빙이 아님


실시간이 필요한 소비처는 Public Topic을 직접 구독하는 별도 경로가 이미 있었음


→ 5분 지연이 허용되는데 상시 구동 스트리밍을 유지할 이유가 없음


② 상시 구동의 운영 비용을 피할 수 있었다
Structured Streaming은 드라이버가 계속 살아 있어야 함 → 리소스 상시 점유, 장애 시 재시작 관리, 체크포인트 상태 관리


CronJob은 실행 시점에만 리소스 사용 → 배치가 순간 집중형이라 다른 워크로드와의 경합 창이 짧음


실패 시 복구도 단순: 다음 주기가 자동으로 밀린 분량을 처리


③ 실행 주기를 유지보수 주기와 함께 설계할 수 있었다
Compaction 30분 주기와 충돌하지 않도록 실행 시각을 명시적으로 배치 — 상시 스트리밍이면 언제든 커밋이 발생해 OCC 충돌 제어가 어려움


**“커밋 시점을 통제할 수 있다”**는 게 배치의 숨은 장점


④ 파일 생성 빈도를 결정론적으로 제어할 수 있었다
이게 애초의 문제(Small File)를 푸는 방법이었음 — 5분에 정확히 한 번 커밋


스트리밍 트리거로도 가능하지만, “주기 실행”이라는 성격을 명시적으로 드러내는 구조가 팀 이해와 운영에 명확했음


⑤ 팀 스택과의 정합
이미 EKS CronJob으로 다른 배치들이 돌고 있었고, Kyuubi로 Spark 리소스를 관리하는 체계가 있었음 → 새로운 운영 패턴을 도입하지 않는 선택


정직한 균형 (반드시 붙일 것):
“다만 offset 관리를 직접 해야 한다는 게 배치의 대가입니다. 지금이라면 Trigger.AvailableNow를 검토할 것 같습니다 — 스트리밍 API로 offset 관리를 체크포인트에 맡기면서도, 있는 데이터를 처리하고 종료하는 배치처럼 운영할 수 있으니 두 방식의 장점을 합칠 수 있거든요.”

■ 6. 발화 압축 (1분 30초)
“Kafka에서 읽어 Iceberg에 적재하는 파이프라인인데, 단계별로 신경 쓴 지점이 다릅니다. 읽기에서는 배치 경계를 고정하는 게 중요했습니다. 배치 방식이라 offset을 직접 관리해야 하고, 재시작 후 lag이 클 때 한 배치가 감당 못 할 양이 들어오는 걸 막아야 하니까요. 변환에서는 dedup이 핵심이었습니다. MERGE INTO는 source 키당 1건을 전제하는데 CDC는 한 배치에 같은 PK가 여러 번 들어오는 게 정상이라, ROW_NUMBER로 최신 1건만 남깁니다. 이때 timestamp만으로는 같은 밀리초 내 순서가 안 잡혀서 Debezium의 ord 값을 밀리초 뒤에 붙여 정렬 기준을 보강했습니다. 그리고 Java 스트리밍에서 PySpark로 전환하면서 epoch Long과 Timestamp 표현 차이, nested nullable 불일치가 있었는데, Iceberg Schema Evolution으로는 풀 수 없는 문제라 — 값의 해석 규칙이 필요한 데이터 변환이니까요 — DataFrame 단계에서 명시적 캐스팅으로 해결했습니다. 쓰기가 가장 중요했습니다. MERGE의 ON 조건에 파티션 컬럼을 넣어 스캔 범위를 좁히고, late data가 최신을 덮지 않도록 event_ts 비교 조건을 걸고, 쓰기 전 repartition으로 파일 수를 제어했습니다. 트리거 간격이 필요조건이라면 쓰기 레이아웃 제어가 충분조건이라고 보는데, 5분으로 늘려도 태스크가 수백 개면 여전히 작은 파일이 수백 개 생기니까요. 운영에서는 Compaction과 커밋이 겹치면 Iceberg OCC 충돌로 재시도가 잦아져서, 실행 시각을 오프셋으로 분리했습니다. Structured Streaming 대신 CronJob 배치를 택한 건 요구 지연이 분 단위였기 때문입니다. Lakehouse는 분석·ML 목적이고, 실시간이 필요한 소비처는 Public Topic을 직접 구독하는 경로가 따로 있었거든요. 배치는 상시 드라이버를 유지할 필요가 없고, 실패해도 다음 주기가 밀린 분량을 자동으로 따라잡고, 무엇보다 커밋 시점을 통제할 수 있어 Compaction 주기와 함께 설계할 수 있었습니다. 다만 offset을 직접 관리해야 하는 대가가 있어서, 지금이라면 Trigger.AvailableNow로 스트리밍 API의 체크포인트 관리를 쓰면서 배치처럼 운영하는 방식도 검토할 것 같습니다.”
앵커 4개:
“트리거 간격이 필요조건, 쓰기 레이아웃이 충분조건”


“MERGE는 키당 1건이 전제 — dedup과 정렬 기준 보강”


“배치의 숨은 장점은 커밋 시점을 통제할 수 있다는 것”


“실시간이 가능해서가 아니라 필요해서 — 실시간 소비처는 별도 경로가 있었다”


Kafka → Iceberg Spark 배치 적재 — 최종 면접 스크립트

■ 0. 배경 (30초 요약)
CDC 이벤트를 Kafka에서 읽어 Iceberg에 적재하는 파이프라인


문제: Kafka Connect 실시간 스트리밍 적재 → 분당 수십만 건을 10초 내외로 commit → 커밋마다 수십 MB 파일 양산 → 파일 생성 속도가 Compaction 병합 속도를 앞지름 → Compaction 실패


진단: 근본 원인은 파일 크기가 아니라 커밋 빈도


해결: PySpark 기반 5분 Mini-Batch 전환 → 커밋 빈도 약 30배 감소, EKS CronJob + Kyuubi 운영



■ 1. 읽기 — Kafka
① Offset 관리 (배치 방식의 대가)
스트리밍은 체크포인트가 자동 관리하지만 배치는 직접 관리 → S3의 별도 경로에 offset 저장


배치 시작 시 이전 offset을 읽고, 완료 후 갱신


② 읽기 범위와 병렬성
endingOffsets = latest — 5분치를 전부 읽는 방식


Kafka 파티션 = Spark 태스크 (파티션 내에서만 순서가 보장되므로 자연스러운 병렬 단위)


파티션 수가 병렬성 상한 → minPartitions로 offset 범위를 분할해 태스크 증설 (뒤에서 dedup하는 구조라 파티션 내 순서 의존 없음)


③ latest 방식의 리스크 인식 (회고 — 물으면 꺼낼 것)
당시엔 문제없었던 이유: 5분 주기라 배치당 부하가 예측 가능, CDC라 유입량 자체에 상한


안전장치: MERGE 기반 멱등 적재 → 실패해도 다음 주기가 같은 구간 재처리 → 실패가 손실이 아니라 지연으로만 나타남


지금이라면: offset 상한을 둘 것. 첫 적재나 장애로 밀린 상황에서 한 배치가 감당 못 할 양이 들어오면 배치 시간 > 주기 → lag 증가 → 배치 더 커짐의 악순환. Small File 문제와 같은 패턴 — 유입 속도가 처리 속도를 앞지르는 임계



■ 2. 변환
① Dedup — MERGE의 전제 조건
MERGE INTO는 source 키당 1건을 전제 → CDC는 한 배치에 같은 PK 다건이 정상 → 위반 시 런타임 에러


ROW_NUMBER() OVER (PARTITION BY pk ORDER BY ...) = 1


정렬 기준 보강: timestamp만으로는 같은 밀리초 내 순서 불명 → Debezium의 ord 값을 밀리초 뒤에 추가


② 스키마 정합 (Java Streaming → PySpark 전환 시)
epoch(Long) → Timestamp 명시 캐스팅 (밀리초 정밀도 유지), Nested StructType nullable 정렬


핵심 논리: Iceberg의 타입 승격은 재작성 없이 재해석 가능한 확장(int→long 등)만 허용. epoch→Timestamp는 값의 해석 규칙이 필요한 변환 → “Schema Evolution은 메타데이터의 일, 이건 데이터의 일” → 쓰기 경로에서 해결


결과: 기존 데이터 재적재 없이 신규 파이프라인이 기존 적재분과 정합



■ 3. 쓰기 — MERGE INTO + 파일 레이아웃
① MERGE 최적화
ON 조건에 파티션 컬럼 포함 → 프루닝 유도 (없으면 매 배치가 테이블 전체 스캔)


Late data 방어: WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE


MERGE는 read-modify-write → 호출 빈도를 줄이는 것 자체가 최적화 (Mini-Batch의 본질)


② 파일 수 제어 — 3단계 서사 (핵심)
1단계: repartition으로 태스크 수를 줄여 파일 수 감소
Compaction 실패라는 급한 상황 → 즉시 검증 가능, 커밋당 결과가 결정론적


2단계: 두 제약이 동시에 작동하고 있었음
Iceberg write.target-file-size-bytes는 기본값 512MB로 이미 작동 중


실제 파일 수 = max(repartition N, 데이터양 ÷ target 크기)


평상시엔 repartition이, burst 땐 target이 롤링해 거대 파일 방지


3단계: 운영 중 인식한 비대칭
target은 큰 파일을 쪼갤 뿐, 작은 파일을 합치지는 못함


유입이 적은 시간대엔 태스크당 데이터가 적어 작은 파일 재발 → 원인은 repartition으로 태스크 수를 고정한 것 자체


개선 방향: 파일 수를 직접 정하지 말고, distribution-mode = hash로 같은 파티션 데이터를 한 태스크에 모으고 크기 기준은 target에 위임


앵커: “개수를 고정하면 크기가 흔들리고, 크기를 고정하면 개수가 조절된다”


③ 결과 수치
전: 10초 커밋 / 커밋당 수십 MB


후: 5분 배치 / 커밋당 수백 MB / 커밋 빈도 30배 감소


보너스: 대형 파일일수록 압축 효율↑ → “Small File은 파일 수도 많고 압축도 나쁜 이중 낭비”



■ 4. 운영
OCC 충돌 회피: Compaction(30분)과 배치(5분)가 겹치면 재시도 빈발 → 실행 시각 오프셋 분리(01~03분 / 31~33분) + 겹쳐도 OCC 재시도가 안전망


유지보수 3종: Compaction 30분 / Rewrite Manifest 1시간 / Expire Snapshots 2주 — 데이터 파일뿐 아니라 메타데이터·매니페스트까지 관리 대상


모니터링: Consumer Lag 추세(값 아님), 배치 소요시간(주기 대비 비율), 커밋당 파일 수·크기, Compaction 성공률, lag/retention 비율(복구 데드라인)



■ 5. 왜 Structured Streaming이 아니라 CronJob 배치였나
요구 지연이 분 단위 — Lakehouse는 분석·ML 목적. 실시간 소비처는 Public Topic 직접 구독 경로가 별도 존재


상시 드라이버 불필요 — 실행 시점만 리소스 점유, 실패 시 다음 주기가 자동 catch-up


커밋 시점 통제 가능 — Compaction 주기와 함께 설계 (배치의 숨은 장점)


팀 스택 정합 — 이미 EKS CronJob + Kyuubi 체계 존재


균형: “대가는 offset 직접 관리. 지금이라면 Trigger.AvailableNow를 검토 — 체크포인트에 offset을 맡기면서 배치처럼 주기 실행”



■ 압축 발화 (1분 30초 버전)
“실시간 스트리밍 적재에서 Compaction이 실패하고 있었는데, 원인을 파일 크기가 아니라 커밋 빈도로 진단하고 PySpark 5분 Mini-Batch로 전환한 작업입니다. 읽기는 배치 방식이라 offset을 직접 관리해야 해서 S3에 저장했고, Kafka 파티션이 곧 Spark 태스크라 병렬성이 파티션 수에 묶이는 걸 minPartitions로 보완했습니다. 다만 endingOffsets를 latest로 뒀는데, 지금 돌아보면 상한을 두는 게 맞았다고 봅니다 — 장애로 밀리면 배치가 커지고 주기를 넘겨 lag이 더 쌓이는 악순환이 가능하니까요. 당시엔 MERGE 기반 멱등 적재라 실패가 손실이 아니라 지연으로만 나타나는 게 안전장치였습니다. 변환에서는 dedup이 핵심이었습니다. MERGE는 source 키당 1건을 전제하는데 CDC는 같은 PK가 여러 번 들어오는 게 정상이라, ROW_NUMBER로 최신 1건만 남기되 timestamp만으로는 같은 밀리초 내 순서가 안 잡혀 Debezium의 ord 값을 정렬 기준에 추가했습니다. 그리고 Java에서 PySpark로 전환하며 epoch Long과 Timestamp 표현 차이가 있었는데, Schema Evolution으로는 풀 수 없는 문제 — 값의 해석 규칙이 필요한 데이터 변환이라 — DataFrame 단계 명시적 캐스팅으로 해결해 기존 데이터 재적재 없이 전환했습니다. 쓰기가 가장 배운 게 많았습니다. 처음엔 repartition으로 태스크 수를 줄여 파일 수를 감소시켰고 급한 문제는 해결됐습니다. Iceberg의 target-file-size는 기본값 512MB로 이미 작동 중이어서, 실제로는 파일 수가 repartition과 target 중 더 강한 제약으로 결정되는 구조였습니다 — 평상시엔 repartition이, 유입이 튀면 target이 롤링해 거대 파일을 막는 식으로요. 그런데 반대 방향엔 비대칭이 있어서, target은 큰 파일을 쪼갤 뿐 작은 파일을 합치지는 못합니다. 유입이 적은 시간대엔 여전히 작은 파일이 생겼고, 원인은 repartition으로 태스크 수를 고정한 것 자체였습니다. 그래서 파일 수를 직접 정하는 대신 distribution-mode를 hash로 두고 크기 기준은 target에 맡기는 방향이 맞다고 정리했습니다. 운영에서는 Compaction과 커밋이 겹치면 OCC 충돌로 재시도가 잦아져 실행 시각을 오프셋으로 분리했고, 데이터 파일뿐 아니라 매니페스트와 스냅샷까지 각자 주기로 관리했습니다.”

■ 앵커 6개 (암기)
“원인은 파일 크기가 아니라 커밋 빈도”


“MERGE는 키당 1건이 전제 — dedup + ord로 정렬 보강”


“Schema Evolution은 메타데이터의 일, 이건 데이터의 일”


“파일 수 = max(repartition, 데이터양 ÷ target)”


“target은 큰 파일을 쪼갤 뿐 작은 파일을 합치지 못한다”


“실패가 손실이 아니라 지연으로만 나타나는 구조” (멱등성)


ClickHouse 기반 대량 로그 배치 시스템
■ 차량 이벤트 로그로 보면 달라지는 것들
① 규모의 차원이 다름
시스템 로그: 서비스 수 × 요청량
차량 이벤트: 차량 대수 × 신호 종류 × 발생 빈도 — 양산 확대되면 기하급수적. "고volume 시계열"이라는 ClickHouse의 전제 조건에 정면으로 부합
② 데이터의 성격이 '자산'
시스템 로그는 며칠 뒤 버려도 되지만, 차량 이벤트는 ML 학습 데이터·품질 이슈 추적·규제 대응에 쓰이는 원본 자산
그래서 Lakehouse(장기 원본) + ClickHouse(최근 구간 서빙)의 이중 구조가 필요한 거고 — 앞서 정리한 "왜 둘 다 필요한가"가 여기서 실질적 근거를 얻어
③ 스키마 설계가 훨씬 어려워짐
 시스템 로그는 (service, level, message)로 대충 수렴하는데, 차량 이벤트는:
이벤트 타입이 수백~수천 종 (센서 신호, 에러코드, 트립 이벤트, 사용자 조작)
차종·SW 버전마다 정의가 다름 (OTA)
→ 정렬 키 설계: (vehicle_id, ts) vs (event_type, ts) vs (vehicle_model, vehicle_id, ts) — 지배적 쿼리가 뭐냐에 따라 갈리고, 하나로 안 되면 Projection이나 별도 테이블로 이중화
→ 필드 저장: 공통 필드(vehicle_id, ts, event_type, sw_version)는 컬럼, 이벤트별 payload는 Map/JSON — "raw는 유연하게, 자주 쓰는 건 구조화"의 실전 적용
④ 품질 검증이 진짜 문제가 됨 (JD 항목의 실체)
 시스템 로그엔 없는 요구들:
물리적 타당성 — 속도 300km/h, 좌표가 바다 한가운데, 배터리 음수값
디바이스 커버리지 — 평소 보고하던 차량이 사라짐 (전체 볼륨은 정상인데 특정 그룹만 누락)
시간 정합성 — 단말 시계 오차, 미래 타임스탬프, late data
중복 — 네트워크 단절 후 재전송
⑤ 이상 탐지의 대상이 인프라가 아니라 도메인
시스템 로그: "에러율이 올랐다"
차량 이벤트: "특정 차종의 특정 SW 버전에서 이 에러코드가 급증" → 품질 이슈의 조기 발견. 이게 JD의 "인사이트 도출 → 서비스 반영" 루프의 실체일 가능성이 높아
■ 그러면 ClickHouse의 역할이 이렇게 정리돼
계층
역할
Kafka
수집 버퍼 (재전송·스파이크 흡수)
ClickHouse
최근 구간 이벤트의 탐색·집계 엔진 — 대시보드, 이상 탐지 쿼리, 품질 모니터링, 엔지니어의 ad-hoc 조사
Lakehouse
전체 이력 원본, ML 학습 데이터 생성, 대규모 배치 가공
Spark
그 가공의 실행 엔진

■ 면접 발화 (이 관점으로 말하면)
"말씀하신 로그가 시스템 로그만이 아니라 차량에서 발생하는 이벤트 로그까지 포함하는 거라면, JD 항목들이 한 줄기로 이해됩니다. 품질 검증이나 카탈로그 운영은 단순 시스템 로그에는 과한 요구인데, 차량 이벤트는 ML 학습과 품질 추적에 쓰이는 자산이니까요.
 그러면 ClickHouse의 역할이 대시보드 서빙을 넘어 최근 구간 이벤트의 탐색·집계 엔진이 됩니다. 특정 차종의 특정 SW 버전에서 어떤 이벤트가 급증했는지를 초 단위로 파고들 수 있어야 품질 이슈를 조기에 발견할 수 있으니까요. 그리고 전체 이력 원본과 ML 학습 데이터 생성은 Lakehouse와 Spark가 담당하는 이중 구조가 자연스럽고요.
 설계에서 어려운 지점은 스키마일 것 같습니다. 이벤트 타입이 수백 종이고 차종·SW 버전마다 정의가 다를 텐데, 공통 필드는 정규 컬럼으로 두고 이벤트별 payload는 Map이나 JSON으로 받아 스키마 진화 부담을 줄이는 방향, 그리고 정렬 키를 지배적 쿼리 패턴 — 차량 단위 조회냐 이벤트 타입 단위 조회냐 — 에 맞추는 판단이 핵심일 것 같습니다."
앵커: "시스템 로그는 버려도 되는 흔적, 차량 이벤트는 보존해야 할 자산 — 그래서 품질·카탈로그·이중 저장이 필요해진다."

ClickHouse의 Index
■ 1. Secondary Index — 정확히는 "Data Skipping Index"
용어부터 정확히 하면, ClickHouse의 보조 인덱스는 RDBMS의 인덱스와 성격이 달라. 행을 직접 가리키지 않고, "이 granule 블록은 읽을 필요 없다"를 판정하는 용도야. 그래서 이름도 data skipping index.
종류와 용도:
타입
원리
적합한 컬럼
minmax
블록별 최소·최대값 저장
값이 정렬 키와 상관관계 있는 것 (예: 수집 시각)
set(N)
블록별 고유값 집합(최대 N개)
저카디널리티 — event_type, sw_version, vehicle_model
bloom_filter
확률적 존재 여부
고카디널리티 동등 검색 — trace_id, session_id
ngrambf / tokenbf
문자열 n-gram/토큰 블룸필터
로그 메시지 부분 문자열 검색

차량 이벤트 케이스에 적용하면:
sql
ORDER BY (vehicle_id, timestamp)              -- 1차 프루닝: 차량 + 시간

INDEX idx_event event_type TYPE set(100) GRANULARITY 4      -- 이벤트 타입 필터
INDEX idx_sw sw_version TYPE set(50) GRANULARITY 4          -- SW 버전 필터  
INDEX idx_trace trace_id TYPE bloom_filter GRANULARITY 1    -- 특정 건 추적
→ "차량 없이 event_type만으로 조회" 같은 정렬 키가 못 커버하는 축을 보조
중요한 한계 (이걸 말해야 깊이가 나와):
정렬 키만큼 효율적이지 않아. 데이터가 그 값 기준으로 정렬돼 있지 않으면 대상 값이 여러 블록에 흩어져 있어서 스킵 효과가 제한적 — "정렬 키는 데이터를 모으고, skipping index는 흩어진 걸 걸러낼 뿐"
쓰기·저장 비용이 붙음 — 인덱스도 유지 대상
선택도가 낮으면 무용지물 — 어차피 대부분 블록을 읽어야 하면 인덱스 판정 비용만 추가
그래서 검증이 필수: EXPLAIN indexes = 1로 실제 스킵되는 granule 수를 확인하고, system.query_log로 읽은 행 수를 비교해야 해. "인덱스를 걸었으니 빨라졌겠지"가 아니라 측정으로 검증하는 게 실무 감각이야.
대안도 함께 알아두면 좋아 — Projection: 같은 데이터를 다른 정렬 순서로 한 벌 더 저장하는 기능. "차량 기준"과 "이벤트 타입 기준" 두 축이 모두 지배적이면, skipping index보다 projection이 근본적 해결이야 (저장 비용 2배가 대가).
■ 2. "이벤트 로그를 하나로 모으는 공간" — 이 해석이 맞아 보여
근거가 몇 겹으로 쌓여:
JD 조직 설명: "자율주행·차량·서비스 데이터를 통합" — 서로 다른 계통의 이벤트를 한 곳에 모으는 게 명시적 미션
데이터 루프의 전제: 차량 신호 + 사용자 행동 + 서비스 이벤트를 함께 봐야 인사이트가 나옴. 따로 저장돼 있으면 조인 자체가 어려움
ClickHouse의 특성이 정확히 이 용도에 맞음: 이질적 이벤트를 넓은 테이블 하나 또는 공통 스키마로 수용(Map/JSON), 고volume 시계열, 임의 탐색 쿼리
설계 관점에서 이어지는 질문 (면접에서 던지면 좋을 것): 이걸 단일 통합 테이블로 갈지, 이벤트 계통별 테이블 + 공통 스키마 규약으로 갈지가 핵심 판단이야.
단일 테이블: 조인 없이 크로스 분석 가능, 대신 스키마가 최소공배수가 되고 프루닝 효율 저하
분리 + 규약: 각 테이블이 자기 축으로 최적화되지만 크로스 분석 시 조인 필요 (ClickHouse의 약점)
현실적 절충: 핵심 공통 필드로 구조화된 통합 이벤트 테이블 + 고volume 계통은 별도 테이블
■ 발화 (두 내용을 합쳐서)
"ClickHouse는 정렬 키 기반 프루닝이 1차이고, 그것만으로 안 되는 축은 data skipping index로 보조할 수 있습니다. 저카디널리티인 event_type이나 sw_version은 set 인덱스, trace_id처럼 고카디널리티 동등 검색은 bloom filter가 맞고요. 다만 정렬 키만큼 효율적이진 않습니다 — 데이터가 그 값으로 모여 있지 않으면 대상이 여러 블록에 흩어져 있어서 스킵 효과가 제한적이거든요. 그래서 EXPLAIN으로 실제 스킵되는 granule 수를 확인하고 검증해야 하고, 두 축이 모두 지배적이라면 skipping index보다 projection으로 다른 정렬본을 하나 더 두는 게 근본적일 수 있습니다.
 그리고 말씀하신 것처럼 데이터 플랫폼 관점에서는 ClickHouse가 차량과 소프트웨어에서 발생하는 이벤트를 한곳에 모으는 공간으로 쓰이는 게 자연스러워 보입니다. 조직 설명에도 자율주행·차량·서비스 데이터를 통합한다고 되어 있는데, 차량 신호와 사용자 행동을 함께 봐야 인사이트가 나오니까요. 그러면 설계에서 갈리는 지점은 이걸 통합 테이블 하나로 갈지, 계통별 테이블에 공통 스키마 규약을 둘지일 것 같습니다 — 통합하면 조인 없이 크로스 분석이 되지만 스키마가 최소공배수가 되고, 분리하면 각자 최적화되지만 ClickHouse가 약한 조인이 필요해지니까요."
앵커: "정렬 키는 데이터를 모으고, skipping index는 흩어진 걸 걸러낼 뿐 — 두 축이 다 중요하면 projection."

------------------------------------------------------------------------------------------------

** Spark 잘 쓰기 - Shuffle
■ 1. Shuffle이 왜 비싼가 (메커니즘)
정의: 파티션 경계를 넘어 데이터를 재분배하는 것 — 같은 key를 같은 executor에 모아야 하는 연산에서 발생 (groupBy, join, distinct, repartition, 윈도우 함수)
비용의 정체 (4중)
디스크 쓰기/읽기 — map 단계가 shuffle 파일을 로컬 디스크에 쓰고, reduce 단계가 원격에서 fetch
네트워크 전송 — executor 간 데이터 이동
직렬화/역직렬화 — CPU 비용
Stage 경계 = 배리어 — 앞 스테이지가 전부 끝나야 다음이 시작. 가장 느린 태스크 하나가 전체를 붙잡음 (skew가 치명적인 이유)
앵커: “Spark 최적화는 대부분 shuffle을 줄이거나, 줄일 수 없으면 균등하게 만드는 일”
■ 2. Join 전략 3종 — 선택 기준
Broadcast Hash Join: 작은 쪽을 전체 executor에 복사 → shuffle 없음. 가장 빠름
조건: 한쪽이 임계값 이하 (autoBroadcastJoinThreshold, 기본 10MB)
함정: Driver를 거쳐 배포되므로 크기를 잘못 잡으면 Driver OOM (내 기존 문서의 그 에러 유형). 통계가 부정확하면 실제보다 작게 추정될 수 있음
차량 맥락 적용: 차량 마스터·신호 정의 같은 소형 차원 테이블은 broadcast 대상 — 대용량 텔레메트리와 조인할 때 이게 되느냐가 성능을 가름
Sort-Merge Join (기본): 양쪽을 key로 셔플·정렬 후 병합 — 대용량×대용량의 표준. 비싸지만 확장 가능
Shuffle Hash Join: 셔플 후 한쪽으로 해시 테이블 — 정렬 비용은 없지만 메모리 부담. AQE가 상황에 따라 선택
■ 3. Shuffle 줄이는 실전 기법
필터·프로젝션을 먼저 — 조인 전에 행·열을 줄이면 셔플되는 데이터 자체가 감소 (Catalyst가 pushdown하지만, UDF나 복잡 로직이 끼면 못 밀어냄 → UDF는 최적화 장벽)
불필요한 repartition 제거 — 습관적 repartition() 호출이 셔플 한 번을 통째로 추가. 파티션 수를 줄일 땐 coalesce(셔플 없음)를 우선 고려
연산 순서 — 조인 전 집계(pre-aggregation)로 데이터 크기를 미리 줄이기
버킷팅(bucketing) — 자주 조인하는 대형 테이블을 같은 key로 미리 버킷팅 저장 → 조인 시 셔플 생략. 반복 조인이 정해져 있으면 강력 (차량 데이터면 vehicle_id 버킷팅)
AQE의 파티션 병합 — 셔플 후 작은 파티션들을 자동 합쳐 태스크 수 최적화
■ 4. Data Skew — 진단부터 대응까지
진단 (Spark UI 보는 순서 — 이게 실무 감각):
	•	Stage의 태스크 duration 분포를 봄 → median은 10초인데 max가 10분이면 skew
	•	shuffle read size의 편차, spill 발생 여부
	•	“성공은 하는데 느린” 잡의 1순위 원인이 skew
대응 단계 (가벼운 것부터):
	1.	AQE Skew Join 활성 (기본 켜짐, Spark 3+) — 런타임에 큰 파티션을 감지해 자동 분할. 1차 방어선이자 대부분의 경우 충분
	2.	Broadcast로 전환 — 작은 쪽이 broadcast 가능하면 셔플 자체가 사라져 skew도 소멸
	3.	Salting — key에 랜덤 접미사를 붙여 인위적으로 분산 후, 조인 상대는 salt 개수만큼 복제해 매칭 → 마지막에 집계 재결합. AQE로 안 되는 극단적 편중에만 (복잡도가 크므로)
	4.	null/기본값 키 분리 — 실무에서 skew의 흔한 정체는 특정 값이 아니라 null이나 “unknown” 같은 기본값이 몰리는 것. 이 경우 해당 키만 분리 처리하는 게 salting보다 간단
차량 데이터의 skew 시나리오 (구체 예시로 답하면 좋음):
	•	테스트 차량·개발 차량이 일반 차량 대비 압도적으로 많은 데이터 생성
	•	특정 리전·차종에 차량이 집중
	•	특정 고빈도 신호 하나가 전체 볼륨의 대부분 (signal_name으로 group by 하면 그 신호 파티션만 폭발)
	•	대응: 위 단계 + 파티션 설계 단계에서 미리 분산(버킷팅) — “런타임 대응보다 레이아웃이 먼저”라는 원칙의 재확인
■ 발화 압축
“Shuffle이 비용의 중심인 이유는 디스크와 네트워크뿐 아니라 스테이지 배리어 때문입니다 — 가장 느린 태스크 하나가 전체를 붙잡으니까 skew가 곧 성능 문제가 됩니다.
그래서 먼저 shuffle을 줄이는 걸 봅니다. 조인 전에 필터와 프로젝션으로 데이터를 줄이고, 습관적인 repartition을 제거하고, 작은 차원 테이블은 broadcast로 처리합니다 — 차량 데이터라면 차량 마스터나 신호 정의 테이블이 broadcast 대상이 되겠고요. 반복적으로 같은 키로 조인한다면 버킷팅으로 셔플 자체를 생략할 수도 있습니다.
Skew는 Spark UI에서 태스크 duration 분포를 보고 진단합니다 — median 대비 max가 크게 튀면 skew고, ‘성공은 하는데 느린’ 잡의 1순위 원인입니다. 대응은 AQE Skew Join이 1차 방어선이고, 대부분 여기서 해결됩니다. 안 되면 broadcast 전환, 그래도 안 되면 salting인데 복잡도가 커서 마지막 수단입니다. 실무에서 skew의 흔한 정체가 null이나 기본값이 몰리는 경우라, 그건 해당 키만 분리하는 게 더 간단하고요. 차량 데이터라면 테스트 차량이나 특정 고빈도 신호가 편중 원인일 텐데, 근본적으로는 런타임 대응보다 파티션·버킷 설계 단계에서 분산시키는 게 우선이라고 봅니다.”
■ 앵커 3개
	1.	“Shuffle의 진짜 비용은 배리어 — 가장 느린 태스크가 전체를 붙잡는다”
	2.	“AQE가 1차 방어선, Salting은 마지막 수단”
	3.	“런타임 대응보다 레이아웃이 먼저”
**Spark 활용 경험 — 면접 대비 정리
■ 0. 포지셔닝 (첫 문장)
	•	“스트리밍과 배치, 두 실행 모델을 다 써봤고, 클라우드 3종(EMR·EKS·Dataproc)에서 운영했습니다. 그리고 애플리케이션 코드만이 아니라 실행 기반(공통 모듈·인프라 구성)까지 만들어봤습니다.”
	•	이 세 축(실행 모델 / 실행 환경 / 코드+기반)이 “다양하게 써봤다”의 실체
■ 1. Kafka → Iceberg Mini-Batch 적재 (PySpark) — 플래그십
	•	한 일: Kafka Connect 실시간 스트리밍 적재를 PySpark 기반 5분 Mini-Batch로 전환, EKS CronJob 주기 실행, Kyuubi를 통한 Spark 실행
	•	판단의 핵심: Compaction 실패의 원인을 파일 크기가 아니라 커밋 빈도로 진단 → “사후 병합 강화”가 아니라 “입구 억제” 선택. 적재 지연을 감수하고 파일 효율을 사는 trade-off
	•	Spark 관점의 논점 (꼬리 대비):
	•	트리거 간격 = 파일 생성 빈도 — 스트리밍/배치의 주기 결정이 곧 저장 레이아웃 결정
	•	쓰기 레이아웃 제어 — “간격은 필요조건, 쓰기 태스크 수·target-file-size 제어가 충분조건”
	•	Structured Streaming을 안 쓰고 배치로 간 이유 — 주기 실행으로 충분했고, CronJob 기반이 상시 구동보다 리소스·운영 면에서 단순. (스트리밍의 체크포인트/WAL 구조는 개념으로 설명 가능)
	•	멱등성 — MERGE INTO 기반 upsert로 재실행 안전
	•	스키마 정합 트러블슈팅: Java Streaming → PySpark 전환 시 epoch(Long)↔Timestamp 표현 차이, Nested nullable 불일치 → DataFrame 단계에서 명시적 캐스팅 + StructType/nullable 정렬로 해결. “Schema Evolution은 메타데이터의 일, 이건 데이터의 일 — 그래서 쓰기 경로에서 풀었다”
■ 2. Product Hub — Spark 배치 + 공통 모듈 (GCP Dataproc)
	•	한 일: 대규모 숙소 데이터의 Property Mapping을 저장 경로와 분리된 비동기 배치로 설계, Spark application 프로토타입 구현·Dataproc 구동 검증
	•	아키텍처 판단: 건당 BigTable 조회는 write latency를 해치므로 → 배치 조회가 훨씬 효율적인 BigTable 특성에 맞춰 처리를 분리. “저장은 빠르게, 무거운 판단은 비동기로”
	•	공통 모듈 3종 (재사용 기반 설계 — 여기가 차별점):
	•	BigTable 연동 모듈 — 커넥션 관리, 배치 읽기/쓰기
	•	Protobuf 직렬화/역직렬화 — Spark에서 Proto 스키마를 일관되게 처리, 한 곳에서 관리
	•	Spark Session 관리 — 설정·리소스 구성을 표준화해 일관된 실행 환경
	•	의도: “이후 추가될 배치 작업이 재사용할 수 있는 구조를 전제로 설계” — 앞서 정리한 **“온보딩 비용은 프레임워크 설계에서 결정된다”**와 같은 사고
	•	인프라까지: GCP 권한 파악, Docker 이미지 구성, Dataproc 클러스터 구성 트러블슈팅 — 실행 환경을 직접 세팅
■ 3. 그 외 Spark 접점 (넓이 증명)
	•	격리 환경 배치 수집: Embulk로 추출된 데이터를 Spark로 Data Lake 최종 적재 (2단계 파이프라인의 후단)
	•	Iceberg 유지보수: Kyuubi 통해 Spark SQL로 rewrite_data_files 실행, EKS CronJob 주기 스케줄 (Compaction 30분 / Rewrite Manifest 1시간 / Expire Snapshots 2주 — 배치 주기와 충돌 최소화 오프셋 설계)
	•	현재(SK AX): 플랫폼의 Spark 런타임 영역 담당 — 사용자에게 실행 환경을 제공하는 쪽
■ 4. “Spark를 고민하며 써봤다”를 드러내는 논점 4개 (질문이 깊어지면)
	1.	실행 모델 선택: “실시간 스트리밍이 항상 정답이 아니다 — 목적이 분석·ML 기반 저장이면 분 단위 지연은 허용 가능하고, 그 대가로 파일 효율과 운영 단순성을 얻는다”
	2.	성능 진단 순서: Spark UI에서 Stage 소요 → 태스크 duration 편차(skew 신호) → Shuffle read/write 크기 → Spill → GC. 대응은 AQE 기본, 안 되면 Salting
	3.	Driver/Executor OOM의 전형: Broadcast 오판(Driver), Skew(Executor), 그리고 Compaction planning 시 파일 목록 메모리 적재 — 내가 실제로 겪은 형태
	4.	레이아웃 우선주의: “튜닝의 시작은 설정이 아니라 데이터 배치 — 파티션 전략과 파일 크기가 성능의 대부분을 결정한다”
■ 5. 정직한 선긋기 (묻기 전에 준비)
	•	깊이의 범위: 애플리케이션 개발 + 설정 수준 튜닝 + 실행 환경 구성이 내 영역. 엔진 내부(Catalyst 룰 커스터마이징, 소스 코드 레벨 개입)는 아님
	•	표현: “Spark를 쓰는 쪽과 제공하는 쪽 양쪽에서 다뤄봤고, 튜닝은 실행 계획과 리소스·레이아웃 레벨에서 판단해왔습니다”
	•	Scala 미사용 → PySpark·Java 기반. “언어보다 실행 모델 이해가 이전 가능하다고 봅니다”


Kafka → Iceberg Spark 배치 적재 — 최종 면접 스크립트

■ 0. 배경 (30초 요약)
CDC 이벤트를 Kafka에서 읽어 Iceberg에 적재하는 파이프라인


문제: Kafka Connect 실시간 스트리밍 적재 → 분당 수십만 건을 10초 내외로 commit → 커밋마다 수십 MB 파일 양산 → 파일 생성 속도가 Compaction 병합 속도를 앞지름 → Compaction 실패


진단: 근본 원인은 파일 크기가 아니라 커밋 빈도


해결: PySpark 기반 5분 Mini-Batch 전환 → 커밋 빈도 약 30배 감소, EKS CronJob + Kyuubi 운영



■ 1. 읽기 — Kafka
① Offset 관리 (배치 방식의 대가)
스트리밍은 체크포인트가 자동 관리하지만 배치는 직접 관리 → S3의 별도 경로에 offset 저장


배치 시작 시 이전 offset을 읽고, 완료 후 갱신


② 읽기 범위와 병렬성
endingOffsets = latest — 5분치를 전부 읽는 방식


Kafka 파티션 = Spark 태스크 (파티션 내에서만 순서가 보장되므로 자연스러운 병렬 단위)


파티션 수가 병렬성 상한 → minPartitions로 offset 범위를 분할해 태스크 증설 (뒤에서 dedup하는 구조라 파티션 내 순서 의존 없음)


③ latest 방식의 리스크 인식 (회고 — 물으면 꺼낼 것)
당시엔 문제없었던 이유: 5분 주기라 배치당 부하가 예측 가능, CDC라 유입량 자체에 상한


안전장치: MERGE 기반 멱등 적재 → 실패해도 다음 주기가 같은 구간 재처리 → 실패가 손실이 아니라 지연으로만 나타남


지금이라면: offset 상한을 둘 것. 첫 적재나 장애로 밀린 상황에서 한 배치가 감당 못 할 양이 들어오면 배치 시간 > 주기 → lag 증가 → 배치 더 커짐의 악순환. Small File 문제와 같은 패턴 — 유입 속도가 처리 속도를 앞지르는 임계



■ 2. 변환
① Dedup — MERGE의 전제 조건
MERGE INTO는 source 키당 1건을 전제 → CDC는 한 배치에 같은 PK 다건이 정상 → 위반 시 런타임 에러


ROW_NUMBER() OVER (PARTITION BY pk ORDER BY ...) = 1


정렬 기준 보강: timestamp만으로는 같은 밀리초 내 순서 불명 → Debezium의 ord 값을 밀리초 뒤에 추가


② 스키마 정합 (Java Streaming → PySpark 전환 시)
epoch(Long) → Timestamp 명시 캐스팅 (밀리초 정밀도 유지), Nested StructType nullable 정렬


핵심 논리: Iceberg의 타입 승격은 재작성 없이 재해석 가능한 확장(int→long 등)만 허용. epoch→Timestamp는 값의 해석 규칙이 필요한 변환 → “Schema Evolution은 메타데이터의 일, 이건 데이터의 일” → 쓰기 경로에서 해결


결과: 기존 데이터 재적재 없이 신규 파이프라인이 기존 적재분과 정합



■ 3. 쓰기 — MERGE INTO + 파일 레이아웃
① MERGE 최적화
ON 조건에 파티션 컬럼 포함 → 프루닝 유도 (없으면 매 배치가 테이블 전체 스캔)


Late data 방어: WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE


MERGE는 read-modify-write → 호출 빈도를 줄이는 것 자체가 최적화 (Mini-Batch의 본질)


② 파일 수 제어 — 3단계 서사 (핵심)
1단계: repartition으로 태스크 수를 줄여 파일 수 감소
Compaction 실패라는 급한 상황 → 즉시 검증 가능, 커밋당 결과가 결정론적


2단계: 두 제약이 동시에 작동하고 있었음
Iceberg write.target-file-size-bytes는 기본값 512MB로 이미 작동 중


실제 파일 수 = max(repartition N, 데이터양 ÷ target 크기)


평상시엔 repartition이, burst 땐 target이 롤링해 거대 파일 방지


3단계: 운영 중 인식한 비대칭
target은 큰 파일을 쪼갤 뿐, 작은 파일을 합치지는 못함


유입이 적은 시간대엔 태스크당 데이터가 적어 작은 파일 재발 → 원인은 repartition으로 태스크 수를 고정한 것 자체


개선 방향: 파일 수를 직접 정하지 말고, distribution-mode = hash로 같은 파티션 데이터를 한 태스크에 모으고 크기 기준은 target에 위임


앵커: “개수를 고정하면 크기가 흔들리고, 크기를 고정하면 개수가 조절된다”


③ 결과 수치
전: 10초 커밋 / 커밋당 수십 MB


후: 5분 배치 / 커밋당 수백 MB / 커밋 빈도 30배 감소


보너스: 대형 파일일수록 압축 효율↑ → “Small File은 파일 수도 많고 압축도 나쁜 이중 낭비”



■ 4. 운영
OCC 충돌 회피: Compaction(30분)과 배치(5분)가 겹치면 재시도 빈발 → 실행 시각 오프셋 분리(01~03분 / 31~33분) + 겹쳐도 OCC 재시도가 안전망


유지보수 3종: Compaction 30분 / Rewrite Manifest 1시간 / Expire Snapshots 2주 — 데이터 파일뿐 아니라 메타데이터·매니페스트까지 관리 대상


모니터링: Consumer Lag 추세(값 아님), 배치 소요시간(주기 대비 비율), 커밋당 파일 수·크기, Compaction 성공률, lag/retention 비율(복구 데드라인)



■ 5. 왜 Structured Streaming이 아니라 CronJob 배치였나
요구 지연이 분 단위 — Lakehouse는 분석·ML 목적. 실시간 소비처는 Public Topic 직접 구독 경로가 별도 존재


상시 드라이버 불필요 — 실행 시점만 리소스 점유, 실패 시 다음 주기가 자동 catch-up


커밋 시점 통제 가능 — Compaction 주기와 함께 설계 (배치의 숨은 장점)


팀 스택 정합 — 이미 EKS CronJob + Kyuubi 체계 존재


균형: “대가는 offset 직접 관리. 지금이라면 Trigger.AvailableNow를 검토 — 체크포인트에 offset을 맡기면서 배치처럼 주기 실행”



■ 압축 발화 (1분 30초 버전)
“실시간 스트리밍 적재에서 Compaction이 실패하고 있었는데, 원인을 파일 크기가 아니라 커밋 빈도로 진단하고 PySpark 5분 Mini-Batch로 전환한 작업입니다. 읽기는 배치 방식이라 offset을 직접 관리해야 해서 S3에 저장했고, Kafka 파티션이 곧 Spark 태스크라 병렬성이 파티션 수에 묶이는 걸 minPartitions로 보완했습니다. 다만 endingOffsets를 latest로 뒀는데, 지금 돌아보면 상한을 두는 게 맞았다고 봅니다 — 장애로 밀리면 배치가 커지고 주기를 넘겨 lag이 더 쌓이는 악순환이 가능하니까요. 당시엔 MERGE 기반 멱등 적재라 실패가 손실이 아니라 지연으로만 나타나는 게 안전장치였습니다. 변환에서는 dedup이 핵심이었습니다. MERGE는 source 키당 1건을 전제하는데 CDC는 같은 PK가 여러 번 들어오는 게 정상이라, ROW_NUMBER로 최신 1건만 남기되 timestamp만으로는 같은 밀리초 내 순서가 안 잡혀 Debezium의 ord 값을 정렬 기준에 추가했습니다. 그리고 Java에서 PySpark로 전환하며 epoch Long과 Timestamp 표현 차이가 있었는데, Schema Evolution으로는 풀 수 없는 문제 — 값의 해석 규칙이 필요한 데이터 변환이라 — DataFrame 단계 명시적 캐스팅으로 해결해 기존 데이터 재적재 없이 전환했습니다. 쓰기가 가장 배운 게 많았습니다. 처음엔 repartition으로 태스크 수를 줄여 파일 수를 감소시켰고 급한 문제는 해결됐습니다. Iceberg의 target-file-size는 기본값 512MB로 이미 작동 중이어서, 실제로는 파일 수가 repartition과 target 중 더 강한 제약으로 결정되는 구조였습니다 — 평상시엔 repartition이, 유입이 튀면 target이 롤링해 거대 파일을 막는 식으로요. 그런데 반대 방향엔 비대칭이 있어서, target은 큰 파일을 쪼갤 뿐 작은 파일을 합치지는 못합니다. 유입이 적은 시간대엔 여전히 작은 파일이 생겼고, 원인은 repartition으로 태스크 수를 고정한 것 자체였습니다. 그래서 파일 수를 직접 정하는 대신 distribution-mode를 hash로 두고 크기 기준은 target에 맡기는 방향이 맞다고 정리했습니다. 운영에서는 Compaction과 커밋이 겹치면 OCC 충돌로 재시도가 잦아져 실행 시각을 오프셋으로 분리했고, 데이터 파일뿐 아니라 매니페스트와 스냅샷까지 각자 주기로 관리했습니다.”

■ 앵커 6개 (암기)
“원인은 파일 크기가 아니라 커밋 빈도”


“MERGE는 키당 1건이 전제 — dedup + ord로 정렬 보강”


“Schema Evolution은 메타데이터의 일, 이건 데이터의 일”


“파일 수 = max(repartition, 데이터양 ÷ target)”


“target은 큰 파일을 쪼갤 뿐 작은 파일을 합치지 못한다”


“실패가 손실이 아니라 지연으로만 나타나는 구조” (멱등성)

ClickHouse의 특징
“ClickHouse는 대용량 데이터의 분석 쿼리에 특화된 OLAP 데이터베이스입니다. 빠른 이유를 세 층으로 이해하고 있습니다.
첫째, 컬럼 저장입니다. 필요한 컬럼만 읽으니 I/O 자체가 줄고, 같은 타입의 값이 연속해 있어 dictionary나 RLE 같은 인코딩이 잘 먹혀 압축률이 높습니다. 그리고 컬럼이 메모리에 연속 배치되니 벡터화 실행과도 자연스럽게 맞물립니다 — 행 단위가 아니라 배치 단위로 처리해서 함수 호출 오버헤드를 줄이고 SIMD를 활용할 수 있고요.
둘째, 저장 구조입니다. MergeTree 엔진이 데이터를 ORDER BY로 지정한 순서대로 물리 정렬해서 저장하고, 삽입마다 생기는 파트를 백그라운드에서 머지합니다. LSM 계열 구조인데, 그래서 소량 빈번 insert는 파트 폭발을 부르고 배치 적재가 권장됩니다 — 제가 Iceberg에서 겪은 small file 문제와 같은 형태입니다.
셋째, 그 정렬을 이용한 프루닝입니다. 모든 행에 인덱스를 두는 게 아니라 8192행 단위 granule마다 하나씩만 엔트리를 두는 sparse index라, 10억 행 테이블도 인덱스가 수 MB 수준이라 메모리에 통째로 올라갑니다. 이진 탐색으로 읽을 granule만 골라내고 나머지는 아예 안 읽는 거죠. 인덱스를 작게 만드는 대신 읽기 단위를 굵게 가져가는 거래라고 이해하고 있습니다.
그래서 RDBMS와 결정적으로 다른 게 PRIMARY KEY가 유니크 제약이 아니라는 점입니다. 중복을 막는 게 아니라 정렬과 스킵의 기준일 뿐이라, 중복 제거가 필요하면 ReplacingMergeTree 같은 별도 엔진을 써야 하고 그것도 머지 시점에 비동기로 적용됩니다.”
