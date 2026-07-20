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
