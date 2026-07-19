# StarRocks Materialized View 사용자 가이드 (v4.1.1 기준)

> **대상 독자**: StarRocks에서 Iceberg 데이터를 조회하는 분석가·Analytics Engineer·Data Engineer, MV 생성·운영 담당자
> **환경 전제**: Base table은 Iceberg external catalog, MV는 StarRocks 4.1.1에서 생성·관리
> **문서 목표**: MV를 생성하는 데서 끝나지 않고, 실제 쿼리가 안정적으로 가속되고 신선도와 운영 책임이 관리되는 상태를 만든다.
> **별도 문서**: 내부 구조와 optimizer/refresh 원리는 「StarRocks MV 엔지니어링 심화 가이드」를 참고한다.

예시의 catalog·database·table·resource group 이름은 실제 환경에 맞게 변경한다.

## 이 문서를 읽는 순서

- **조회 사용자**: 1장 → 2장 → 5장 → 6장 → 10장
- **MV 생성자**: 1장부터 9장까지
- **플랫폼 운영자**: 7장부터 부록까지 포함해 검토

---

## 목차

1. [MV를 왜 쓰는가](#1-mv를-왜-쓰는가)
2. [가장 먼저 결정할 것: Transparent rewrite인가 Direct query인가](#2-가장-먼저-결정할-것-transparent-rewrite인가-direct-query인가)
3. [만들기 전에 확인할 것](#3-만들기-전에-확인할-것)
4. [상황별 MV 생성 레시피](#4-상황별-mv-생성-레시피)
5. [Query Rewrite를 잘 활용하는 법](#5-query-rewrite를-잘-활용하는-법)
6. [Freshness를 사용자 계약으로 정의하는 법](#6-freshness를-사용자-계약으로-정의하는-법)
7. [생성 후 검증 절차](#7-생성-후-검증-절차)
8. [운영 중 문제 해결](#8-운영-중-문제-해결)
9. [주의사항과 플랫폼 승인 대상](#9-주의사항과-플랫폼-승인-대상)
10. [FAQ](#10-faq)
11. [부록 A. MV 요청 양식](#11-부록-a-mv-요청-양식)
12. [부록 B. 공식 참고 자료](#12-부록-b-공식-참고-자료)

---

## 1. MV를 왜 쓰는가

대시보드·리포트·정기 분석 쿼리는 같은 조인과 집계를 반복한다. 매번 Iceberg 원천의 파일을 읽고 동일한 계산을 수행하면 응답 시간이 길어지고, 원격 I/O와 클러스터 컴퓨팅을 반복해서 소비한다.

Materialized View는 계산 결과를 StarRocks가 관리하는 물리 데이터로 저장하고 정해진 방식으로 갱신한다.

- **쿼리 가속**: 원천 파일 스캔과 무거운 조인·집계를 줄인다.
- **동시성 향상**: 반복 계산을 줄여 동일 자원에서 더 많은 조회를 처리할 수 있다.
- **Transparent query rewrite**: 조건을 만족하는 PCT MV가 있으면 기존 base table SQL을 바꾸지 않고도 optimizer가 MV를 선택할 수 있다.

다만 다음 사항을 전제로 한다.

- MV가 생성됐다고 모든 쿼리가 자동으로 rewrite되는 것은 아니다.
- Partition mapping이 없거나 깨져 있으면 refresh가 전체 재계산에 가까워질 수 있다.
- External catalog MV는 base 변경 직후 자동으로 refresh되지 않으며, 고정 주기 또는 수동 refresh를 사용한다.
- Shared-data 환경에서는 MV 데이터가 항상 특정 노드의 로컬 디스크에만 존재한다고 표현해서는 안 된다. 핵심은 Iceberg 원천이 아니라 StarRocks 관리 형식으로 materialize된다는 점이다.

### MV가 적합한 경우

- 동일하거나 유사한 join·aggregation 쿼리가 반복된다.
- Data Cache가 적중해도 계산 시간이 SLA를 초과한다.
- 쿼리 패턴과 주요 필터 축이 비교적 안정적이다.
- 수분에서 수시간의 결과 지연을 명시적으로 허용할 수 있다.
- 원천 partition과 MV partition을 매핑할 수 있다.

### 먼저 다른 방법을 검토할 경우

- 일회성 탐색 쿼리이거나 재사용 가능성이 낮다.
- 병목이 원격 파일 I/O뿐이고 join·aggregation 비용은 작다. 이 경우 Data Cache와 partition pruning부터 확인한다.
- 쿼리 조건과 grain이 매번 크게 달라 공통 계산을 찾기 어렵다.
- 항상 최신 결과가 필수다.
- 원천이 비파티션 대형 table이고 변경이 잦아 refresh 비용을 제한하기 어렵다.
- 현재 쿼리가 이미 충분히 빠르다.

> MV는 만들수록 좋은 자원이 아니다. Refresh compute, 저장 공간, metadata, optimizer planning 비용을 함께 소비한다.

---

## 2. 가장 먼저 결정할 것: Transparent rewrite인가 Direct query인가

| 방식 | 사용자가 실행하는 쿼리 | 권장 refresh mode | 4.1.1 주의점 |
|---|---|---|---|
| **Transparent rewrite** | 기존처럼 base Iceberg table 조회 | `PCT` | `EXPLAIN`으로 실제 MV 선택 여부를 확인해야 한다. |
| **Direct query** | 애플리케이션·BI가 MV 이름을 직접 조회 | `PCT`, 제한적으로 `INCREMENTAL` | MV의 현재 저장 상태를 그대로 읽으므로 freshness 계약과 fallback이 필요하다. |

기존 SQL을 바꾸지 않고 가속하려면 `PCT`를 기본 선택으로 사용한다.

StarRocks 4.1.1에서는 `INCREMENTAL` 및 `AUTO` MV가 query rewrite 후보로 사용되지 않으며, 이 모드의 MV에 대한 `FORCE` refresh와 partition 지정 refresh도 거부된다.

`INCREMENTAL`은 다음 조건을 모두 만족할 때만 검토한다.

1. 원천이 Iceberg append-only table이다.
2. Consumer가 MV를 직접 조회해도 된다.
3. `UPDATE`, `MERGE`, `OVERWRITE`, `DELETE` 등 non-append operation을 통제할 수 있다.
4. Compaction의 snapshot operation이 현재 환경에서 안전한지 별도 검증했다.
5. Refresh 실패 시 재생성 또는 PCT MV로 전환하는 절차가 있다.

---

## 3. 만들기 전에 확인할 것

### 3.1 기존 MV부터 확인한다

같은 목적의 MV가 이미 있는지 먼저 확인한다. 중복 MV는 refresh와 저장 비용을 중복으로 사용하고 optimizer의 후보 탐색 비용도 증가시킨다.

```sql
SHOW MATERIALIZED VIEWS FROM dw;

SHOW MATERIALIZED VIEWS FROM dw
WHERE NAME LIKE "mv_sales%";
```

정의와 운영 정보를 함께 확인하려면 Information Schema를 사용한다.

```sql
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    IS_ACTIVE,
    REFRESH_MODE,
    REFRESH_TRIGGER,
    REFRESH_POLICY,
    QUERY_REWRITE_STATUS,
    QUERY_REWRITE_STATUS_REASON,
    MATERIALIZED_VIEW_DEFINITION
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = 'dw'
  AND TABLE_NAME LIKE 'mv_sales%';
```

비슷한 MV가 있다면 다음 순서로 검토한다.

1. 기존 MV로 대표 쿼리가 rewrite되는지 확인한다.
2. 부족한 column이나 grain만 확장할 수 있는지 검토한다.
3. Nested MV가 필요한 경우 플랫폼 팀과 refresh chain 및 지연을 검토한다.
4. 신규 MV가 불가피할 때만 생성한다.

### 3.2 생성 전에 필요한 정보를 정리한다

| 항목 | 작성 내용 |
|---|---|
| 대표 쿼리 | 실제 운영 SQL 1~3개 |
| 사용 방식 | Transparent rewrite / Direct query |
| 성능 목표 | 현재 p50·p95, 목표 latency, scan bytes |
| Freshness | 허용 source-to-query lag |
| 주요 필터 | 날짜, tenant, region, store 등 |
| Join·Group key | 실제 반복 쿼리 기준 |
| 원천 partition | Iceberg partition column과 transform, spec 변경 이력 |
| Write operation | append, overwrite, merge, delete, compaction 여부 |
| 사용량 | 호출 빈도, 동시 사용자, 조회 기간 |
| Owner | 장애 대응 팀과 문의 채널 |
| Fallback | MV 장애 시 base query 또는 대체 경로 |
| 폐기 기준 | 일정 기간 rewrite/direct-query 사용 없음 등 |

### 3.3 공통 설계 규칙

1. 쿼리의 필터 predicate로 쓰는 column은 MV 출력에 포함한다.
2. 날짜 기반 대형 MV는 가능한 한 실제 Iceberg partition spec과 매핑되는 `PARTITION BY`를 둔다.
3. `ORDER BY` 물리 sort key는 반복되는 filter와 scan pattern을 기준으로 정한다.
4. Join key 또는 고빈도 group key를 distribution key 후보로 검토한다.
5. Rewrite 목적 MV의 defining query 내부에는 `ORDER BY`를 넣지 않는다. 물리 sort key는 CREATE 문장의 `ORDER BY (...)`로 지정한다.
6. `rand()`, `random()`, `uuid()`, `sleep()` 같은 비결정적 함수는 rewrite 대상 MV와 쿼리에서 피한다.
7. 생성 직후 대규모 전체 refresh가 위험하면 `REFRESH DEFERRED`를 사용한다.
8. External Iceberg MV의 수동 refresh는 모든 MV partition을 refresh할 수 있으므로 초기 적재 비용을 사전에 산정한다.

---

## 4. 상황별 MV 생성 레시피

### 4.0 플랫폼 컨벤션 예시

- 이름: `mv_<domain>_<grain>_<purpose>`
- `COMMENT`: 목적, owner, 문의 채널, freshness SLA, 사용 방식 기록
- `refresh_mode`: Rewrite 목적은 `pct`, incremental은 플랫폼 승인 후 사용
- `query_rewrite_consistency`: 기본은 `checked`
- Resource group, timeout, spill 설정은 클러스터 정책에 맞게 지정
- Generic template에 고정된 `START` 날짜를 넣지 않는다. 운영 스케줄이 필요하면 배포 시점에 결정한다.

### 4.1 기본형: 일 단위 집계 PCT MV

아래 예시에서 `order_dt`는 DATE형이며 Iceberg base도 일 단위로 partition되어 있다고 가정한다.

```sql
CREATE MATERIALIZED VIEW dw.mv_sales_daily_store_agg
COMMENT "purpose=daily_store_sales; owner=commerce-data; mode=rewrite; source_to_query_sla=75m"
DISTRIBUTED BY HASH(store_id)
REFRESH DEFERRED
SCHEDULE EVERY (INTERVAL 1 HOUR)
PARTITION BY date_trunc('day', order_dt)
ORDER BY (order_dt, store_id)
PROPERTIES (
    "refresh_mode" = "pct",
    "query_rewrite_consistency" = "checked",
    "mv_rewrite_staleness_second" = "3600"
)
AS
SELECT
    order_dt,
    store_id,
    sum(amount) AS total_amount,
    count(*) AS order_cnt
FROM iceberg_cat.sales.orders
GROUP BY order_dt, store_id;
```

핵심 포인트:

- `PARTITION BY`는 logical column 이름만 보고 정하지 말고 실제 Iceberg partition transform과 매핑되는지 확인한다.
- `mv_rewrite_staleness_second=3600`은 “데이터가 최대 1시간만 지연된다”는 보장이 아니다. 마지막 refresh 시각이 1시간 이내이면 base 변경 여부와 무관하게 rewrite를 허용하는 설정이다.
- 일·매장 grain의 `sum`과 `count`는 월·지역 등 상위 grain으로 rollup될 수 있다.
- `count(distinct)`는 단순 합산으로 rollup할 수 없으므로 별도 설계가 필요하다.
- `partition_refresh_number`와 `partition_refresh_strategy`는 실제 partition 크기와 refresh duration을 측정한 뒤 조정한다. 4.1 계열 기본 batch 크기는 1 partition이다.

### 4.2 조인 가속 MV

```sql
CREATE MATERIALIZED VIEW dw.mv_orders_with_store
COMMENT "purpose=flatten_orders_store; owner=commerce-data; mode=rewrite"
DISTRIBUTED BY HASH(order_id)
REFRESH DEFERRED
SCHEDULE EVERY (INTERVAL 2 HOUR)
PARTITION BY date_trunc('day', order_dt)
ORDER BY (order_dt, store_id)
PROPERTIES (
    "refresh_mode" = "pct",
    "query_rewrite_consistency" = "checked",
    "mv_rewrite_staleness_second" = "7200"
)
AS
SELECT
    o.order_id,
    o.order_dt,
    o.amount,
    o.customer_id,
    s.store_id,
    s.store_name,
    s.region,
    s.store_type
FROM iceberg_cat.sales.orders o
JOIN iceberg_cat.master.stores s
  ON o.store_id = s.store_id;
```

조인 MV는 다음을 추가로 확인한다.

- Fact와 dimension 변경이 어떤 MV partition refresh로 이어지는가.
- Unpartitioned dimension 변경으로 refresh 범위가 과도하게 넓어지지 않는가.
- Join key cardinality와 distribution이 skew를 만들지 않는가.
- 추가 filter·aggregation 쿼리는 논리적 유도 가능성, freshness gate, CBO 판단을 모두 통과해야 rewrite된다.

### 4.3 `COUNT(DISTINCT)` rollup 가속 MV

정확한 distinct count를 상위 grain으로 rollup해야 한다면 bitmap aggregate state를 저장한다.

```sql
CREATE MATERIALIZED VIEW dw.mv_dau_bitmap
COMMENT "purpose=dau_bitmap; owner=growth-data; mode=rewrite"
DISTRIBUTED BY HASH(event_dt)
REFRESH DEFERRED
SCHEDULE EVERY (INTERVAL 1 HOUR)
PARTITION BY date_trunc('day', event_dt)
ORDER BY (event_dt, channel)
PROPERTIES (
    "refresh_mode" = "pct",
    "query_rewrite_consistency" = "checked"
)
AS
SELECT
    event_dt,
    channel,
    bitmap_union(to_bitmap(user_id)) AS user_bitmap
FROM iceberg_cat.log.app_events
GROUP BY event_dt, channel;
```

```sql
SELECT
    event_dt,
    count(DISTINCT user_id)
FROM iceberg_cat.log.app_events
WHERE event_dt >= '2026-07-01'
GROUP BY event_dt;
```

주의사항:

- `to_bitmap` 입력은 bitmap 표현에 적합한 비음수 정수 ID인지 확인한다.
- 문자열 ID나 범위가 맞지 않는 ID는 hash·dictionary·auto-increment 기반 전략을 별도로 검토한다.
- MV에 `count(distinct)` 결과값만 저장하면 더 큰 grain으로의 rollup이 제한될 수 있다.

### 4.4 최근 구간만 유지하는 MV

```sql
CREATE MATERIALIZED VIEW dw.mv_sales_daily_hot
COMMENT "purpose=recent_sales_acceleration; owner=commerce-data; retention=3m"
DISTRIBUTED BY HASH(store_id)
REFRESH DEFERRED
SCHEDULE EVERY (INTERVAL 1 HOUR)
PARTITION BY date_trunc('day', order_dt)
ORDER BY (order_dt, store_id)
PROPERTIES (
    "refresh_mode" = "pct",
    "partition_ttl" = "3 MONTH",
    "query_rewrite_consistency" = "checked"
)
AS
SELECT
    order_dt,
    store_id,
    sum(amount) AS total_amount,
    count(*) AS order_cnt
FROM iceberg_cat.sales.orders
GROUP BY order_dt, store_id;
```

최근 구간은 MV, 이전 구간은 base table로 보상하는 Union rewrite가 가능하다. 다만 실제 mixed-range 쿼리의 plan은 반드시 `EXPLAIN`으로 확인한다.

### 4.5 Direct query용 `INCREMENTAL` MV

```sql
CREATE MATERIALIZED VIEW serving.mv_event_hourly_incremental
COMMENT "purpose=event_hourly_serving; owner=serving; mode=direct_query; source_contract=append_only"
DISTRIBUTED BY HASH(tenant_id)
REFRESH DEFERRED
SCHEDULE EVERY (INTERVAL 5 MINUTE)
PARTITION BY event_day
ORDER BY (event_day, event_hour, tenant_id)
PROPERTIES (
    "refresh_mode" = "incremental",
    "query_rewrite_consistency" = "disable"
)
AS
SELECT
    date_trunc('day', event_time) AS event_day,
    date_trunc('hour', event_time) AS event_hour,
    tenant_id,
    event_type,
    count(*) AS event_count
FROM iceberg_cat.events.raw_events
GROUP BY
    date_trunc('day', event_time),
    date_trunc('hour', event_time),
    tenant_id,
    event_type;
```

4.1.1 기준 제한:

- Query rewrite에 참여하지 않는다.
- `FORCE` refresh를 사용할 수 없다.
- Partition 범위를 지정한 refresh를 사용할 수 없다.
- Iceberg append-only가 아닌 operation이 발생하면 refresh가 실패할 수 있다.
- `DISTINCT` aggregate와 일부 operator 조합은 incremental refresh 대상이 아니다.
- Compaction이 생성하는 `replace` snapshot은 배포 전에 현재 writer와 StarRocks 조합에서 반드시 실측한다.

`AUTO`는 4.1.1 내부 상태와 release note에는 등장하지만 공개 CREATE SQL의 안정적인 사용자 계약으로 안내하지 않는다.

---

## 5. Query Rewrite를 잘 활용하는 법

### 5.1 원칙: PCT MV는 base table을 그대로 조회한다

```sql
-- MV와 유사한 grain
SELECT order_dt, store_id, sum(amount)
FROM iceberg_cat.sales.orders
GROUP BY order_dt, store_id;

-- 더 좁은 predicate
SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE store_id = 1024
  AND order_dt >= '2026-07-01'
GROUP BY order_dt;

-- 상위 grain rollup
SELECT date_trunc('month', order_dt) AS month_start, sum(amount)
FROM iceberg_cat.sales.orders
GROUP BY date_trunc('month', order_dt);
```

Optimizer는 SQL 문자열의 단순 일치가 아니라 “query 결과를 MV 결과로부터 유도할 수 있는가”를 판단한다. 그 뒤 freshness와 cost 조건을 적용해 최종 plan을 선택한다.

### 5.2 Rewrite가 안 되는 대표 패턴

- Query가 MV에 없는 column을 요구한다.
- Query grain이 MV보다 더 세밀하다.
- `count(distinct)` 결과값을 단순 합산해 상위 grain을 계산하려 한다.
- MV나 query에 rewrite가 지원되지 않는 비결정적 함수가 있다.
- MV가 `INACTIVE`다.
- MV가 freshness 조건을 통과하지 못한다.
- `refresh_mode=INCREMENTAL`이다.
- CBO가 base scan이 더 저렴하다고 판단한다.

예시:

```sql
-- MV에 coupon_id가 없다면 유도할 수 없다.
SELECT order_dt, coupon_id, sum(amount)
FROM iceberg_cat.sales.orders
GROUP BY order_dt, coupon_id;

-- MV는 일·매장 grain인데 customer grain을 요구한다.
SELECT order_dt, store_id, customer_id, sum(amount)
FROM iceberg_cat.sales.orders
GROUP BY order_dt, store_id, customer_id;

-- 비결정적 predicate 예시
SELECT store_id, sum(amount)
FROM iceberg_cat.sales.orders
WHERE rand() < 0.1
GROUP BY store_id;
```

`current_date()`를 비결정적 함수의 대표 예로 단정하지 않는다. 공식 제한에 명시된 `rand`, `random`, `uuid`, `sleep` 같은 함수부터 피한다.

### 5.3 Rewrite 여부 확인

```sql
EXPLAIN
SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01'
GROUP BY order_dt;
```

Plan의 `OlapScanNode`에서 `TABLE`이 대상 MV 이름이면 rewrite가 성립한 것이다.

```sql
TRACE REASON MV
SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01'
GROUP BY order_dt;

TRACE LOGS MV
SELECT order_dt, sum(amount)
FROM iceberg_cat.sales.orders
WHERE order_dt >= '2026-07-01'
GROUP BY order_dt;
```

### 5.4 View·Text-based rewrite

논리 view를 경유한 MV rewrite는 기본 비활성이므로 필요한 세션에서 별도 활성화한다.

```sql
SET enable_view_based_mv_rewrite = true;
```

Text-based rewrite는 기본 활성화이며 query 또는 subquery의 정규화된 AST match를 사용한다.

```sql
SHOW VARIABLES LIKE 'enable_materialized_view_text_match_rewrite';
SHOW VARIABLES LIKE 'materialized_view_subquery_text_match_max_count';
```

후보 수 상한을 무작정 늘리면 optimizer planning time이 증가할 수 있다.

### 5.5 MV 직접 조회

`INCREMENTAL` MV는 4.1.1에서 rewrite 대상이 아니므로 직접 조회한다.

```sql
SELECT *
FROM serving.mv_event_hourly_incremental
WHERE event_hour >= '2026-07-17 00:00:00';
```

PCT MV도 직접 조회할 수 있지만, 일반 조회는 base SQL과 transparent rewrite를 우선 권장한다. 이를 통해 MV의 이름이나 세부 구조에 consumer가 직접 결합되는 것을 줄일 수 있다.

---

## 6. Freshness를 사용자 계약으로 정의하는 법

“1시간마다 refresh”와 “최대 1시간 지연”은 같은 의미가 아니다.

External Iceberg MV의 source-to-query lag는 대략 다음 요소로 구성된다.

```text
Iceberg snapshot이 StarRocks에 보이기까지의 metadata visibility 시간
+ 다음 schedule까지 기다리는 시간
+ refresh queue 대기 시간
+ refresh 실행 시간
+ 실패와 재시도 시간
```

따라서 최소한 다음을 구분한다.

- **Refresh interval**: refresh 시작을 시도하는 주기
- **Refresh duration**: 실제 계산과 commit에 걸리는 시간
- **Source-to-MV lag**: source commit부터 MV 반영까지의 시간
- **Allowed query staleness**: 사용자가 허용하는 결과 지연
- **Rewrite staleness allowance**: `mv_rewrite_staleness_second`가 허용하는 마지막 refresh age

예시 계약:

> 매시 10분에 refresh를 시작하며 정상 상태에서 15분 이내 완료한다. Iceberg metadata visibility를 포함한 source-to-query lag의 p95 목표는 75분이다. 마지막 성공 refresh가 90분을 초과하면 알림을 발생시키고 base query fallback을 사용한다.

`mv_rewrite_staleness_second`는 SLA enforcement 기능이 아니다. 마지막 refresh가 설정 시간 이내이면 base가 변경되었더라도 rewrite를 허용하는 optimizer 정책이다.

### 강한 정합성이 필요한 조회

정산·감사처럼 시점 정합성이 중요한 경우 다음 두 가지를 모두 확인한다.

1. MV rewrite를 사용하지 않는다.
2. StarRocks가 의도한 최신 Iceberg snapshot을 실제로 인식하고 있는지 확인한다.

```sql
-- 별도의 검증 세션에서 실행한다.
SET enable_materialized_view_rewrite = false;
SELECT ... FROM iceberg_cat.sales.orders ...;
```

MV rewrite만 끈다고 external catalog metadata cache까지 자동으로 최신이 되는 것은 아니다.

---

## 7. 생성 후 검증 절차

### 7.1 DDL과 상태 확인

```sql
SHOW CREATE MATERIALIZED VIEW dw.mv_sales_daily_store_agg;

SHOW MATERIALIZED VIEWS FROM dw
WHERE NAME = "mv_sales_daily_store_agg";
```

확인 항목:

- `IS_ACTIVE = true`
- `REFRESH_MODE = PCT` 또는 의도한 모드
- `REFRESH_TRIGGER`와 `REFRESH_POLICY`
- `QUERY_REWRITE_STATUS`와 reason
- Last refresh state, duration, error
- 실제 partition과 sort/distribution 설정

### 7.2 Refresh 이력 확인

MV 이름을 task name으로 추정하지 말고 metadata view를 통해 정확히 연결한다.

```sql
SELECT
    tr.TASK_NAME,
    tr.STATE,
    tr.CREATE_TIME,
    tr.PROCESS_TIME,
    tr.FINISH_TIME,
    tr.ERROR_CODE,
    tr.ERROR_MESSAGE,
    tr.EXTRA_MESSAGE
FROM information_schema.task_runs tr
JOIN information_schema.materialized_views mv
  ON tr.TASK_NAME = mv.TASK_NAME
WHERE mv.TABLE_SCHEMA = 'dw'
  AND mv.TABLE_NAME = 'mv_sales_daily_store_agg'
ORDER BY tr.CREATE_TIME DESC
LIMIT 20;
```

`EXTRA_MESSAGE`에서는 다음을 확인한다.

- 실제 refresh된 MV partition
- 계획된 base partition과 실제 scan partition
- 다음 partition batch 경계
- `CREATE_TIME`, `PROCESS_TIME`, `EXTRA_MESSAGE.processStartTime`을 이용한 queue·실행 구간

### 7.3 정합성 검증

External MV의 수동 refresh는 전체 partition refresh가 될 수 있다. 대형 production MV에 아래 명령을 무조건 실행하지 말고, 검증 환경·소형 MV 또는 전체 비용을 승인한 배포 창에서만 사용한다.

```sql
REFRESH MATERIALIZED VIEW dw.mv_sales_daily_store_agg WITH SYNC MODE;
```

검증 쿼리는 rewrite를 끈 별도 세션에서 실행한다.

```sql
SET enable_materialized_view_rewrite = false;

WITH base_agg AS (
    SELECT
        order_dt,
        store_id,
        sum(amount) AS total_amount,
        count(*) AS order_cnt
    FROM iceberg_cat.sales.orders
    WHERE order_dt >= '2026-07-01'
      AND order_dt <  '2026-07-16'
    GROUP BY order_dt, store_id
),
mv_agg AS (
    SELECT
        order_dt,
        store_id,
        total_amount,
        order_cnt
    FROM dw.mv_sales_daily_store_agg
    WHERE order_dt >= '2026-07-01'
      AND order_dt <  '2026-07-16'
)
SELECT
    'base' AS source,
    count(*) AS row_cnt,
    sum(total_amount) AS amount_sum,
    sum(order_cnt) AS order_cnt
FROM base_agg
UNION ALL
SELECT
    'mv' AS source,
    count(*) AS row_cnt,
    sum(total_amount) AS amount_sum,
    sum(order_cnt) AS order_cnt
FROM mv_agg;
```

행 단위 차이도 확인한다.

```sql
WITH base_agg AS (
    SELECT order_dt, store_id, sum(amount) AS total_amount, count(*) AS order_cnt
    FROM iceberg_cat.sales.orders
    WHERE order_dt >= '2026-07-01'
      AND order_dt <  '2026-07-16'
    GROUP BY order_dt, store_id
),
mv_agg AS (
    SELECT order_dt, store_id, total_amount, order_cnt
    FROM dw.mv_sales_daily_store_agg
    WHERE order_dt >= '2026-07-01'
      AND order_dt <  '2026-07-16'
)
SELECT
    coalesce(b.order_dt, m.order_dt) AS order_dt,
    coalesce(b.store_id, m.store_id) AS store_id,
    b.total_amount AS base_amount,
    m.total_amount AS mv_amount,
    b.order_cnt AS base_count,
    m.order_cnt AS mv_count
FROM base_agg b
FULL OUTER JOIN mv_agg m
  ON b.order_dt = m.order_dt
 AND b.store_id = m.store_id
WHERE b.order_dt IS NULL
   OR m.order_dt IS NULL
   OR b.total_amount <> m.total_amount
   OR b.order_cnt <> m.order_cnt;
```

- 최근 partition과 과거 변경 partition을 모두 포함한다.
- FLOAT·DOUBLE 합산은 허용 오차를 정의한다. 금액은 가능하면 DECIMAL로 검증한다.
- 검증 중 source commit이 발생했는지 snapshot을 함께 기록한다.
- 검증 종료 후 세션 설정을 복구하거나 세션을 종료한다.

### 7.4 Rewrite와 성능 검증

대표 쿼리 3~5개에 대해 다음을 확인한다.

1. `EXPLAIN`에서 MV rewrite 성립
2. `TRACE REASON MV`에서 탈락 사유 없음
3. Rewrite on/off의 scan rows, scan bytes, CPU time, wall time 비교
4. Cold cache 단발성 수치가 아니라 warm-up 후 여러 번 실행한 p50·p95 비교
5. Planning time이 과도하게 증가하지 않았는지 확인

```sql
SET enable_materialized_view_rewrite = false;
SELECT /* mv_benchmark_off */ ...;

SET enable_materialized_view_rewrite = true;
SELECT /* mv_benchmark_on */ ...;
```

### 7.5 Partition refresh 검증

Source의 특정 partition만 변경된 뒤 scheduled refresh 결과를 확인한다.

- `EXTRA_MESSAGE.mvPartitionsToRefresh`
- `refBasePartitionsToRefreshMap`
- `basePartitionsToRefreshMap`
- `SHOW MATERIALIZED VIEWS`의 last refresh partition fields

매번 모든 partition이 대상으로 잡히면 다음을 확인한다.

1. Iceberg partition spec과 MV `PARTITION BY`가 실제로 매핑되는가.
2. Partition spec이 변경됐는가.
3. Join된 다른 base table 변경 때문에 범위가 확장되는가.
4. Manual refresh를 사용해 all-partition refresh가 발생한 것은 아닌가.

### 7.6 Freshness 검증

```sql
SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    LAST_REFRESH_FINISHED_TIME,
    LAST_REFRESH_TIME,
    BASE_TABLE_REFRESH_VERSION_TIMES,
    LAST_REFRESH_STATE,
    LAST_REFRESH_ERROR_MESSAGE
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = 'dw'
  AND TABLE_NAME = 'mv_sales_daily_store_agg';
```

Iceberg snapshot 이력도 확인한다.

```sql
SELECT committed_at, snapshot_id, operation, summary
FROM iceberg_cat.sales.orders$snapshots
ORDER BY committed_at DESC
LIMIT 10;
```

Wall clock의 마지막 refresh 완료 시각만 보지 말고, `LAST_REFRESH_TIME`과 `BASE_TABLE_REFRESH_VERSION_TIMES`를 사용해 어떤 source version까지 반영됐는지 확인한다.

### 7.7 배포 완료 체크리스트

- [ ] 기존 MV와 중복 여부를 확인했다.
- [ ] Transparent rewrite / Direct query 방식을 결정했다.
- [ ] Owner, SLA, fallback, 폐기 기준을 기록했다.
- [ ] 최종 DDL을 보관했다.
- [ ] 초기 refresh 비용과 duration을 기록했다.
- [ ] 대표 partition에서 base와 MV 정합성을 비교했다.
- [ ] 대표 base query의 rewrite와 성능 이득을 확인했다.
- [ ] 변경 partition만 refresh되는지 확인했다.
- [ ] 최소 2~3 refresh 주기 동안 freshness를 관찰했다.
- [ ] Failure, lag, `INACTIVE` 상태 알림을 연결했다.

---

## 8. 운영 중 문제 해결

### 8.1 MV가 `INACTIVE`인 경우

증상:

- Refresh가 실행되지 않는다.
- Base query가 MV로 rewrite되지 않는다.
- MV 직접 조회가 제한될 수 있다.

진단:

```sql
SHOW MATERIALIZED VIEWS FROM dw
WHERE NAME = "mv_sales_daily_store_agg";
```

`INACTIVE_REASON`과 base schema·view 정의 변경을 확인한다.

호환 가능한 상태로 복구한 뒤 재활성화를 시도한다.

```sql
ALTER MATERIALIZED VIEW dw.mv_sales_daily_store_agg ACTIVE;
```

모든 schema 변경이 MV를 반드시 inactive로 만드는 것은 아니며, 반대로 정의가 호환되지 않으면 `ACTIVE` 명령만으로 복구되지 않는다. 이 경우 MV 수정 또는 재생성이 필요하다.

### 8.2 Refresh가 실패한 경우

진단 순서:

1. `SHOW MATERIALIZED VIEWS`의 last refresh error
2. `information_schema.task_runs`의 `ERROR_MESSAGE`와 `EXTRA_MESSAGE`
3. Iceberg catalog 접근과 snapshot visibility
4. Queue, timeout, memory, spill, resource group 상태
5. `INCREMENTAL`이면 snapshot operation이 append-only 계약을 위반했는지 확인

`INCREMENTAL` 실패를 `FORCE`로 우회하려 하지 않는다. 4.1.1은 `INCREMENTAL/AUTO` MV의 FORCE와 partition refresh를 거부한다.

### 8.3 Refresh 시간이 schedule interval보다 긴 경우

동일 MV는 동시에 여러 refresh를 병렬 실행하는 것으로 가정해서는 안 된다. Refresh duration이 interval보다 길면 backlog가 누적될 수 있다.

대응 순서:

1. Partition mapping과 실제 scan partition 확인
2. Queue 대기와 실행 시간 분리
3. Partition grain 조정
4. `partition_refresh_strategy=adaptive` 검토
5. Resource group, spill, timeout 조정
6. Refresh interval 완화
7. Append-only 계약이 확실한 direct-query use case라면 INCREMENTAL 별도 검토

### 8.4 MV가 있는데 rewrite되지 않는 경우

다음 순서로 확인한다.

1. `REFRESH_MODE`가 `INCREMENTAL/AUTO`인가.
2. `IS_ACTIVE`가 true인가.
3. `QUERY_REWRITE_STATUS`가 enabled 상태인가.
4. 세션의 `enable_materialized_view_rewrite`가 true인가.
5. Query가 요구하는 column과 grain이 MV로부터 유도 가능한가.
6. Freshness와 external consistency gate를 통과하는가.
7. Defining query에 rewrite 제한 구조가 있는가.
8. CBO가 base scan을 더 저렴하게 판단했는가.
9. `TRACE REASON MV`에 어떤 탈락 사유가 표시되는가.

---

## 9. 주의사항과 플랫폼 승인 대상

| 패턴 | 문제와 권장 대응 |
|---|---|
| 유사 MV를 확인 없이 신규 생성 | Refresh·storage·planning 비용이 중복된다. 기존 MV부터 확인한다. |
| 필터 column을 MV 출력에서 누락 | Predicate compensation이 불가능해질 수 있다. 대표 쿼리 column을 기준으로 설계한다. |
| `mv_rewrite_staleness_second`를 freshness 보장으로 해석 | 마지막 refresh age 기반 rewrite 허용값일 뿐이다. 실제 lag를 별도 모니터링한다. |
| External MV에서 partition 수동 refresh가 비용을 제한한다고 가정 | 4.1 REFRESH 레퍼런스는 external MV 수동 refresh 시 전체 partition refresh를 경고한다. Scheduled PCT와 task run 실측을 우선한다. |
| Rewrite 목적에 `INCREMENTAL` 사용 | 4.1.1에서 rewrite가 비활성이다. PCT를 사용한다. |
| Append-only 확인 없이 `INCREMENTAL` 사용 | Non-append operation에서 refresh가 실패할 수 있다. |
| Compaction 영향 검증 없이 `INCREMENTAL` 사용 | `replace` snapshot 처리 계약을 환경에서 검증해야 한다. |
| `AUTO`를 일반 사용자 DDL에 사용 | 공개 CREATE SQL의 안정적인 사용자 계약으로 안내하지 않는다. |
| `loose` 또는 `force_mv`를 사유 없이 사용 | Stale result를 강제로 노출할 수 있다. 플랫폼 승인이 필요하다. |
| Defining query 내부 `ORDER BY` 사용 | SPJG query rewrite 대상에서 제외될 수 있다. 물리 sort key만 지정한다. |
| Base schema를 사전 공유 없이 변경 | MV inactive 또는 refresh/rewrite 장애로 이어질 수 있다. 영향 MV를 먼저 확인한다. |
| Owner·SLA·fallback 없는 MV | 장애와 비용 책임이 불명확하다. 생성 승인 조건으로 강제한다. |
| `EXPLAIN` 없이 rewrite를 가정 | MV 생성 성공과 query rewrite 성립은 별개다. |
| 무분별한 nested MV | Refresh chain과 source-to-query lag가 증가한다. 플랫폼 팀과 설계한다. |

---

## 10. FAQ

### Q. MV를 쓰면 쿼리를 바꿔야 하나요?

Transparent rewrite용 PCT MV라면 base table SQL을 그대로 사용한다. 다만 optimizer가 실제 MV를 선택했는지는 `EXPLAIN`으로 확인해야 한다. `INCREMENTAL` MV는 4.1.1에서 직접 조회 방식으로 사용한다.

### Q. 내 쿼리가 MV를 타는지 어떻게 확인하나요?

`EXPLAIN`의 `OlapScanNode`에서 `TABLE`이 MV 이름인지 확인한다. 성립하지 않으면 `TRACE REASON MV`를 사용한다.

### Q. MV 데이터는 실시간인가요?

아니다. External Iceberg MV는 metadata visibility, schedule wait, queue, refresh duration만큼 지연될 수 있다. COMMENT의 SLA와 `information_schema.materialized_views`의 version time을 확인한다.

### Q. Base와 MV 값이 다릅니다.

먼저 source snapshot과 MV가 반영한 version을 비교한다. 단순 refresh 시각만 보지 않는다. 동일한 source 시점을 비교했는데도 다르면 rewrite를 끈 별도 세션에서 7.3의 정합성 검증을 수행한다.

### Q. MV를 직접 조회해도 되나요?

가능하다. 다만 PCT rewrite용 MV는 base query를 권장한다. Direct query는 consumer가 MV schema와 freshness 상태에 직접 결합되므로 별도의 contract가 필요하다.

### Q. `partition_refresh_number`는 크게 잡을수록 좋은가요?

아니다. Batch를 키우면 task 수는 줄지만 peak memory와 scan 부하가 커질 수 있다. Partition 크기, refresh duration, queue, spill을 측정한 뒤 조정한다.

### Q. 수동 refresh에서 partition 범위를 지정하면 Iceberg MV도 그 범위만 갱신되나요?

공식 4.1 문서에는 external catalog MV를 수동 refresh할 때 모든 MV partition을 refresh한다는 경고가 있다. Production에서는 범위 제한을 전제로 비용을 계산하지 말고 현재 cluster에서 task run을 실측한다.

### Q. 권한은 무엇이 필요한가요?

공식 문서 기준으로 MV를 생성할 database의 `CREATE MATERIALIZED VIEW` privilege와 defining query가 참조하는 object의 `SELECT` privilege가 필요하다. UDF를 사용하면 해당 function의 `USAGE` privilege도 필요하며, 수동 refresh에는 대상 MV의 `REFRESH` privilege가 필요하다. 조직의 배포 role·승인 절차를 추가로 따른다.

---

## 11. 부록 A. MV 요청 양식

```text
[MV 목적]
- Transparent rewrite / Direct query:
- 대상 dashboard·job·service:
- 현재 p50/p95 latency와 목표:

[대표 쿼리]
- Query 1:
- Query 2:

[Freshness]
- 허용 source-to-query lag:
- 희망 refresh interval:
- 실패 시 fallback:

[원천]
- Catalog / database / table:
- Iceberg format version:
- Partition spec과 변경 이력:
- Write operations:
- Compaction / overwrite 정책:

[설계 후보]
- MV grain:
- Partition key:
- Distribution key:
- Sort key:
- Refresh mode와 선택 근거:

[운영]
- Owner / 문의 채널:
- 예상 호출량·동시성:
- 알림 기준:
- 폐기 기준:
```

---

## 12. 부록 B. 공식 참고 자료

- CREATE MATERIALIZED VIEW: https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW/
- REFRESH MATERIALIZED VIEW: https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW/
- SHOW MATERIALIZED VIEWS: https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/SHOW_MATERIALIZED_VIEW/
- Information Schema materialized_views: https://docs.starrocks.io/docs/sql-reference/information_schema/materialized_views/
- Query rewrite with materialized views: https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/
- Create a partitioned materialized view: https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/create_partitioned_materialized_view/
- Understand Materialized View Task Runs: https://docs.starrocks.io/docs/using_starrocks/async_mv/materialized_view_task_run_details/
- Iceberg Metadata Tables: https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_meta_table/
- StarRocks 4.1 release notes: https://docs.starrocks.io/releasenotes/release-4.1/

---

*검토 기준일: 2026-07-17. StarRocks 4.1.1 동작을 기준으로 작성했으며, Latest-4.1 문서에 후속 patch의 변경이 포함될 수 있으므로 업그레이드 시 INCREMENTAL rewrite, external manual refresh, Iceberg partition change detection을 다시 검증한다.*
