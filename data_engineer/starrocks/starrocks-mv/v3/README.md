# StarRocks Materialized View 문서

StarRocks 4.1 기준, Iceberg + Polaris(REST Catalog) 기반 Data Platform 환경의 MV 문서 모음.

| 문서 | 대상 | 내용 |
|---|---|---|
| [엔지니어링 심화 가이드](./starrocks-mv-deep-dive.md) | 플랫폼 운영자, 엔진 담당 엔지니어 | 아키텍처, Sync/Async, Refresh(PCT/IVM), Query Rewrite, Iceberg+Polaris 특화, 운영 원리, 검증 계획 |
| [사용자 가이드](./starrocks-mv-user-guide.md) | 플랫폼 사용자 (분석가·엔지니어) | 사용 기준, 생성 레시피, rewrite 사용법, 신선도, 검증 시나리오, 주의사항 |

- `images/`: 아키텍처 다이어그램 (PNG, 심화 가이드에서 참조)
- `dot/`: 다이어그램 원본 (Graphviz dot — 수정 후 `dot -Tpng -Gdpi=135 dot/NN.dot -o images/NN-*.png`로 재생성)
