# Full vs Half cluster 4-way benchmark

- Full cluster: 16× r7gd.8xlarge data nodes (prior runs from sessions 3-8)
- Half cluster: 8× r7gd.8xlarge data nodes (this session)
- All runs: fleet GC before each query, classic `stream.search.enabled=false` vs streaming `stream.search.enabled=true, arrow_columnar.enabled=true`
- Key column **"s-half/c-full"**: streaming latency on half cluster vs classic latency on full cluster (LZ4). Negative = streaming on half-cluster **matches or beats** classic on full cluster.

| Query | cLZ4 full | cLZ4 half | sLZ4 full | sLZ4 half | cFSST+ full | cFSST+ half | sFSST+ full | sFSST+ half | s-half/c-full |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| om_multi_term_low_card | 46.2s | 61.0s | 42.5s | 61.8s | 38.5s | 49.4s | 37.8s | 49.7s | +34% |
| om_multi_term_high_card | 24.8s | 50.8s | 19.9s | 39.0s | 24.6s | 48.3s | 19.5s | 38.0s | +57% |
| om_multi_term_low_card_max | 19.8s | 36.4s | 18.9s | 36.9s | 14.4s | 24.7s | 13.7s | 25.6s | +86% |
| om_multi_term_high_card_max | 16.6s | 35.9s | 16.6s | 35.6s | 16.5s | 37.4s | 16.2s | 38.2s | +114% |
| om_fm_high_card_cardinality | 150.8s | 172.7s | 98.8s | 176.7s | 208.3s | 163.5s | 95.0s | 159.3s | +17% |
| om_fm_low_card_cardinality | 2.4s | 5.7s | - | 5.5s | 2.4s | 5.5s | - | 5.1s | +130% |
| om_fm_high_card_max_date | 4.3s | 8.7s | - | 8.5s | 4.1s | 9.0s | - | 8.8s | +98% |
| om_fm_low_card_max_date | 2.8s | 6.0s | - | 6.0s | - | 6.2s | 3.1s | 6.1s | +115% |
| om_device_guid_bigsize | - | ERR | - | ERR | - | ERR | - | ERR | - |
| cbp_heavy_nested_default | 83.7s | 110.2s | 36.0s | 89.1s | 105.5s | 116.7s | 43.6s | 103.5s | +6% |
| cbp_heavy_nested_map | 7.2s | 12.3s | - | 11.4s | 8.2s | 14.1s | 5.2s | 14.1s | +58% |
| cbp_multi_terms_deferred | 39.3s | 94.8s | 45.8s | 88.6s | 42.7s | 82.1s | 40.7s | 80.0s | +125% |
| cbp_nested_user_card | 16.1s | 25.9s | 11.0s | 26.1s | 16.1s | 26.0s | 10.7s | 25.1s | +62% |
| cbp_tcc_urls_over_5 | 22.9s | 42.8s | - | 41.4s | 14.1s | 26.9s | 14.4s | 26.1s | +81% |
| cbp_fm_urls_over_5 | 26.1s | 48.0s | 27.2s | 46.7s | 12.2s | 24.5s | 11.9s | 24.2s | +79% |
| cbp_q07_min_max_eventdate | 50ms | 501ms | - | 507ms | 50ms | 516ms | 50ms | 524ms | +914% |
| cbp_q10_region_stats | 8.4s | 17.1s | - | 16.7s | 7.8s | 16.7s | 8.8s | 16.7s | +99% |
| cbp_q23_title_search_cardinality | 5.9s | 12.7s | 18.2s | 12.5s | 24.6s | 40.1s | 21.7s | 39.4s | +112% |
| cbp_q31_engine_client_stats | 35.9s | 74.2s | - | 75.0s | - | 256.7s | 128.7s | 250.3s | +109% |
| cbp_q32_watch_client_stats | 38.5s | 78.0s | - | 78.9s | - | 262.6s | 130.9s | 257.8s | +105% |
| cbp_q33_watch_client_all | 137.0s | 274.3s | 140.1s | 278.9s | 138.4s | 265.1s | 137.2s | 277.8s | +104% |
| cbp_q34_url_popularity | 2.8s | 6.0s | - | 5.9s | 1.5s | 2.6s | 4.9s | 2.7s | +110% |
| cbp_q41_url_hash_date | 1.7s | 4.2s | - | 4.2s | - | 3.9s | 1.8s | 4.4s | +147% |
| cbp_q42_window_client_dims | 2.9s | 6.4s | - | 6.2s | 2.5s | 6.5s | 3.0s | 6.3s | +115% |
| cbp_q43_hourly_composite | 300ms | 1.1s | - | 762ms | 200ms | 1.2s | 200ms | 883ms | +154% |
| cb_q08_advengine_terms | 2.4s | 6.9s | - | 6.5s | 2.7s | 6.8s | 2.9s | 6.9s | +169% |
| cb_q09_region_users | 9.5s | 21.3s | - | 20.0s | - | 20.5s | 10.0s | 20.3s | +110% |
| cb_q11_mobile_users | 5.2s | 11.5s | - | 10.8s | 5.0s | 11.0s | 5.2s | 11.1s | +107% |
| cb_q13_phrase_terms | 4.5s | 10.6s | - | 8.2s | 13.5s | 27.8s | 12.5s | 20.3s | +83% |
| cb_q14_phrase_users | - | ERR | - | ERR | - | ERR | - | ERR | - |
| cb_q16_top_users | 7.6s | 16.0s | - | 16.1s | 7.6s | 16.2s | 7.8s | 16.3s | +112% |
| cb_q_heavy_nested_urls | 69.2s | 80.4s | - | 81.4s | 94.1s | 104.3s | 42.2s | 109.5s | +18% |
