# Half-cluster 4-way benchmark (8 data nodes, r7gd.8xlarge)

- FSST+ run: /local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/results/half_fsst_1778219623
- LZ4 run:   /local/home/hvamsi/code/OpenSearch/.claude/worktrees/omnissa-streaming/results/half_lz4_1778224390

| Query | cLZ4 | sLZ4 | Δ | cFSST+ | sFSST+ | Δ |
|---|---|---|---|---|---|---|
| cb_q08_advengine_terms | 6.9s | 6.5s | -7% | 6.8s | 6.9s | +1% |
| cb_q09_region_users | 21.3s | 20.0s | -6% | 20.5s | 20.3s | -1% |
| cb_q11_mobile_users | 11.5s | 10.8s | -7% | 11.0s | 11.1s | +2% |
| cb_q13_phrase_terms | 10.6s | 8.2s | -23% | 27.8s | 20.3s | -27% |
| cb_q14_phrase_users | ERR | ERR | - | ERR | ERR | - |
| cb_q16_top_users | 16.0s | 16.1s | +1% | 16.2s | 16.3s | +0% |
| cb_q_heavy_nested_urls | 80.4s | 81.4s | +1% | 104.3s | 109.5s | +5% |
| cbp_fm_urls_over_5 | 48.0s | 46.7s | -3% | 24.5s | 24.2s | -1% |
| cbp_heavy_nested_default | 110.2s | 89.1s | -19% | 116.7s | 103.5s | -11% |
| cbp_heavy_nested_map | 12.3s | 11.4s | -8% | 14.1s | 14.1s | +0% |
| cbp_multi_terms_deferred | 94.8s | 88.6s | -7% | 82.1s | 80.0s | -3% |
| cbp_nested_user_card | 25.9s | 26.1s | +1% | 26.0s | 25.1s | -4% |
| cbp_q07_min_max_eventdate | 501ms | 507ms | +1% | 516ms | 524ms | +2% |
| cbp_q10_region_stats | 17.1s | 16.7s | -2% | 16.7s | 16.7s | -0% |
| cbp_q23_title_search_cardinality | 12.7s | 12.5s | -2% | 40.1s | 39.4s | -2% |
| cbp_q31_engine_client_stats | 74.2s | 75.0s | +1% | 256.7s | 250.3s | -2% |
| cbp_q32_watch_client_stats | 78.0s | 78.9s | +1% | 262.6s | 257.8s | -2% |
| cbp_q33_watch_client_all | 274.3s | 278.9s | +2% | 265.1s | 277.8s | +5% |
| cbp_q34_url_popularity | 6.0s | 5.9s | -3% | 2.6s | 2.7s | +4% |
| cbp_q41_url_hash_date | 4.2s | 4.2s | +1% | 3.9s | 4.4s | +12% |
| cbp_q42_window_client_dims | 6.4s | 6.2s | -2% | 6.5s | 6.3s | -3% |
| cbp_q43_hourly_composite | 1.1s | 762ms | -28% | 1.2s | 883ms | -27% |
| cbp_tcc_urls_over_5 | 42.8s | 41.4s | -3% | 26.9s | 26.1s | -3% |
| om_device_guid_bigsize | ERR | ERR | - | ERR | ERR | - |
| om_fm_high_card_cardinality | 172.7s | 176.7s | +2% | 163.5s | 159.3s | -3% |
| om_fm_high_card_max_date | 8.7s | 8.5s | -2% | 9.0s | 8.8s | -2% |
| om_fm_low_card_cardinality | 5.7s | 5.5s | -4% | 5.5s | 5.1s | -8% |
| om_fm_low_card_max_date | 6.0s | 6.0s | -1% | 6.2s | 6.1s | -2% |
| om_multi_term_high_card | 50.8s | 39.0s | -23% | 48.3s | 38.0s | -21% |
| om_multi_term_high_card_max | 35.9s | 35.6s | -1% | 37.4s | 38.2s | +2% |
| om_multi_term_low_card | 61.0s | 61.8s | +1% | 49.4s | 49.7s | +1% |
| om_multi_term_low_card_max | 36.4s | 36.9s | +1% | 24.7s | 25.6s | +4% |
