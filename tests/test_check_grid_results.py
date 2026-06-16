import csv
import json

from check_grid_results import load_state_files, load_trade_counts, parse_param_key, split_symbol_param


def test_parse_cj_param_key_keeps_scientific_notation():
    params = parse_param_key(
        "cj_dyn_asymmetric_lp0.3_lm0.3_kp0.05_km0.05_ep7.0_em7.0_a0.001_ph5e-05_sm2.0_vs0.1_ls0.7_ks1.2_es1.3_ss1.0_c0.12_l2"
    )

    assert params["quote_engine"] == "CJ"
    assert params["lambda_plus"] == 0.3
    assert params["kappa_plus"] == 0.05
    assert params["epsilon_plus"] == 7.0
    assert params["phi"] == 5e-05
    assert params["spread_multiplier"] == 2.0
    assert params["volatility_spread_multiplier"] == 0.1
    assert params["lambda_scale"] == 0.7
    assert params["kappa_scale"] == 1.2
    assert params["epsilon_scale"] == 1.3
    assert params["sigma2_scale"] == 1.0
    assert params["num_levels"] == 2


def test_split_symbol_param_supports_obi_and_cj_keys():
    assert split_symbol_param("BTC_v8_m4_s0.1") == ("BTC", "v8_m4_s0.1")
    assert split_symbol_param("BTC_cj_lp0.3_kp0.05") == ("BTC", "cj_lp0.3_kp0.05")


def test_loaders_group_cj_state_and_trades_by_param_key(tmp_path):
    param_key = "cj_lp0.3_lm0.3_kp0.05_km0.05_ep7.0_em7.0_ph5e-05_sm2.0_l2"
    state_path = tmp_path / f"state_BTC_{param_key}.json"
    trade_path = tmp_path / f"trades_BTC_{param_key}.csv"

    state_path.write_text(
        json.dumps(
            {
                "realized_pnl": 1.23,
                "portfolio_value": 101.23,
                "initial_portfolio_value": 100.0,
                "fill_count": 2,
            }
        )
    )
    with trade_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "side", "price", "size"])
        writer.writerow(["2026-06-16T12:00:00Z", "buy", "66000", "0.001"])
        writer.writerow(["2026-06-16T12:01:00Z", "sell", "66010", "0.001"])

    slots = load_state_files(str(tmp_path))
    counts = load_trade_counts(str(tmp_path))

    assert len(slots) == 1
    assert slots[0]["symbol"] == "BTC"
    assert slots[0]["param_key"] == param_key
    assert slots[0]["params"]["quote_engine"] == "CJ"
    assert counts[param_key] == 2
