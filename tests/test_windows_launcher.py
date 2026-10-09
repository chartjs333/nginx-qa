from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_run_bat_manages_quick_tunnel_around_server() -> None:
    launcher = (ROOT / "run.bat").read_text(encoding="utf-8")

    assert 'call :resolve_quick_tunnel_setting' in launcher
    assert 'call :start_quick_tunnel' in launcher
    assert launcher.count('call :stop_quick_tunnel') >= 2
    assert 'TELEGRAM_WEBHOOK_AUTO_REGISTER=1' in launcher
    assert '/api/v1/telegram/agents/%TELEGRAM_WEBHOOK_MODE%' in launcher
    assert 'cloudflared_quick_tunnel.ps1" -Action Start' in launcher
    assert 'cloudflared_quick_tunnel.ps1" -Action Stop' in launcher


def test_quick_tunnel_script_has_scoped_process_cleanup() -> None:
    helper = (ROOT / "cloudflared_quick_tunnel.ps1").read_text(encoding="utf-8")

    assert 'Cloudflare.cloudflared' in helper
    assert 'WindowStyle = "Hidden"' in helper
    assert '"--no-autoupdate"' in helper
    assert 'trycloudflare\\.com' in helper
    assert 'Resolve-DnsName' in helper
    assert 'Public DNS is ready' in helper
    assert 'process_ids = $managedProcessIds' in helper
    assert '$actualExecutable -ieq $expectedExecutable' in helper
    assert '[string]$process.CommandLine -like "*$expectedCommand*"' in helper


def test_env_example_documents_quick_tunnel_switch() -> None:
    example = (ROOT / ".env.example").read_text(encoding="utf-8")

    assert "CLOUDFLARED_QUICK_TUNNEL=1" in example
    assert "TELEGRAM_WEBHOOK_MODE=sequential" in example
