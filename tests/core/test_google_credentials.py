import json
import os
import pytest
from unittest import mock
from google.oauth2 import service_account
from core import _google_credentials


# -----------------------------
# _load_credentials
# -----------------------------
@mock.patch("core._google_credentials.service_account.Credentials.from_service_account_info")
def test_load_credentials_from_env_valid(mock_from_info, monkeypatch):
    fake_creds = mock.Mock()
    mock_from_info.return_value = fake_creds
    creds_dict = {"type": "service_account", "project_id": "test"}
    monkeypatch.setenv("GOOGLE_CREDENTIALS_JSON", json.dumps(creds_dict))

    result = _google_credentials._load_credentials()
    assert result == fake_creds
    mock_from_info.assert_called_once()


@mock.patch("core._google_credentials.service_account.Credentials.from_service_account_file")
def test_load_credentials_from_env_invalid_json(mock_from_file, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_JSON", "not-json")
    result = _google_credentials._load_credentials()
    mock_from_file.assert_called_once()
    assert result == mock_from_file.return_value


@mock.patch("core._google_credentials.service_account.Credentials.from_service_account_file")
def test_load_credentials_from_env_not_dict(mock_from_file, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_JSON", json.dumps(["not", "dict"]))
    result = _google_credentials._load_credentials()
    mock_from_file.assert_called_once()
    assert result == mock_from_file.return_value


@mock.patch("os.path.exists", return_value=True)
@mock.patch("core._google_credentials.service_account.Credentials.from_service_account_file")
def test_load_credentials_no_env(mock_from_file, mock_exists, monkeypatch):
    monkeypatch.delenv("GOOGLE_CREDENTIALS_JSON", raising=False)
    result = _google_credentials._load_credentials()
    mock_from_file.assert_called_once_with("credentials.json", scopes=mock.ANY)
    assert result == mock_from_file.return_value


@mock.patch("os.path.exists", return_value=True)
@mock.patch("core._google_credentials.service_account.Credentials.from_service_account_file")
def test_load_credentials_env_with_type_error(mock_from_file, mock_exists, monkeypatch):
    invalid_json = '{"type": "service_account", "project_id": 123'  # Missing closing brace
    monkeypatch.setenv("GOOGLE_CREDENTIALS_JSON", invalid_json)
    result = _google_credentials._load_credentials()
    mock_from_file.assert_called_once_with("credentials.json", scopes=mock.ANY)
    assert result == mock_from_file.return_value


# -----------------------------
# get_drive_client
# -----------------------------
@mock.patch("core._google_credentials.build")
@mock.patch("core._google_credentials._load_credentials")
def test_get_drive_client(mock_load, mock_build):
    mock_creds = mock.Mock()
    mock_load.return_value = mock_creds
    fake_service = mock.Mock()
    mock_build.return_value = fake_service

    result = _google_credentials.get_drive_client()
    mock_load.assert_called_once()
    mock_build.assert_called_once_with("drive", "v3", credentials=mock_creds)
    assert result == fake_service


# -----------------------------
# get_sheets_client
# -----------------------------
@mock.patch("core._google_credentials.build")
@mock.patch("core._google_credentials._load_credentials")
def test_get_sheets_client(mock_load, mock_build):
    mock_creds = mock.Mock()
    mock_load.return_value = mock_creds
    fake_service = mock.Mock()
    mock_build.return_value = fake_service

    result = _google_credentials.get_sheets_client()
    mock_load.assert_called_once()
    mock_build.assert_called_once_with("sheets", "v4", credentials=mock_creds)
    assert result == fake_service


# -----------------------------
# get_gspread_client
# -----------------------------
@mock.patch("core._google_credentials.gspread.authorize")
@mock.patch("core._google_credentials._load_credentials")
def test_get_gspread_client(mock_load, mock_auth):
    mock_creds = mock.Mock()
    mock_load.return_value = mock_creds
    fake_gspread = mock.Mock()
    mock_auth.return_value = fake_gspread

    result = _google_credentials.get_gspread_client()
    mock_load.assert_called_once()
    mock_auth.assert_called_once_with(mock_creds)
    assert result == fake_gspread


# -----------------------------
# Logging behavior
# -----------------------------
@mock.patch("core._google_credentials.service_account.Credentials.from_service_account_file")
@mock.patch("core._google_credentials.log.warning")
def test_load_credentials_logs_warning_for_invalid_json(mock_warn, mock_file, monkeypatch):
    monkeypatch.setenv("GOOGLE_CREDENTIALS_JSON", "{invalid}")
    _google_credentials._load_credentials()
    mock_warn.assert_called()
    mock_file.assert_called_once()