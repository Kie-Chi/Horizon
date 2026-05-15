from __future__ import annotations

import asyncio
import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import httpx
from rich.console import Console

from src.models import (
    AIConfig,
    CVEConfig,
    CVEProviderConfig,
    CVEProviderType,
    Config,
    FilteringConfig,
    HackerNewsConfig,
    SourcesConfig,
    SourceType,
)
from src.orchestrator import HorizonOrchestrator
from src.scrapers.cve import CVEScraper, NVD_MAX_TIME_WINDOW_DAYS
from src.storage.manager import StorageManager


def _provider(type_: CVEProviderType, **overrides) -> CVEProviderConfig:
    base = {
        "type": type_,
        "enabled": True,
        "min_cvss": None,
        "keywords": [],
        "vendors": [],
        "products": [],
    }
    base.update(overrides)
    return CVEProviderConfig(**base)


def _cve_config(
    *providers: CVEProviderConfig,
    enabled: bool = True,
    nvd_api_key_env: str = None,
    keywords: list[str] | None = None,
    vendors: list[str] | None = None,
    products: list[str] | None = None,
) -> CVEConfig:
    return CVEConfig(
        enabled=enabled,
        keywords=keywords or [],
        vendors=vendors or [],
        products=products or [],
        providers=list(providers),
        nvd_api_key_env=nvd_api_key_env,
    )


def _make_scraper(
    config: CVEConfig,
    responses: list[httpx.Response],
    storage: StorageManager | None = None,
    console: Console | None = None,
) -> CVEScraper:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.get.side_effect = responses
    return CVEScraper(config, client, storage=storage, console=console)


def _kev_response(entries: list[dict]) -> httpx.Response:
    return httpx.Response(
        200,
        json={"title": "Known Exploited Vulnerabilities", "vulnerabilities": entries},
        request=httpx.Request("GET", "https://example.com/kev"),
    )


def _atom_response(entries: list[tuple[str, str]]) -> httpx.Response:
    feed_entries = "".join(
        f"""
        <entry>
          <id>tag:{tag}</id>
          <updated>{updated}</updated>
          <link href="https://github.com/CVEProject/cvelistV5/releases/tag/{tag}" />
        </entry>
        """
        for tag, updated in entries
    )
    xml = f"""
    <feed xmlns="http://www.w3.org/2005/Atom">
      {feed_entries}
    </feed>
    """
    return httpx.Response(
        200,
        text=xml,
        request=httpx.Request("GET", "https://github.com/CVEProject/cvelistV5/releases.atom"),
    )


def _nvd_api_response(
    entries: list[dict],
    total_results: int = None,
    start_index: int = 0,
    results_per_page: int | None = None,
) -> httpx.Response:
    """Simulate NVD API 2.0 JSON response (not gzipped)."""
    total = total_results if total_results is not None else len(entries)
    page_size = results_per_page if results_per_page is not None else len(entries)
    return httpx.Response(
        200,
        json={
            "resultsPerPage": page_size,
            "startIndex": start_index,
            "totalResults": total,
            "vulnerabilities": [{"cve": entry} for entry in entries],
        },
        request=httpx.Request("GET", "https://services.nvd.nist.gov/rest/json/cves/2.0"),
    )


def _cvelist_zip_response(entries: list[dict]) -> httpx.Response:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for entry in entries:
            cve_id = entry["cveMetadata"]["cveId"]
            archive.writestr(
                f"deltaCves/{cve_id}.json",
                json.dumps(entry),
            )
    return httpx.Response(
        200,
        content=buffer.getvalue(),
        request=httpx.Request("GET", "https://github.com/CVEProject/cvelistV5/releases/download/example.zip"),
    )


def _ghsa_response(entries: list[dict], next_url: str | None = None) -> httpx.Response:
    headers = {}
    if next_url:
        headers["Link"] = f'<{next_url}>; rel="next"'
    return httpx.Response(
        200,
        json=entries,
        headers=headers,
        request=httpx.Request("GET", "https://api.github.com/advisories"),
    )


def _kev_entry(**overrides) -> dict:
    base = {
        "cveID": "CVE-2026-0001",
        "vendorProject": "Linux Kernel",
        "product": "Kernel",
        "vulnerabilityName": "Use-after-free in netfilter",
        "shortDescription": "A remote attacker could trigger a kernel panic.",
        "requiredAction": "Apply vendor patch.",
        "knownRansomwareCampaignUse": "Unknown",
        "dateAdded": "2026-05-13",
        "dueDate": "2026-06-03",
        "references": ["https://nvd.nist.gov/vuln/detail/CVE-2026-0001"],
    }
    base.update(overrides)
    return base


def _nvd_entry(**overrides) -> dict:
    base = {
        "id": "CVE-2026-0001",
        "published": "2026-05-13T12:00:00.000",
        "lastModified": "2026-05-14T01:00:00.000",
        "descriptions": [{"lang": "en", "value": "OpenSSL certificate validation bypass."}],
        "metrics": {
            "cvssMetricV31": [
                {
                    "cvssData": {
                        "baseScore": 8.8,
                        "vectorString": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
                    },
                    "baseSeverity": "HIGH",
                }
            ]
        },
        "weaknesses": [{"description": [{"lang": "en", "value": "CWE-295"}]}],
        "references": [{"url": "https://vendor.example/advisory"}],
        "configurations": [
            {
                "nodes": [
                    {
                        "cpeMatch": [
                            {
                                "criteria": "cpe:2.3:a:openssl:openssl:*:*:*:*:*:*:*:*",
                            }
                        ]
                    }
                ]
            }
        ],
    }
    base.update(overrides)
    return base


def _cvelist_entry(**overrides) -> dict:
    base = {
        "cveMetadata": {
            "cveId": "CVE-2026-0100",
            "datePublished": "2026-05-13T12:00:00.000Z",
            "dateUpdated": "2026-05-14T01:00:00.000Z",
        },
        "containers": {
            "cna": {
                "descriptions": [
                    {"lang": "en", "value": "OpenSSL certificate validation bypass."}
                ],
                "affected": [
                    {
                        "vendor": "openssl",
                        "product": "openssl",
                    }
                ],
                "references": [{"url": "https://vendor.example/advisory"}],
                "problemTypes": [
                    {
                        "descriptions": [
                            {"lang": "en", "cweId": "CWE-295", "description": "Improper Certificate Validation"}
                        ]
                    }
                ],
                "metrics": [
                    {
                        "cvssV3_1": {
                            "baseScore": 8.8,
                            "vectorString": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
                            "baseSeverity": "HIGH",
                        }
                    }
                ],
            }
        },
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = {**base[key], **value}
        else:
            base[key] = value
    return base


def _ghsa_entry(**overrides) -> dict:
    base = {
        "ghsa_id": "GHSA-abcd-efgh-ijkl",
        "cve_id": "CVE-2026-0001",
        "published_at": "2026-05-13T12:00:00Z",
        "updated_at": "2026-05-14T01:00:00Z",
        "summary": "OpenSSL certificate validation bypass.",
        "description": "A crafted certificate chain could bypass validation.",
        "severity": "high",
        "html_url": "https://github.com/advisories/GHSA-abcd-efgh-ijkl",
        "cvss": {
            "score": 8.8,
            "vector_string": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
        },
        "cwes": [{"cwe_id": "CWE-295", "name": "Improper Certificate Validation"}],
        "references": [{"url": "https://github.com/advisories/GHSA-abcd-efgh-ijkl"}],
        "vulnerabilities": [
            {
                "package": {
                    "ecosystem": "pip",
                    "name": "openssl-helper",
                }
            }
        ],
    }
    base.update(overrides)
    return base


class TestCVEKevMapping:
    def test_maps_kev_entry_to_content_item(self):
        provider = _provider(CVEProviderType.CISA_KEV)
        scraper = _make_scraper(_cve_config(provider), [_kev_response([_kev_entry()])])
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        item = result[0]
        assert item.source_type == SourceType.CVE
        assert item.id == "cve:kev:CVE-2026-0001"
        assert item.author == "CISA KEV"
        assert item.metadata["kev"] is True
        assert item.metadata["vendor"] == "Linux Kernel"


class TestCVENvdMapping:
    def test_maps_nvd_recent_entry_to_content_item(self):
        provider = _provider(CVEProviderType.NVD_RECENT)
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([_nvd_entry()])])
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        item = result[0]
        assert item.id == "cve:nvd:CVE-2026-0001"
        assert item.metadata["cvss"] == 8.8
        assert item.metadata["cwe"] == "CWE-295"
        assert item.metadata["vendors"] == ["openssl"]
        assert item.metadata["products"] == ["openssl"]
        assert item.source_type == SourceType.CVE

    def test_maps_cvelist_delta_entry_to_content_item(self):
        provider = _provider(CVEProviderType.CVELIST_V5_DELTA)
        scraper = _make_scraper(
            _cve_config(provider),
            [
                _atom_response([("cve_2026-05-14_0700Z", "2026-05-14T07:00:00Z")]),
                _cvelist_zip_response([_cvelist_entry()]),
            ],
        )
        since = datetime(2026, 5, 13, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        item = result[0]
        assert item.id == "cve:cvelist:CVE-2026-0100"
        assert item.author == "CVE List V5"
        assert item.metadata["provider"] == "cvelist_v5_delta"
        assert item.metadata["cvss"] == 8.8
        assert item.metadata["cwe"] == "CWE-295"

    def test_cvelist_retries_next_hour_asset_name_when_expected_name_404s(self):
        provider = _provider(CVEProviderType.CVELIST_V5_DELTA)
        tag = "cve_2026-05-14_0800Z"
        scraper = _make_scraper(
            _cve_config(provider),
            [
                _atom_response([(tag, "2026-05-14T08:00:00Z")]),
                httpx.Response(
                    404,
                    request=httpx.Request(
                        "GET",
                        f"https://github.com/CVEProject/cvelistV5/releases/download/{tag}/2026-05-14_delta_CVEs_at_0800Z.zip",
                    ),
                ),
                _cvelist_zip_response([_cvelist_entry()]),
            ],
        )
        since = datetime(2026, 5, 13, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        assert result[0].metadata["provider"] == "cvelist_v5_delta"
        requests = [str(call.args[0]) for call in scraper.client.get.await_args_list]
        assert requests[1].endswith(f"/{tag}/2026-05-14_delta_CVEs_at_0800Z.zip")
        assert requests[2].endswith(f"/{tag}/2026-05-14_delta_CVEs_at_0900Z.zip")

    def test_maps_ghsa_entry_to_content_item(self):
        provider = _provider(CVEProviderType.GHSA)
        scraper = _make_scraper(_cve_config(provider), [_ghsa_response([_ghsa_entry()])])
        since = datetime(2026, 5, 13, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        item = result[0]
        assert item.id == "cve:ghsa:CVE-2026-0001"
        assert item.author == "GitHub Advisory Database"
        assert item.metadata["provider"] == "ghsa"
        assert item.metadata["ghsa_id"] == "GHSA-abcd-efgh-ijkl"
        assert item.metadata["cvss"] == 8.8
        assert item.metadata["cwe"] == "CWE-295"

    def test_ghsa_tolerates_non_dict_nested_entries(self):
        provider = _provider(CVEProviderType.GHSA)
        scraper = _make_scraper(
            _cve_config(provider),
            [
                _ghsa_response(
                    [
                        _ghsa_entry(
                            vulnerabilities=["pip"],
                            cwes=["CWE-295"],
                            references=["https://github.com/advisories/GHSA-abcd-efgh-ijkl"],
                        )
                    ]
                )
            ],
        )
        since = datetime(2026, 5, 13, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        assert result[0].metadata["provider"] == "ghsa"


class TestCVEFiltering:
    def test_filters_old_entries_by_since(self):
        provider = _provider(CVEProviderType.CISA_KEV)
        scraper = _make_scraper(
            _cve_config(provider),
            [_kev_response([_kev_entry(dateAdded="2026-05-10")])],
        )
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert asyncio.run(scraper.fetch(since)) == []

    def test_nvd_modified_filters_by_last_modified(self):
        provider = _provider(CVEProviderType.NVD_MODIFIED)
        entry = _nvd_entry(
            published="2026-05-01T00:00:00.000",
            lastModified="2026-05-14T05:00:00.000",
        )
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([entry])])
        since = datetime(2026, 5, 13, tzinfo=timezone.utc)
        assert len(asyncio.run(scraper.fetch(since))) == 1

    def test_filters_nvd_by_min_cvss(self):
        provider = _provider(CVEProviderType.NVD_RECENT, min_cvss=7.0)
        entry = _nvd_entry(
            id="CVE-2026-0002",
            metrics={"cvssMetricV31": [{"cvssData": {"baseScore": 6.0}}]},
        )
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([entry])])
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert asyncio.run(scraper.fetch(since)) == []

    def test_filters_nvd_missing_cvss_when_threshold_configured(self):
        provider = _provider(CVEProviderType.NVD_RECENT, min_cvss=7.0)
        entry = _nvd_entry(id="CVE-2026-0003", metrics={})
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([entry])])
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert asyncio.run(scraper.fetch(since)) == []

    def test_keyword_filter_matches_description(self):
        provider = _provider(CVEProviderType.NVD_RECENT, keywords=["certificate"])
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([_nvd_entry()])])
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert len(asyncio.run(scraper.fetch(since))) == 1

    def test_global_keyword_filter_matches_description(self):
        provider = _provider(CVEProviderType.NVD_RECENT)
        scraper = _make_scraper(
            _cve_config(provider, keywords=["certificate"]),
            [_nvd_api_response([_nvd_entry()])],
        )
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert len(asyncio.run(scraper.fetch(since))) == 1

    def test_provider_keywords_append_to_global_defaults(self):
        provider = _provider(CVEProviderType.NVD_RECENT, keywords=["openssl"])
        scraper = _make_scraper(
            _cve_config(provider, keywords=["certificate"]),
            [_nvd_api_response([_nvd_entry()])],
        )
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert len(asyncio.run(scraper.fetch(since))) == 1

    def test_global_vendor_filter_applies_to_provider(self):
        provider = _provider(CVEProviderType.NVD_RECENT)
        scraper = _make_scraper(
            _cve_config(provider, vendors=["openssl"]),
            [_nvd_api_response([_nvd_entry()])],
        )
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert len(asyncio.run(scraper.fetch(since))) == 1

    def test_vendor_and_product_filters_and_with_keywords(self):
        provider = _provider(
            CVEProviderType.NVD_RECENT,
            keywords=["OpenSSL"],
            vendors=["openssl"],
            products=["openssl"],
        )
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([_nvd_entry()])])
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert len(asyncio.run(scraper.fetch(since))) == 1

    def test_filters_ghsa_by_min_cvss(self):
        provider = _provider(CVEProviderType.GHSA, min_cvss=9.0)
        scraper = _make_scraper(_cve_config(provider), [_ghsa_response([_ghsa_entry()])])
        since = datetime(2026, 5, 13, tzinfo=timezone.utc)
        assert asyncio.run(scraper.fetch(since)) == []

class TestCVEDeduplication:
    def test_deduplicates_recent_and_modified_same_cve(self):
        recent = _provider(CVEProviderType.NVD_RECENT)
        modified = _provider(CVEProviderType.NVD_MODIFIED)
        entry_recent = _nvd_entry()
        entry_modified = _nvd_entry(
            descriptions=[{"lang": "en", "value": "Richer description"}],
            references=[{"url": "https://example.com/a"}, {"url": "https://example.com/b"}],
        )
        scraper = _make_scraper(
            _cve_config(recent, modified),
            [_nvd_api_response([entry_recent]), _nvd_api_response([entry_modified])],
        )
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        assert result[0].metadata["provider"] == "nvd_recent"

    def test_kev_takes_priority_and_merges_nvd_metadata(self):
        kev = _provider(CVEProviderType.CISA_KEV)
        recent = _provider(CVEProviderType.NVD_RECENT)
        scraper = _make_scraper(
            _cve_config(kev, recent),
            [_kev_response([_kev_entry()]), _nvd_api_response([_nvd_entry()])],
        )
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        item = result[0]
        assert item.metadata["provider"] == "cisa_kev"
        assert item.metadata["cvss"] == 8.8
        assert "references" in item.metadata

    def test_cvelist_merges_with_nvd_by_cve_id(self):
        cvelist = _provider(CVEProviderType.CVELIST_V5_DELTA)
        recent = _provider(CVEProviderType.NVD_RECENT)
        client = AsyncMock(spec=httpx.AsyncClient)

        async def respond(url, **kwargs):
            if str(url).endswith("releases.atom"):
                return _atom_response([("cve_2026-05-14_0700Z", "2026-05-14T07:00:00Z")])
            if "releases/download" in str(url):
                return _cvelist_zip_response([_cvelist_entry()])
            return _nvd_api_response([_nvd_entry(id="CVE-2026-0100")])

        client.get.side_effect = respond
        scraper = CVEScraper(_cve_config(cvelist, recent), client)
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        assert result[0].metadata["cve_id"] == "CVE-2026-0100"

    def test_keeps_ghsa_only_advisory_without_cve_alias(self):
        ghsa = _provider(CVEProviderType.GHSA)
        scraper = _make_scraper(
            _cve_config(ghsa),
            [_ghsa_response([_ghsa_entry(cve_id=None)])],
        )
        since = datetime(2026, 5, 13, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        assert result[0].metadata["ghsa_id"] == "GHSA-abcd-efgh-ijkl"

class TestCVEResilience:
    def test_cvelist_release_failure_does_not_block_later_releases(self):
        provider = _provider(CVEProviderType.CVELIST_V5_DELTA)
        tag_one = "cve_2026-05-14_0800Z"
        tag_two = "cve_2026-05-14_0900Z"
        scraper = _make_scraper(
            _cve_config(provider),
            [
                _atom_response(
                    [
                        (tag_one, "2026-05-14T08:00:00Z"),
                        (tag_two, "2026-05-14T09:00:00Z"),
                    ]
                ),
                httpx.Response(
                    404,
                    request=httpx.Request(
                        "GET",
                        f"https://github.com/CVEProject/cvelistV5/releases/download/{tag_one}/2026-05-14_delta_CVEs_at_0800Z.zip",
                    ),
                ),
                httpx.Response(
                    404,
                    request=httpx.Request(
                        "GET",
                        f"https://github.com/CVEProject/cvelistV5/releases/download/{tag_one}/2026-05-14_delta_CVEs_at_0900Z.zip",
                    ),
                ),
                _cvelist_zip_response(
                    [
                        _cvelist_entry(
                            cveMetadata={
                                "cveId": "CVE-2026-0200",
                                "datePublished": "2026-05-14T09:10:00.000Z",
                                "dateUpdated": "2026-05-14T09:10:00.000Z",
                            }
                        )
                    ]
                ),
            ],
        )
        since = datetime(2026, 5, 13, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        assert result[0].metadata["cve_id"] == "CVE-2026-0200"

    def test_provider_failure_does_not_block_others(self):
        kev = _provider(CVEProviderType.CISA_KEV)
        recent = _provider(CVEProviderType.NVD_RECENT)
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.side_effect = [
            httpx.HTTPError("boom"),
            _nvd_api_response([_nvd_entry()]),
        ]
        scraper = CVEScraper(_cve_config(kev, recent), client)
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert len(result) == 1
        assert result[0].metadata["provider"] == "nvd_recent"

    def test_bad_dates_are_skipped(self):
        provider = _provider(CVEProviderType.CISA_KEV)
        scraper = _make_scraper(
            _cve_config(provider),
            [_kev_response([_kev_entry(dateAdded="not-a-date")])],
        )
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert asyncio.run(scraper.fetch(since)) == []


class TestCVECaching:
    def test_sends_conditional_headers_from_cached_state(self, tmp_path):
        storage = StorageManager(data_dir=str(tmp_path))
        storage.save_scraper_state("cve", {
            "providers": {
                "cisa_kev": {
                    "etag": '"abc"',
                    "last_modified_header": "Wed, 14 May 2026 00:00:00 GMT",
                }
            }
        })
        provider = _provider(CVEProviderType.CISA_KEV)
        response = _kev_response([_kev_entry()])
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = response
        scraper = CVEScraper(_cve_config(provider), client, storage=storage)
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        asyncio.run(scraper.fetch(since))

        headers = client.get.call_args.kwargs["headers"]
        assert headers["If-None-Match"] == '"abc"'
        assert headers["If-Modified-Since"] == "Wed, 14 May 2026 00:00:00 GMT"

    def test_returns_empty_on_304_and_preserves_state(self, tmp_path):
        storage = StorageManager(data_dir=str(tmp_path))
        provider = _provider(CVEProviderType.CISA_KEV)
        response = httpx.Response(
            304,
            request=httpx.Request("GET", "https://example.com/kev"),
        )
        scraper = _make_scraper(_cve_config(provider), [response], storage=storage)
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))

        assert result == []
        saved = storage.load_scraper_state("cve")
        assert saved["providers"]["cisa_kev"]["cache"] == {}

    def test_migrates_legacy_flat_provider_state(self, tmp_path):
        storage = StorageManager(data_dir=str(tmp_path))
        storage.save_scraper_state("cve", {
            "providers": {
                "cisa_kev": {
                    "etag": '"abc"',
                    "last_modified_header": "Wed, 14 May 2026 00:00:00 GMT",
                    "last_success_at": "2026-05-14T00:00:00+00:00",
                }
            }
        })
        provider = _provider(CVEProviderType.CISA_KEV)
        scraper = _make_scraper(_cve_config(provider), [_kev_response([_kev_entry()])], storage=storage)

        asyncio.run(scraper.fetch(datetime(2026, 5, 12, tzinfo=timezone.utc)))

        saved = storage.load_scraper_state("cve")
        assert saved["providers"]["cisa_kev"]["cache"]["etag"] == '"abc"'
        assert saved["providers"]["cisa_kev"]["runtime"]["last_success_at"]


class TestCVEListDelta:
    def test_skips_already_processed_release(self, tmp_path):
        storage = StorageManager(data_dir=str(tmp_path))
        storage.save_scraper_state("cve", {
            "providers": {
                "cvelist_v5_delta": {
                    "cursor": {
                        "last_release_tag": "cve_2026-05-14_0700Z",
                        "last_release_updated": "2026-05-14T07:00:00+00:00",
                    }
                }
            }
        })
        provider = _provider(CVEProviderType.CVELIST_V5_DELTA)
        scraper = _make_scraper(
            _cve_config(provider),
            [_atom_response([("cve_2026-05-14_0700Z", "2026-05-14T07:00:00Z")])],
            storage=storage,
        )

        result = asyncio.run(scraper.fetch(datetime(2026, 5, 13, tzinfo=timezone.utc)))

        assert result == []


class TestNvdApiFeatures:
    def test_time_window_exceeds_120_days_returns_empty(self):
        """When the time window exceeds 120 days, NVD API provider is skipped."""
        provider = _provider(CVEProviderType.NVD_RECENT)
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([_nvd_entry()])])
        # Set since to more than 120 days ago.
        since = datetime.now(timezone.utc) - timedelta(days=200)

        result = asyncio.run(scraper.fetch(since))
        assert result == []

    def test_api_key_header_sent_when_configured(self, monkeypatch):
        """When nvd_api_key_env is set and env var exists, apiKey header is sent."""
        monkeypatch.setenv("NVD_TEST_KEY", "test-key-123")
        provider = _provider(CVEProviderType.NVD_RECENT)
        config = _cve_config(provider, nvd_api_key_env="NVD_TEST_KEY")
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = _nvd_api_response([_nvd_entry()])
        scraper = CVEScraper(config, client)
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        asyncio.run(scraper.fetch(since))

        # The last call should be the NVD API call (KEV might not be present).
        for call in client.get.call_args_list:
            headers = call.kwargs.get("headers", {})
            if "apiKey" in headers:
                assert headers["apiKey"] == "test-key-123"
                return
        # If only NVD provider, the call should have apiKey.
        nvd_calls = [
            c for c in client.get.call_args_list
            if "apiKey" in c.kwargs.get("headers", {})
        ]
        assert len(nvd_calls) > 0

    def test_ghsa_uses_github_token_when_available(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "gh-test-token")
        provider = _provider(CVEProviderType.GHSA)
        client = AsyncMock(spec=httpx.AsyncClient)
        client.get.return_value = _ghsa_response([_ghsa_entry()])
        scraper = CVEScraper(_cve_config(provider), client)

        asyncio.run(scraper.fetch(datetime(2026, 5, 13, tzinfo=timezone.utc)))

        headers = client.get.call_args.kwargs["headers"]
        assert headers["Authorization"] == "token gh-test-token"

    def test_fetches_multiple_pages_when_more_results_exist(self):
        """When totalResults exceeds one page, the scraper should continue paging."""
        provider = _provider(CVEProviderType.NVD_RECENT)
        first_entry = _nvd_entry(id="CVE-2026-0001")
        second_entry = _nvd_entry(id="CVE-2026-0002")
        scraper = _make_scraper(
            _cve_config(provider),
            [
                _nvd_api_response(
                    [first_entry],
                    total_results=2,
                    start_index=0,
                    results_per_page=1,
                ),
                _nvd_api_response(
                    [second_entry],
                    total_results=2,
                    start_index=1,
                    results_per_page=1,
                ),
            ],
        )
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)

        result = asyncio.run(scraper.fetch(since))
        assert len(result) == 2

    def test_cvss_severity_mapping(self):
        """Verify min_cvss float maps to correct API severity level."""
        assert CVEScraper._cvss_to_severity(9.5) == "CRITICAL"
        assert CVEScraper._cvss_to_severity(9.0) == "CRITICAL"
        assert CVEScraper._cvss_to_severity(8.0) == "HIGH"
        assert CVEScraper._cvss_to_severity(7.0) == "HIGH"
        assert CVEScraper._cvss_to_severity(5.0) == "MEDIUM"
        assert CVEScraper._cvss_to_severity(4.0) == "MEDIUM"
        assert CVEScraper._cvss_to_severity(1.0) == "LOW"
        assert CVEScraper._cvss_to_severity(0.1) == "LOW"
        assert CVEScraper._cvss_to_severity(0.0) is None

    def test_format_nvd_date(self):
        """Verify NVD API date formatting."""
        dt = datetime(2026, 5, 13, 12, 30, 45, tzinfo=timezone.utc)
        assert CVEScraper._format_nvd_date(dt) == "2026-05-13T12:30:45.000+00:00"

    def test_nvd_api_params_for_recent(self):
        """Verify API params built correctly for nvd_recent provider."""
        provider = _provider(CVEProviderType.NVD_RECENT)
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([])])
        since = datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc)
        now = datetime(2026, 5, 13, 0, 0, 0, tzinfo=timezone.utc)

        params = scraper._build_nvd_api_params(provider, since, now)
        assert "pubStartDate" in params
        assert "pubEndDate" in params
        assert "lastModStartDate" not in params
        assert "lastModEndDate" not in params

    def test_nvd_api_params_for_modified(self):
        """Verify API params built correctly for nvd_modified provider."""
        provider = _provider(CVEProviderType.NVD_MODIFIED)
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([])])
        since = datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc)
        now = datetime(2026, 5, 13, 0, 0, 0, tzinfo=timezone.utc)

        params = scraper._build_nvd_api_params(provider, since, now)
        assert "lastModStartDate" in params
        assert "lastModEndDate" in params
        assert "pubStartDate" not in params
        assert "pubEndDate" not in params

    def test_nvd_api_params_includes_severity_when_min_cvss_set(self):
        """Verify cvssV3Severity is included when min_cvss is configured."""
        provider = _provider(CVEProviderType.NVD_RECENT, min_cvss=7.0)
        scraper = _make_scraper(_cve_config(provider), [_nvd_api_response([])])
        since = datetime(2026, 5, 12, tzinfo=timezone.utc)
        now = datetime(2026, 5, 13, tzinfo=timezone.utc)

        params = scraper._build_nvd_api_params(provider, since, now)
        assert params.get("cvssV3Severity") == "HIGH"

    def test_nvd_api_url_renders_no_rejected_as_bare_flag(self):
        url = CVEScraper._build_nvd_api_url(
            {"resultsPerPage": "2000", "startIndex": "0"},
            bare_flags=["noRejected"],
        )
        assert "noRejected=true" not in url
        assert "noRejected=" not in url
        assert url.endswith("resultsPerPage=2000&startIndex=0&noRejected")


class TestOrchestratorIntegration:
    def test_registers_cve_scraper_when_enabled(self, monkeypatch, tmp_path):
        config = Config(
            version="1.0",
            ai=AIConfig(provider="openai", model="gpt-4", api_key_env="OPENAI_API_KEY"),
            sources=SourcesConfig(
                hackernews=HackerNewsConfig(enabled=False),
                reddit={"enabled": False, "subreddits": [], "users": [], "fetch_comments": 0},
                telegram={"enabled": False, "channels": []},
                github=[],
                rss=[],
                cve=_cve_config(_provider(CVEProviderType.CISA_KEV)),
            ),
            filtering=FilteringConfig(ai_score_threshold=7.0, time_window_hours=24),
        )
        storage = StorageManager(data_dir=str(tmp_path))
        orchestrator = HorizonOrchestrator(config, storage)
        fetch_mock = AsyncMock(return_value=[])
        monkeypatch.setattr(CVEScraper, "fetch", fetch_mock)
        since = datetime.now(timezone.utc) - timedelta(days=1)

        asyncio.run(orchestrator.fetch_all_sources(since))

        assert fetch_mock.await_count == 1

    def test_skips_cve_scraper_when_disabled(self, monkeypatch, tmp_path):
        config = Config(
            version="1.0",
            ai=AIConfig(provider="openai", model="gpt-4", api_key_env="OPENAI_API_KEY"),
            sources=SourcesConfig(
                hackernews=HackerNewsConfig(enabled=False),
                reddit={"enabled": False, "subreddits": [], "users": [], "fetch_comments": 0},
                telegram={"enabled": False, "channels": []},
                github=[],
                rss=[],
                cve=_cve_config(_provider(CVEProviderType.CISA_KEV), enabled=False),
            ),
            filtering=FilteringConfig(ai_score_threshold=7.0, time_window_hours=24),
        )
        storage = StorageManager(data_dir=str(tmp_path))
        orchestrator = HorizonOrchestrator(config, storage)
        fetch_mock = AsyncMock(return_value=[])
        monkeypatch.setattr(CVEScraper, "fetch", fetch_mock)
        since = datetime.now(timezone.utc) - timedelta(days=1)

        asyncio.run(orchestrator.fetch_all_sources(since))

        assert fetch_mock.await_count == 0

    def test_sub_source_label_maps_provider_names(self):
        item = MagicMock()
        item.metadata = {"provider": "nvd_modified"}
        item.author = "NVD"
        assert HorizonOrchestrator._sub_source_label(item) == "NVD modified"

    def test_sub_source_label_maps_ghsa_provider_name(self):
        item = MagicMock()
        item.metadata = {"provider": "ghsa"}
        item.author = "GitHub Advisory Database"
        assert HorizonOrchestrator._sub_source_label(item) == "GHSA"
