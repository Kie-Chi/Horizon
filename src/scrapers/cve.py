"""CVE scraper implementation."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from xml.etree import ElementTree

import httpx
from rich.console import Console

from .base import BaseScraper
from ..models import CVEConfig, CVEProviderConfig, CVEProviderType, ContentItem, SourceType

logger = logging.getLogger(__name__)

CISA_KEV_JSON_URL = (
    "https://www.cisa.gov/sites/default/files/feeds/"
    "known_exploited_vulnerabilities.json"
)
GHSA_API_URL = "https://api.github.com/advisories"
NVD_API_BASE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"
NVD_CVE_URL = "https://nvd.nist.gov/vuln/detail/{cve_id}"
CISA_KEV_CATALOG_URL = "https://www.cisa.gov/known-exploited-vulnerabilities-catalog"
CVELIST_V5_RELEASES_ATOM_URL = "https://github.com/CVEProject/cvelistV5/releases.atom"
CVELIST_V5_RELEASE_DOWNLOAD_URL = (
    "https://github.com/CVEProject/cvelistV5/releases/download/{tag}/{asset_name}"
)
CVELIST_V5_CVE_URL = "https://www.cve.org/CVERecord?id={cve_id}"

NVD_API_TIMEOUT = 60.0
NVD_MAX_TIME_WINDOW_DAYS = 120
NVD_API_RESULTS_PER_PAGE = 2000
CVELIST_V5_TIMEOUT = 60.0
CVELIST_V5_TAG_RE = re.compile(r"^cve_(\d{4}-\d{2}-\d{2})_(\d{4}Z)$")

CVSS_SEVERITY_MAP = {
    9.0: "CRITICAL",
    7.0: "HIGH",
    4.0: "MEDIUM",
    0.1: "LOW",
}


@dataclass(frozen=True)
class CVEReleaseEntry:
    tag: str
    updated_at: datetime


class CVEScraper(BaseScraper):
    """Scraper for official CVE feeds."""

    SOURCE_TYPE = SourceType.CVE

    def __init__(
        self,
        config: CVEConfig,
        http_client: httpx.AsyncClient,
        state_path: Optional[Path] = None,
        console: Optional[Console] = None,
    ):
        super().__init__({"cve": config}, http_client)
        self.cve_config = config
        self.state_path = state_path or Path("data/cache/cve_state.json")
        self.console = console or Console()
        self._state = self._load_state()
        self._nvd_api_key = self._resolve_nvd_api_key()
        self._github_token = self._resolve_github_token()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def fetch(self, since: datetime) -> List[ContentItem]:
        if not self.cve_config.enabled:
            return []

        items_by_key: Dict[str, ContentItem] = {}
        merged_items: list[ContentItem] = []
        since_utc = self._ensure_utc(since)
        providers = [provider for provider in self.cve_config.providers if provider.enabled]
        tasks = [self._fetch_provider(provider, since_utc) for provider in providers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for provider, result in zip(providers, results):
            if isinstance(result, Exception):
                logger.warning("CVE provider %s failed: %s", provider.type.value, result)
                continue
            for item in result:
                item_keys = self._dedupe_keys(item)
                current = next(
                    (items_by_key[key] for key in item_keys if key in items_by_key),
                    None,
                )
                if current is None:
                    merged_items.append(item)
                    for key in item_keys:
                        items_by_key[key] = item
                    continue
                merged = self._merge_duplicate(current, item)
                if merged is not current:
                    current_index = merged_items.index(current)
                    merged_items[current_index] = merged
                for key in self._dedupe_keys(current) + item_keys + self._dedupe_keys(merged):
                    items_by_key[key] = merged

        self._save_state()
        return merged_items

    # ------------------------------------------------------------------
    # Provider dispatch
    # ------------------------------------------------------------------

    async def _fetch_provider(
        self, provider: CVEProviderConfig, since_utc: datetime
    ) -> List[ContentItem]:
        started_at = time.monotonic()
        handlers = {
            CVEProviderType.CISA_KEV: self._fetch_cisa_kev,
            CVEProviderType.CVELIST_V5_DELTA: self._fetch_cvelist_v5_delta,
            CVEProviderType.GHSA: self._fetch_ghsa,
            CVEProviderType.NVD_RECENT: self._fetch_nvd_api,
            CVEProviderType.NVD_MODIFIED: self._fetch_nvd_api,
        }
        handler = handlers.get(provider.type)
        if handler is None:
            logger.warning("Unsupported CVE provider type: %s", provider.type.value)
            return []
        items = await handler(provider, since_utc)
        self._provider_runtime_state(provider)["last_success_at"] = datetime.now(
            timezone.utc
        ).isoformat()

        logger.info(
            "CVE provider %s finished in %.2fs with %d items",
            provider.type.value,
            time.monotonic() - started_at,
            len(items),
        )
        return items

    # ------------------------------------------------------------------
    # CISA KEV
    # ------------------------------------------------------------------

    async def _fetch_cisa_kev(
        self, provider: CVEProviderConfig, since_utc: datetime
    ) -> List[ContentItem]:
        headers = self._kev_conditional_headers(provider)
        response = await self._get_with_kev_cache(provider, CISA_KEV_JSON_URL, headers)
        if response.status_code == 304:
            logger.info("CVE provider %s returned 304 Not Modified", provider.type.value)
            return []

        parse_started_at = time.monotonic()
        payload = await asyncio.to_thread(json.loads, response.text)
        vulns = payload.get("vulnerabilities", [])
        items: List[ContentItem] = []
        for entry in vulns:
            item = self._kev_entry_to_item(entry, provider, since_utc)
            if item is not None:
                items.append(item)
        self._update_kev_cache(provider, response)
        logger.info(
            "CVE provider %s parsed %d raw KEV records into %d items in %.2fs",
            provider.type.value,
            len(vulns),
            len(items),
            time.monotonic() - parse_started_at,
        )
        return items

    async def _get_with_kev_cache(
        self, provider: CVEProviderConfig, url: str, headers: dict[str, str]
    ) -> httpx.Response:
        response = await self.client.get(url, headers=headers, follow_redirects=True)
        if response.status_code != 304:
            response.raise_for_status()
        return response

    def _kev_conditional_headers(self, provider: CVEProviderConfig) -> dict[str, str]:
        """Build conditional request headers for CISA KEV (ETag / Last-Modified)."""
        state = self._kev_state(provider)
        headers: dict[str, str] = {}
        etag = state.get("etag")
        last_modified = state.get("last_modified_header")
        if etag:
            headers["If-None-Match"] = str(etag)
        if last_modified:
            headers["If-Modified-Since"] = str(last_modified)
        return headers

    def _update_kev_cache(
        self, provider: CVEProviderConfig, response: httpx.Response
    ) -> None:
        state = self._provider_cache_state(provider)
        etag = response.headers.get("ETag")
        last_modified = response.headers.get("Last-Modified")
        if etag:
            state["etag"] = etag
        if last_modified:
            state["last_modified_header"] = last_modified

    def _kev_state(self, provider: CVEProviderConfig) -> dict[str, Any]:
        return self._provider_cache_state(provider)

    def _provider_state(self, provider: CVEProviderConfig) -> dict[str, Any]:
        providers = self._state.setdefault("providers", {})
        state = providers.setdefault(provider.type.value, {})

        # Backward-compatible migration from the older flat per-provider shape.
        if any(key in state for key in ("etag", "last_modified_header")):
            cache = state.setdefault("cache", {})
            if "etag" in state and "etag" not in cache:
                cache["etag"] = state.pop("etag")
            if "last_modified_header" in state and "last_modified_header" not in cache:
                cache["last_modified_header"] = state.pop("last_modified_header")
        if "last_success_at" in state:
            runtime = state.setdefault("runtime", {})
            runtime.setdefault("last_success_at", state.pop("last_success_at"))

        state.setdefault("cache", {})
        state.setdefault("cursor", {})
        state.setdefault("runtime", {})
        return state

    def _provider_cache_state(self, provider: CVEProviderConfig) -> dict[str, Any]:
        return self._provider_state(provider)["cache"]

    def _provider_cursor_state(self, provider: CVEProviderConfig) -> dict[str, Any]:
        return self._provider_state(provider)["cursor"]

    def _provider_runtime_state(self, provider: CVEProviderConfig) -> dict[str, Any]:
        return self._provider_state(provider)["runtime"]

    # ------------------------------------------------------------------
    # GHSA
    # ------------------------------------------------------------------

    async def _fetch_ghsa(
        self, provider: CVEProviderConfig, since_utc: datetime
    ) -> List[ContentItem]:
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self._github_token:
            headers["Authorization"] = f"token {self._github_token}"
        url: Optional[str] = f"{GHSA_API_URL}?per_page=100&sort=updated&direction=desc"
        advisories: list[dict[str, Any]] = []

        while url:
            response = await self.client.get(
                url,
                headers=headers,
                follow_redirects=True,
            )
            response.raise_for_status()
            page_entries = response.json()
            if not isinstance(page_entries, list):
                break

            reached_cutoff = False
            for entry in page_entries:
                modified_at = self._parse_datetime(entry.get("updated_at"))
                published_at = self._parse_datetime(entry.get("published_at"))
                compare_dt = modified_at or published_at
                if compare_dt is None:
                    continue
                if compare_dt <= since_utc:
                    reached_cutoff = True
                    continue
                advisories.append(entry)

            if reached_cutoff:
                break
            next_url = response.links.get("next", {}).get("url")
            url = str(next_url) if next_url else None

        parse_started_at = time.monotonic()
        items: List[ContentItem] = []
        for entry in advisories:
            item = self._ghsa_entry_to_item(entry, provider, since_utc)
            if item is not None:
                items.append(item)

        logger.info(
            "CVE provider %s parsed %d raw GHSA records into %d items in %.2fs",
            provider.type.value,
            len(advisories),
            len(items),
            time.monotonic() - parse_started_at,
        )
        return items

    # ------------------------------------------------------------------
    # CVE List V5 delta releases
    # ------------------------------------------------------------------

    async def _fetch_cvelist_v5_delta(
        self, provider: CVEProviderConfig, since_utc: datetime
    ) -> List[ContentItem]:
        cursor_state = self._provider_cursor_state(provider)
        last_release_updated = self._parse_datetime(cursor_state.get("last_release_updated"))
        last_release_tag = str(cursor_state.get("last_release_tag") or "")

        releases = await self._fetch_cvelist_release_entries()
        pending_releases = [
            release
            for release in releases
            if release.updated_at > since_utc
            and (
                last_release_updated is None
                or release.updated_at > last_release_updated
                or (
                    release.updated_at == last_release_updated
                    and release.tag > last_release_tag
                )
            )
        ]

        release_results = await asyncio.gather(
            *[self._fetch_cvelist_release_records(release) for release in pending_releases],
            return_exceptions=True,
        )

        items: List[ContentItem] = []
        for release, release_result in zip(pending_releases, release_results):
            if isinstance(release_result, httpx.HTTPStatusError):
                logger.warning(
                    "Skipping cvelistV5 release %s after asset lookup failed: %s",
                    release.tag,
                    release_result,
                )
                continue
            if isinstance(release_result, Exception):
                raise release_result

            for entry in release_result:
                item = self._cvelist_entry_to_item(
                    entry, provider, since_utc, release.updated_at
                )
                if item is not None:
                    items.append(item)
            cursor_state["last_release_tag"] = release.tag
            cursor_state["last_release_updated"] = release.updated_at.isoformat()

        logger.info(
            "CVE provider %s processed %d delta releases into %d items",
            provider.type.value,
            len(pending_releases),
            len(items),
        )
        return items

    async def _fetch_cvelist_release_entries(self) -> list[CVEReleaseEntry]:
        response = await self.client.get(
            CVELIST_V5_RELEASES_ATOM_URL,
            timeout=CVELIST_V5_TIMEOUT,
            follow_redirects=True,
        )
        response.raise_for_status()
        root = ElementTree.fromstring(response.text)
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        releases: list[CVEReleaseEntry] = []
        for entry in root.findall("atom:entry", ns):
            link = entry.find("atom:link", ns)
            updated = entry.findtext("atom:updated", default="", namespaces=ns)
            if link is None:
                continue
            href = str(link.attrib.get("href") or "").strip()
            tag = href.rstrip("/").rsplit("/", 1)[-1]
            updated_at = self._parse_datetime(updated)
            if not tag or updated_at is None:
                continue
            if not CVELIST_V5_TAG_RE.match(tag):
                continue
            releases.append(CVEReleaseEntry(tag=tag, updated_at=updated_at))
        releases.sort(key=lambda release: (release.updated_at, release.tag))
        return releases

    async def _fetch_cvelist_release_records(
        self, release: CVEReleaseEntry
    ) -> list[dict[str, Any]]:
        response = await self._download_cvelist_release_asset(release.tag)

        records: list[dict[str, Any]] = []
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            for name in archive.namelist():
                if not name.endswith(".json"):
                    continue
                if not Path(name).name.startswith("CVE-"):
                    continue
                with archive.open(name) as handle:
                    try:
                        records.append(json.loads(handle.read().decode("utf-8")))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        logger.warning("Skipping invalid cvelistV5 record %s: %s", name, exc)
        return records

    async def _download_cvelist_release_asset(self, tag: str) -> httpx.Response:
        last_error: Optional[httpx.HTTPStatusError] = None
        for url in self._build_cvelist_release_asset_urls(tag):
            response = await self.client.get(
                url,
                timeout=CVELIST_V5_TIMEOUT,
                follow_redirects=True,
            )
            try:
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                last_error = exc
                if response.status_code != 404:
                    raise
        if last_error is not None:
            raise last_error
        raise ValueError(f"Unable to resolve cvelistV5 asset URL for release {tag}")

    @staticmethod
    def _build_cvelist_release_asset_urls(tag: str) -> list[str]:
        match = CVELIST_V5_TAG_RE.match(tag)
        if match is None:
            raise ValueError(f"Unsupported cvelistV5 release tag format: {tag}")
        day, hour = match.groups()
        release_hour = datetime.strptime(f"{day}T{hour}", "%Y-%m-%dT%H%MZ").replace(
            tzinfo=timezone.utc
        )
        candidate_hours = [release_hour, release_hour + timedelta(hours=1)]
        urls: list[str] = []
        for candidate in candidate_hours:
            asset_hour = candidate.strftime("%H00Z")
            asset_name = f"{day}_delta_CVEs_at_{asset_hour}.zip"
            urls.append(
                CVELIST_V5_RELEASE_DOWNLOAD_URL.format(tag=tag, asset_name=asset_name)
            )
        return urls

    # ------------------------------------------------------------------
    # NVD API
    # ------------------------------------------------------------------

    async def _fetch_nvd_api(
        self, provider: CVEProviderConfig, since_utc: datetime
    ) -> List[ContentItem]:
        now_utc = datetime.now(timezone.utc)
        window_days = (now_utc - since_utc).days

        # NVD API 2.0 limits a single request to at most 120 days.
        if window_days > NVD_MAX_TIME_WINDOW_DAYS:
            self.console.print(
                f"[yellow]⚠️  CVE provider {provider.type.value}: time window "
                f"({window_days} days) exceeds NVD API 2.0 limit of "
                f"{NVD_MAX_TIME_WINDOW_DAYS} days. Multi-segment requests "
                f"are not yet implemented; this provider will be skipped.[/yellow]"
            )
            logger.warning(
                "CVE provider %s skipped: time window %d days > %d limit",
                provider.type.value, window_days, NVD_MAX_TIME_WINDOW_DAYS,
            )
            return []

        params = self._build_nvd_api_params(provider, since_utc, now_utc)
        headers: dict[str, str] = {}
        if self._nvd_api_key:
            headers["apiKey"] = self._nvd_api_key

        base_params = {
            **params,
            "resultsPerPage": str(NVD_API_RESULTS_PER_PAGE),
        }
        start_index = 0
        total_results = None
        vulns: list[dict[str, Any]] = []

        while True:
            page_params = {**base_params, "startIndex": str(start_index)}
            url = self._build_nvd_api_url(page_params, bare_flags=["noRejected"])
            logger.info("CVE provider %s requesting NVD API: %s", provider.type.value, url)

            response = await self.client.get(
                url,
                headers=headers,
                timeout=NVD_API_TIMEOUT,
                follow_redirects=True,
            )
            response.raise_for_status()
            payload = response.json()

            if total_results is None:
                total_results = int(payload.get("totalResults", 0) or 0)
            page_vulns = payload.get("vulnerabilities", [])
            vulns.extend(page_vulns)

            results_per_page = int(payload.get("resultsPerPage", len(page_vulns)) or 0)
            fetched_count = len(page_vulns)
            next_index = start_index + max(results_per_page, fetched_count)
            if fetched_count == 0 or next_index >= total_results:
                break
            start_index = next_index

        parse_started_at = time.monotonic()
        items: List[ContentItem] = []
        for wrapper in vulns:
            item = self._nvd_entry_to_item(wrapper.get("cve", {}), provider, since_utc)
            if item is not None:
                items.append(item)

        logger.info(
            "CVE provider %s parsed %d raw NVD records into %d items in %.2fs",
            provider.type.value,
            len(vulns),
            len(items),
            time.monotonic() - parse_started_at,
        )
        return items

    def _build_nvd_api_params(
        self, provider: CVEProviderConfig, since_utc: datetime, now_utc: datetime
    ) -> dict[str, str]:
        """Build NVD API 2.0 query parameters from provider config and time window."""
        params: dict[str, str] = {}

        if provider.type == CVEProviderType.NVD_RECENT:
            params["pubStartDate"] = self._format_nvd_date(since_utc)
            params["pubEndDate"] = self._format_nvd_date(now_utc)
        elif provider.type == CVEProviderType.NVD_MODIFIED:
            params["lastModStartDate"] = self._format_nvd_date(since_utc)
            params["lastModEndDate"] = self._format_nvd_date(now_utc)

        # Coarse CVSS severity server-side filter.
        if provider.type != CVEProviderType.CISA_KEV and provider.min_cvss is not None:
            severity = self._cvss_to_severity(provider.min_cvss)
            if severity:
                params["cvssV3Severity"] = severity

        return params

    @staticmethod
    def _build_nvd_api_url(
        params: dict[str, str], bare_flags: Optional[list[str]] = None
    ) -> str:
        query = urlencode(params)
        if bare_flags:
            extras = "&".join(flag for flag in bare_flags if flag)
            if extras:
                query = f"{query}&{extras}" if query else extras
        return f"{NVD_API_BASE_URL}?{query}" if query else NVD_API_BASE_URL

    @staticmethod
    def _format_nvd_date(dt: datetime) -> str:
        """Format datetime as NVD API 2.0 ISO 8601 string.

        Required format: yyyy-MM-dd'T'HH:mm:ss.SSS+offset
        """
        return dt.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")

    @staticmethod
    def _cvss_to_severity(min_cvss: float) -> Optional[str]:
        """Map a float CVSS threshold to the coarsest NVD API severity level.
        """
        for threshold, severity in sorted(CVSS_SEVERITY_MAP.items(), reverse=True):
            if min_cvss >= threshold:
                return severity
        return None

    def _resolve_nvd_api_key(self) -> Optional[str]:
        """Resolve the NVD API key from the environment variable."""
        if not self.cve_config.nvd_api_key_env:
            return None
        return os.environ.get(self.cve_config.nvd_api_key_env)

    @staticmethod
    def _resolve_github_token() -> Optional[str]:
        """Resolve the GitHub token from the standard project environment variable."""
        return os.environ.get("GITHUB_TOKEN")

    # ------------------------------------------------------------------
    # CISA KEV entry parsing
    # ------------------------------------------------------------------

    def _kev_entry_to_item(
        self, entry: dict[str, Any], provider: CVEProviderConfig, since_utc: datetime
    ) -> Optional[ContentItem]:
        cve_id = str(entry.get("cveID") or "").strip()
        if not cve_id:
            return None
        published_at = self._parse_datetime(entry.get("dateAdded"))
        if published_at is None or published_at <= since_utc:
            return None

        vendor = str(entry.get("vendorProject") or "").strip()
        product = str(entry.get("product") or "").strip()
        vulnerability_name = str(entry.get("vulnerabilityName") or "").strip()
        short_description = str(entry.get("shortDescription") or "").strip()
        required_action = str(entry.get("requiredAction") or "").strip()
        known_ransomware = str(entry.get("knownRansomwareCampaignUse") or "").strip()
        due_date = str(entry.get("dueDate") or "").strip()
        references = self._extract_references(entry)

        if not self._matches_filters(
            provider=provider,
            title=vulnerability_name or cve_id,
            description=short_description,
            vendors=[vendor] if vendor else [],
            products=[product] if product else [],
            cwe="",
            references=references,
        ):
            return None

        vendor_label = vendor or product or "Unknown Vendor"
        content_parts = [
            short_description,
            f"Vendor: {vendor}" if vendor else "",
            f"Product: {product}" if product else "",
            f"Required Action: {required_action}" if required_action else "",
            f"Due Date: {due_date}" if due_date else "",
            (
                f"Known Ransomware Campaign Use: {known_ransomware}"
                if known_ransomware
                else ""
            ),
        ]
        metadata = {
            "provider": provider.type.value,
            "cve_id": cve_id,
            "vendor": vendor or None,
            "product": product or None,
            "kev": True,
            "date_added": entry.get("dateAdded"),
            "due_date": due_date or None,
            "required_action": required_action or None,
            "known_ransomware_campaign_use": known_ransomware or None,
            "references": references,
        }
        return ContentItem(
            id=self._generate_id("cve", "kev", cve_id),
            source_type=self.SOURCE_TYPE,
            title=f"{cve_id} | {vendor_label} | Known Exploited Vulnerability",
            url=self._kev_url(entry),
            content="\n".join(part for part in content_parts if part),
            author="CISA KEV",
            published_at=published_at,
            metadata=self._compact_dict(metadata),
        )

    # ------------------------------------------------------------------
    # CVE List V5 entry parsing
    # ------------------------------------------------------------------

    def _cvelist_entry_to_item(
        self,
        entry: dict[str, Any],
        provider: CVEProviderConfig,
        since_utc: datetime,
        release_updated_at: datetime,
    ) -> Optional[ContentItem]:
        metadata = entry.get("cveMetadata", {})
        cve_id = str(metadata.get("cveId") or "").strip()
        if not cve_id:
            return None

        published = self._parse_datetime(metadata.get("datePublished"))
        last_modified = self._parse_datetime(metadata.get("dateUpdated"))
        compare_dt = last_modified or published or release_updated_at
        if compare_dt <= since_utc:
            return None

        containers = self._cvelist_containers(entry)
        description = self._pick_cvelist_description(containers)
        vendors, products = self._extract_cvelist_products(containers)
        cvss_score, cvss_vector, severity = self._extract_cvelist_cvss(containers)
        cwe = self._extract_cvelist_cwe(containers)
        references = self._extract_cvelist_reference_urls(containers)

        if provider.min_cvss is not None and (
            cvss_score is None or cvss_score < provider.min_cvss
        ):
            return None
        if not self._matches_filters(
            provider=provider,
            title=cve_id,
            description=description,
            vendors=vendors,
            products=products,
            cwe=cwe,
            references=references,
        ):
            return None

        label_parts = [
            part for part in [vendors[0] if vendors else "", products[0] if products else ""] if part
        ]
        primary_label = " / ".join(label_parts) if label_parts else "Unspecified"
        score_label = f"CVSS {cvss_score:.1f}" if cvss_score is not None else "CVSS n/a"
        content_parts = [
            description,
            f"Vendors: {', '.join(vendors)}" if vendors else "",
            f"Products: {', '.join(products)}" if products else "",
            f"CWE: {cwe}" if cwe else "",
            f"References: {', '.join(references[:5])}" if references else "",
        ]
        item_metadata = {
            "provider": provider.type.value,
            "cve_id": cve_id,
            "cvss": cvss_score,
            "cvss_vector": cvss_vector,
            "severity": severity,
            "cwe": cwe or None,
            "vendors": vendors,
            "products": products,
            "published": metadata.get("datePublished"),
            "last_modified": metadata.get("dateUpdated"),
            "references": references,
            "kev": False,
            "source_repo": "CVEProject/cvelistV5",
        }
        return ContentItem(
            id=self._generate_id("cve", "cvelist", cve_id),
            source_type=self.SOURCE_TYPE,
            title=f"{cve_id} | {primary_label} | {score_label}",
            url=CVELIST_V5_CVE_URL.format(cve_id=cve_id),
            content="\n".join(part for part in content_parts if part),
            author="CVE List V5",
            published_at=compare_dt,
            metadata=self._compact_dict(item_metadata),
        )

    # ------------------------------------------------------------------
    # GHSA entry parsing
    # ------------------------------------------------------------------

    def _ghsa_entry_to_item(
        self, entry: dict[str, Any], provider: CVEProviderConfig, since_utc: datetime
    ) -> Optional[ContentItem]:
        ghsa_id = str(entry.get("ghsa_id") or "").strip()
        if not ghsa_id:
            return None

        published = self._parse_datetime(entry.get("published_at"))
        last_modified = self._parse_datetime(entry.get("updated_at"))
        compare_dt = last_modified or published
        if compare_dt is None or compare_dt <= since_utc:
            return None

        cve_id = str(entry.get("cve_id") or "").strip() or None
        summary = str(entry.get("summary") or "").strip()
        description = str(entry.get("description") or "").strip()
        vendors, products = self._extract_ghsa_products(entry.get("vulnerabilities", []))
        cvss_score, cvss_vector, severity = self._extract_ghsa_cvss(entry)
        cwe = self._extract_ghsa_cwe(entry.get("cwes", []))
        references = self._extract_ghsa_reference_urls(entry.get("references", []))

        if provider.min_cvss is not None and (
            cvss_score is None or cvss_score < provider.min_cvss
        ):
            return None
        if not self._matches_filters(
            provider=provider,
            title=cve_id or ghsa_id,
            description=summary or description,
            vendors=vendors,
            products=products,
            cwe=cwe,
            references=references,
        ):
            return None

        primary_id = cve_id or ghsa_id
        label_parts = [
            part
            for part in [vendors[0] if vendors else "", products[0] if products else ""]
            if part
        ]
        primary_label = " / ".join(label_parts) if label_parts else "Unspecified"
        score_label = f"CVSS {cvss_score:.1f}" if cvss_score is not None else "CVSS n/a"
        content_parts = [
            description or summary,
            f"Package ecosystems: {', '.join(vendors)}" if vendors else "",
            f"Packages: {', '.join(products)}" if products else "",
            f"CWE: {cwe}" if cwe else "",
            f"References: {', '.join(references[:5])}" if references else "",
        ]
        metadata = {
            "provider": provider.type.value,
            "cve_id": cve_id,
            "ghsa_id": ghsa_id,
            "cvss": cvss_score,
            "cvss_vector": cvss_vector,
            "severity": severity,
            "cwe": cwe or None,
            "vendors": vendors,
            "products": products,
            "published": entry.get("published_at"),
            "last_modified": entry.get("updated_at"),
            "references": references,
            "kev": False,
        }
        return ContentItem(
            id=self._generate_id("cve", "ghsa", primary_id),
            source_type=self.SOURCE_TYPE,
            title=f"{primary_id} | {primary_label} | {score_label}",
            url=str(entry.get("html_url") or f"https://github.com/advisories/{ghsa_id}"),
            content="\n".join(part for part in content_parts if part),
            author="GitHub Advisory Database",
            published_at=compare_dt,
            metadata=self._compact_dict(metadata),
        )

    # ------------------------------------------------------------------
    # NVD entry parsing
    # ------------------------------------------------------------------

    def _nvd_entry_to_item(
        self, entry: dict[str, Any], provider: CVEProviderConfig, since_utc: datetime
    ) -> Optional[ContentItem]:
        cve_id = str(entry.get("id") or "").strip()
        if not cve_id:
            return None
        published = self._parse_datetime(entry.get("published"))
        last_modified = self._parse_datetime(entry.get("lastModified"))
        compare_dt = last_modified if provider.type == CVEProviderType.NVD_MODIFIED else published
        if compare_dt is None or compare_dt <= since_utc:
            return None

        description = self._pick_nvd_description(entry.get("descriptions", []))
        vendors, products = self._extract_nvd_products(entry.get("configurations", []))
        cvss_score, cvss_vector, severity = self._extract_nvd_cvss(entry.get("metrics", {}))
        cwe = self._extract_nvd_cwe(entry.get("weaknesses", []))
        references = self._extract_nvd_reference_urls(entry.get("references", []))

        # Local precise CVSS float filtering (server-side only does coarse severity).
        if provider.type != CVEProviderType.CISA_KEV and provider.min_cvss is not None:
            if cvss_score is None or cvss_score < provider.min_cvss:
                return None

        if not self._matches_filters(
            provider=provider,
            title=cve_id,
            description=description,
            vendors=vendors,
            products=products,
            cwe=cwe,
            references=references,
        ):
            return None

        label_parts = [
            part for part in [vendors[0] if vendors else "", products[0] if products else ""] if part
        ]
        primary_label = " / ".join(label_parts) if label_parts else "Unspecified"
        score_label = f"CVSS {cvss_score:.1f}" if cvss_score is not None else "CVSS n/a"
        content_parts = [
            description,
            f"Vendors: {', '.join(vendors)}" if vendors else "",
            f"Products: {', '.join(products)}" if products else "",
            f"CWE: {cwe}" if cwe else "",
            f"References: {', '.join(references[:5])}" if references else "",
        ]
        metadata = {
            "provider": provider.type.value,
            "cve_id": cve_id,
            "cvss": cvss_score,
            "cvss_vector": cvss_vector,
            "severity": severity,
            "cwe": cwe or None,
            "vendors": vendors,
            "products": products,
            "published": entry.get("published"),
            "last_modified": entry.get("lastModified"),
            "references": references,
            "kev": False,
        }
        return ContentItem(
            id=self._generate_id("cve", "nvd", cve_id),
            source_type=self.SOURCE_TYPE,
            title=f"{cve_id} | {primary_label} | {score_label}",
            url=NVD_CVE_URL.format(cve_id=cve_id),
            content="\n".join(part for part in content_parts if part),
            author="NVD",
            published_at=compare_dt,
            metadata=self._compact_dict(metadata),
        )

    # ------------------------------------------------------------------
    # Shared helpers (unchanged)
    # ------------------------------------------------------------------

    @staticmethod
    def _cvelist_containers(entry: dict[str, Any]) -> list[dict[str, Any]]:
        containers = entry.get("containers", {})
        result: list[dict[str, Any]] = []
        cna = containers.get("cna")
        if isinstance(cna, dict):
            result.append(cna)
        for adp in containers.get("adp", []):
            if isinstance(adp, dict):
                result.append(adp)
        return result

    @staticmethod
    def _pick_cvelist_description(containers: list[dict[str, Any]]) -> str:
        descriptions = [
            description
            for container in containers
            for description in container.get("descriptions", [])
        ]
        return CVEScraper._pick_nvd_description(descriptions)

    @staticmethod
    def _extract_cvelist_products(containers: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
        vendors: list[str] = []
        products: list[str] = []
        for container in containers:
            for affected in container.get("affected", []):
                vendor = str(affected.get("vendor") or "").strip()
                product = str(affected.get("product") or "").strip()
                if vendor and vendor.lower() != "n/a":
                    vendors.append(vendor)
                if product and product.lower() != "n/a":
                    products.append(product)
        return CVEScraper._unique(vendors), CVEScraper._unique(products)

    @staticmethod
    def _extract_cvelist_cvss(
        containers: list[dict[str, Any]]
    ) -> tuple[Optional[float], Optional[str], Optional[str]]:
        priority = ("cvssV4_0", "cvssV3_1", "cvssV3_0", "cvssV2_0")
        for key in priority:
            for container in containers:
                for metric in container.get("metrics", []):
                    data = metric.get(key)
                    if not isinstance(data, dict):
                        continue
                    score = data.get("baseScore")
                    if score is None:
                        continue
                    vector = data.get("vectorString")
                    severity = data.get("baseSeverity")
                    try:
                        return (
                            float(score),
                            str(vector) if vector else None,
                            str(severity) if severity else None,
                        )
                    except (TypeError, ValueError):
                        continue
        return None, None, None

    @staticmethod
    def _extract_cvelist_cwe(containers: list[dict[str, Any]]) -> str:
        for container in containers:
            for problem_type in container.get("problemTypes", []):
                for description in problem_type.get("descriptions", []):
                    cwe_id = str(description.get("cweId") or "").strip()
                    value = str(description.get("description") or "").strip()
                    if cwe_id and cwe_id.upper().startswith("CWE-"):
                        return cwe_id
                    if value:
                        return value
        return ""

    @staticmethod
    def _extract_cvelist_reference_urls(containers: list[dict[str, Any]]) -> list[str]:
        urls = [
            str(reference.get("url") or "").strip()
            for container in containers
            for reference in container.get("references", [])
        ]
        return [url for url in urls if url]

    @staticmethod
    def _ensure_utc(moment: datetime) -> datetime:
        if moment.tzinfo is None:
            return moment.replace(tzinfo=timezone.utc)
        return moment.astimezone(timezone.utc)

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if not value:
            return None
        text = str(value).strip()
        for fmt in (None, "%Y-%m-%d"):
            try:
                if fmt is None:
                    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(text, fmt)
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except ValueError:
                continue
        logger.warning("Unable to parse CVE timestamp: %s", value)
        return None

    @staticmethod
    def _pick_nvd_description(descriptions: list[dict[str, Any]]) -> str:
        for entry in descriptions:
            if entry.get("lang") == "en" and entry.get("value"):
                return str(entry["value"]).strip()
        for entry in descriptions:
            if entry.get("value"):
                return str(entry["value"]).strip()
        return ""

    @staticmethod
    def _extract_nvd_products(configurations: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
        vendors: list[str] = []
        products: list[str] = []
        for config in configurations or []:
            for node in config.get("nodes", []):
                for match in node.get("cpeMatch", []):
                    criteria = str(match.get("criteria") or "")
                    parts = criteria.split(":")
                    if len(parts) >= 5:
                        vendors.append(parts[3].replace("_", " "))
                        products.append(parts[4].replace("_", " "))
        return CVEScraper._unique(vendors), CVEScraper._unique(products)

    @staticmethod
    def _extract_nvd_cvss(metrics: dict[str, Any]) -> tuple[Optional[float], Optional[str], Optional[str]]:
        priority = ("cvssMetricV31", "cvssMetricV30", "cvssMetricV2")
        for key in priority:
            metric_list = metrics.get(key) or []
            if not metric_list:
                continue
            data = metric_list[0].get("cvssData", {})
            score = data.get("baseScore")
            if score is None:
                continue
            vector = data.get("vectorString")
            severity = metric_list[0].get("baseSeverity") or data.get("baseSeverity")
            try:
                return (
                    float(score),
                    str(vector) if vector else None,
                    str(severity) if severity else None,
                )
            except (TypeError, ValueError):
                continue
        return None, None, None

    @staticmethod
    def _extract_nvd_cwe(weaknesses: list[dict[str, Any]]) -> str:
        for weakness in weaknesses or []:
            for desc in weakness.get("description", []):
                value = str(desc.get("value") or "").strip()
                if value:
                    return value
        return ""

    @staticmethod
    def _extract_nvd_reference_urls(references: list[dict[str, Any]]) -> list[str]:
        urls = [str(ref.get("url") or "").strip() for ref in references or []]
        return [url for url in urls if url]

    @staticmethod
    def _extract_ghsa_products(vulnerabilities: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
        vendors: list[str] = []
        products: list[str] = []
        for vulnerability in vulnerabilities or []:
            if not isinstance(vulnerability, dict):
                continue
            package = vulnerability.get("package", {})
            if not isinstance(package, dict):
                continue
            ecosystem = str(package.get("ecosystem") or "").strip()
            name = str(package.get("name") or "").strip()
            if ecosystem:
                vendors.append(ecosystem)
            if name:
                products.append(name)
        return CVEScraper._unique(vendors), CVEScraper._unique(products)

    @staticmethod
    def _extract_ghsa_cvss(
        entry: dict[str, Any]
    ) -> tuple[Optional[float], Optional[str], Optional[str]]:
        cvss = entry.get("cvss")
        if not isinstance(cvss, dict):
            return None, None, None
        score = CVEScraper._safe_float(cvss.get("score"))
        vector = str(cvss.get("vector_string") or "").strip() or None
        severity = str(entry.get("severity") or "").strip() or None
        return score, vector, severity

    @staticmethod
    def _extract_ghsa_cwe(cwes: list[dict[str, Any]]) -> str:
        for cwe in cwes or []:
            if not isinstance(cwe, dict):
                continue
            cwe_id = str(cwe.get("cwe_id") or "").strip()
            if cwe_id:
                return cwe_id
            name = str(cwe.get("name") or "").strip()
            if name:
                return name
        return ""

    @staticmethod
    def _extract_ghsa_reference_urls(references: list[dict[str, Any]]) -> list[str]:
        urls = [
            str(ref.get("url") or "").strip()
            for ref in references or []
            if isinstance(ref, dict)
        ]
        return [url for url in urls if url]

    @staticmethod
    def _extract_references(entry: dict[str, Any]) -> list[str]:
        refs = entry.get("references")
        if isinstance(refs, list):
            urls = [str(ref).strip() for ref in refs if str(ref).strip()]
            if urls:
                return urls
        notes = entry.get("notes")
        if isinstance(notes, list):
            urls = [str(note).strip() for note in notes if str(note).strip().startswith("http")]
            if urls:
                return urls
        return []

    @staticmethod
    def _kev_url(entry: dict[str, Any]) -> str:
        for url in CVEScraper._extract_references(entry):
            if "cisa.gov" in url or "nvd.nist.gov" in url:
                return url
        return CISA_KEV_CATALOG_URL

    def _matches_filters(
        self,
        *,
        provider: CVEProviderConfig,
        title: str,
        description: str,
        vendors: list[str],
        products: list[str],
        cwe: str,
        references: list[str],
    ) -> bool:
        keywords = self._resolved_filter_terms(self.cve_config.keywords, provider.keywords)
        vendors_filter = self._resolved_filter_terms(self.cve_config.vendors, provider.vendors)
        products_filter = self._resolved_filter_terms(self.cve_config.products, provider.products)
        haystack = " ".join(
            [
                title,
                description,
                cwe,
                " ".join(vendors),
                " ".join(products),
                " ".join(references),
            ]
        ).lower()

        if keywords and not any(keyword in haystack for keyword in keywords):
            return False
        lowered_vendors = [vendor.lower() for vendor in vendors]
        if vendors_filter and not any(
            any(token in vendor for vendor in lowered_vendors) for token in vendors_filter
        ):
            return False
        lowered_products = [product.lower() for product in products]
        if products_filter and not any(
            any(token in product for product in lowered_products) for token in products_filter
        ):
            return False
        return True

    @staticmethod
    def _resolved_filter_terms(defaults: list[str], provider_values: list[str]) -> list[str]:
        merged = CVEScraper._unique(list(defaults) + list(provider_values))
        return [term.strip().lower() for term in merged if term.strip()]

    # ------------------------------------------------------------------
    # Merge / dedup logic
    # ------------------------------------------------------------------

    @staticmethod
    def _dedupe_keys(item: ContentItem) -> list[str]:
        metadata = item.metadata
        keys: list[str] = []
        for key in ("cve_id", "ghsa_id"):
            value = str(metadata.get(key) or "").strip()
            if value:
                keys.append(value)
        aliases = metadata.get("aliases")
        if isinstance(aliases, list):
            keys.extend(str(alias).strip() for alias in aliases if str(alias).strip())
        if not keys:
            keys.append(item.id)
        return CVEScraper._unique(keys)

    def _merge_duplicate(self, current: ContentItem, incoming: ContentItem) -> ContentItem:
        current_priority = self._provider_priority(str(current.metadata.get("provider") or ""))
        incoming_priority = self._provider_priority(str(incoming.metadata.get("provider") or ""))
        if incoming_priority < current_priority:
            preferred, secondary = incoming, current
        elif incoming_priority > current_priority:
            preferred, secondary = current, incoming
        else:
            preferred, secondary = self._prefer_more_complete(current, incoming)

        merged_meta = dict(preferred.metadata)
        for key, value in secondary.metadata.items():
            if key not in merged_meta or merged_meta[key] in (None, "", []):
                merged_meta[key] = value
                continue
            if key == "references":
                merged_meta[key] = self._unique(list(merged_meta[key]) + list(value))
        preferred.metadata = merged_meta
        if not preferred.content and secondary.content:
            preferred.content = secondary.content
        return preferred

    @staticmethod
    def _prefer_more_complete(left: ContentItem, right: ContentItem) -> tuple[ContentItem, ContentItem]:
        left_score = CVEScraper._completeness_score(left)
        right_score = CVEScraper._completeness_score(right)
        if right_score > left_score:
            return right, left
        if left_score > right_score:
            return left, right
        if left.metadata.get("provider") == CVEProviderType.NVD_RECENT.value:
            return left, right
        if right.metadata.get("provider") == CVEProviderType.NVD_RECENT.value:
            return right, left
        return left, right

    @staticmethod
    def _completeness_score(item: ContentItem) -> int:
        meta = item.metadata
        score = 0
        for key in ("cvss", "cwe", "references", "vendors", "products", "required_action"):
            value = meta.get(key)
            if value not in (None, "", []):
                score += 1
        if item.content:
            score += 1
        return score

    @staticmethod
    def _provider_priority(provider: str) -> int:
        order = {
            CVEProviderType.CISA_KEV.value: 0,
            CVEProviderType.CVELIST_V5_DELTA.value: 1,
            CVEProviderType.GHSA.value: 2,
            CVEProviderType.NVD_RECENT.value: 3,
            CVEProviderType.NVD_MODIFIED.value: 4,
        }
        return order.get(provider, 99)

    @staticmethod
    def _unique(values: list[str]) -> list[str]:
        seen: set[str] = set()
        unique: list[str] = []
        for value in values:
            cleaned = str(value).strip()
            if not cleaned:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            unique.append(cleaned)
        return unique

    @staticmethod
    def _compact_dict(values: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in values.items() if value is not None and value != []}

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value in (None, ""):
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # State persistence (only used for CISA_KEV caching now)
    # ------------------------------------------------------------------

    def _load_state(self) -> dict[str, Any]:
        try:
            if not self.state_path.exists():
                return {"providers": {}}
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Failed to load CVE state cache %s: %s", self.state_path, exc)
            return {"providers": {}}

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(
                json.dumps(self._state, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("Failed to save CVE state cache %s: %s", self.state_path, exc)
