"""
Stock market data fetcher module using direct HTTP requests.
Interacts with Yahoo Finance's public chart API with robust retry, crumb authentication, and error handling.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from pydantic import ValidationError

from src.config import settings
from src.models import StockPriceRecord

logger = logging.getLogger(__name__)


class StockFetchError(Exception):
    """Custom exception for fatal stock data fetching errors."""
    pass


class YahooFinanceFetcher:
    """
    Direct HTTP-based fetcher for Yahoo Finance historical and intraday quotes.
    Uses requests.Session with connection pooling, retries, exponential backoff,
    and automatic session cookie/crumb resolution.
    """
    BASE_URL = "https://query2.finance.yahoo.com/v8/finance/chart/{ticker}"
    CRUMB_URL = "https://query2.finance.yahoo.com/v1/test/getcrumb"
    COOKIE_URL = "https://fc.yahoo.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.crumb: Optional[str] = None
        if session:
            self.session = session
        else:
            self.session = requests.Session()
            # Browser-like User-Agent required by Yahoo Finance
            self.session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
            })

            # Configure Tier-1 HTTP retry strategy with exponential backoff
            retries = Retry(
                total=settings.http_max_retries,
                backoff_factor=1,  # Waits 1s, 2s, 4s between retries
                status_forcelist=[500, 502, 503, 504],
                raise_on_status=False
            )
            adapter = HTTPAdapter(max_retries=retries)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)

    def _ensure_crumb(self) -> str:
        """
        Obtain or refresh Yahoo Finance session cookie and authentication crumb.
        Ensures calls to the chart endpoint succeed without HTTP 429/401 blocks.
        """
        if self.crumb:
            return self.crumb

        timeout = (settings.http_timeout_connect, settings.http_timeout_read)
        try:
            # 1. Establish session cookies
            self.session.get(self.COOKIE_URL, timeout=timeout)
            
            # 2. Fetch temporary authentication crumb
            crumb_resp = self.session.get(self.CRUMB_URL, timeout=timeout)
            if crumb_resp.status_code == 200 and crumb_resp.text:
                self.crumb = crumb_resp.text.strip()
                logger.info("Successfully established Yahoo Finance session crumb.")
                return self.crumb
            else:
                logger.warning(
                    f"Could not retrieve crumb (status {crumb_resp.status_code}). Proceeding without crumb."
                )
                return ""
        except Exception as e:
            logger.warning(f"Error during crumb resolution: {e}. Proceeding without crumb.")
            return ""

    def fetch_raw_chart_data(
        self,
        ticker: str,
        range_param: str = "1mo",
        interval_param: str = "1d"
    ) -> Dict[str, Any]:
        """
        Execute direct HTTP GET request to Yahoo Finance chart API.
        
        Args:
            ticker: Stock ticker symbol (e.g. AAPL)
            range_param: Query range (e.g. 1d, 5d, 1mo, 1y)
            interval_param: Bar interval (e.g. 1m, 1h, 1d)
            
        Returns:
            Dict containing the raw JSON response
            
        Raises:
            StockFetchError: If the HTTP request fails or returns an error status
        """
        symbol = ticker.strip().upper()
        url = self.BASE_URL.format(ticker=symbol)
        crumb = self._ensure_crumb()

        params = {
            "range": range_param,
            "interval": interval_param,
            "includePrePost": "false",
            "events": "div,splits",
        }
        if crumb:
            params["crumb"] = crumb

        timeout = (settings.http_timeout_connect, settings.http_timeout_read)

        logger.info(f"Fetching chart data for {symbol} (range={range_param}, interval={interval_param})")
        try:
            response = self.session.get(url, params=params, timeout=timeout)

            # If 401/429 occurs, attempt single crumb refresh
            if response.status_code in (401, 429) and self.crumb:
                logger.info(f"Received HTTP {response.status_code}. Refreshing crumb and retrying once...")
                self.crumb = None
                crumb = self._ensure_crumb()
                if crumb:
                    params["crumb"] = crumb
                response = self.session.get(url, params=params, timeout=timeout)

            if response.status_code == 429:
                raise StockFetchError(f"HTTP 429: Rate limited by Yahoo Finance API for ticker {symbol}")
            
            if response.status_code >= 400:
                raise StockFetchError(
                    f"HTTP {response.status_code} Error while querying {url}: {response.text[:200]}"
                )

            return response.json()

        except requests.exceptions.Timeout as e:
            raise StockFetchError(f"Connection timed out while fetching {symbol}: {str(e)}") from e
        except requests.exceptions.RequestException as e:
            raise StockFetchError(f"Network error while fetching {symbol}: {str(e)}") from e
        except ValueError as e:
            raise StockFetchError(f"Failed to parse JSON response for {symbol}: {str(e)}") from e

    def parse_and_validate(
        self,
        ticker: str,
        payload: Dict[str, Any]
    ) -> Tuple[List[StockPriceRecord], int]:
        """
        Parse raw Yahoo Finance chart JSON payload and validate via Pydantic.
        
        Enforces:
        - Non-imputation policy: If any critical price (open, high, low, close) is None,
          the record is logged and skipped.
        - Market closure awareness: Empty payload during weekends/holidays is handled gracefully.
        
        Returns:
            Tuple of (valid_records_list, rejected_records_count)
        """
        symbol = ticker.strip().upper()
        chart = payload.get("chart", {})
        
        # Check for API-level error returned in the payload
        api_error = chart.get("error")
        if api_error:
            error_desc = api_error.get("description", str(api_error))
            logger.error(f"Yahoo Finance returned error for {symbol}: {error_desc}")
            raise StockFetchError(f"API Error for {symbol}: {error_desc}")

        results = chart.get("result")
        if not results:
            logger.warning(f"No chart results found for {symbol} (market may be closed or ticker invalid)")
            return [], 0

        data = results[0]
        timestamps = data.get("timestamp", [])
        indicators = data.get("indicators", {}).get("quote", [])

        if not timestamps or not indicators:
            logger.info(f"No trading data points returned for {symbol} (market closed / holiday)")
            return [], 0

        quotes = indicators[0]
        opens = quotes.get("open", [])
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        closes = quotes.get("close", [])
        volumes = quotes.get("volume", [])

        valid_records: List[StockPriceRecord] = []
        rejected_count = 0

        # Iterate through timestamps and corresponding quote arrays
        for idx, ts in enumerate(timestamps):
            try:
                # Safely extract values at current index
                o = opens[idx] if idx < len(opens) else None
                h = highs[idx] if idx < len(highs) else None
                l = lows[idx] if idx < len(lows) else None
                c = closes[idx] if idx < len(closes) else None
                v = volumes[idx] if idx < len(volumes) else None

                # Non-imputation rule: All core price fields must be present and non-null
                if None in (o, h, l, c):
                    logger.warning(
                        f"[{symbol}] Discarding incomplete bar at index {idx} (timestamp={ts}): "
                        f"open={o}, high={h}, low={l}, close={c}"
                    )
                    rejected_count += 1
                    continue

                # Treat null volume as 0 (valid for certain halts/intraday ticks)
                vol_clean = int(v) if v is not None else 0

                # Normalize timestamp to UTC datetime
                dt = datetime.fromtimestamp(ts, tz=timezone.utc)

                # Validate against strict financial constraints in Pydantic model
                record = StockPriceRecord(
                    symbol=symbol,
                    timestamp=dt,
                    open_price=round(float(o), 4),
                    high_price=round(float(h), 4),
                    low_price=round(float(l), 4),
                    close_price=round(float(c), 4),
                    volume=vol_clean
                )
                valid_records.append(record)

            except ValidationError as ve:
                logger.warning(
                    f"[{symbol}] Pydantic validation failed for record at timestamp {ts}: {ve}"
                )
                rejected_count += 1
            except Exception as ex:
                logger.warning(
                    f"[{symbol}] Unexpected error parsing candle at index {idx}: {ex}"
                )
                rejected_count += 1

        logger.info(
            f"Parsed {symbol}: {len(valid_records)} valid records, {rejected_count} rejected"
        )
        return valid_records, rejected_count
