"""
Odoo XML-RPC client for real-time data access.

Proof-of-concept module for direct Odoo API reads (and potentially writes).
Falls back gracefully if Odoo credentials are not configured.

Configuration (environment variables):
    ODOO_URL      -- Odoo instance URL (e.g., https://your-org.odoo.com)
    ODOO_DB       -- Odoo database name
    ODOO_USER     -- Odoo API username (email)
    ODOO_API_KEY  -- Odoo API key (NOT password -- use Settings > API Keys)

Usage:
    from odoo_client import OdooClient
    client = OdooClient()
    po = client.get_purchase_order("PO00123")
"""
from __future__ import annotations

import os
import xmlrpc.client
from dataclasses import dataclass, field
from typing import Any


@dataclass
class OdooConfig:
    url: str = ""
    db: str = ""
    user: str = ""
    api_key: str = ""

    @classmethod
    def from_env(cls) -> OdooConfig:
        return cls(
            url=os.environ.get("ODOO_URL", ""),
            db=os.environ.get("ODOO_DB", ""),
            user=os.environ.get("ODOO_USER", ""),
            api_key=os.environ.get("ODOO_API_KEY", ""),
        )

    @property
    def is_configured(self) -> bool:
        return bool(self.url and self.db and self.user and self.api_key)


class OdooClient:
    """XML-RPC client for Odoo ERP."""

    def __init__(self, config: OdooConfig | None = None):
        self._config = config or OdooConfig.from_env()
        self._uid: int | None = None
        self._common: xmlrpc.client.ServerProxy | None = None
        self._models: xmlrpc.client.ServerProxy | None = None

    @property
    def is_configured(self) -> bool:
        return self._config.is_configured

    def _authenticate(self) -> int:
        """Authenticate and return user ID."""
        if self._uid is not None:
            return self._uid

        if not self._config.is_configured:
            raise RuntimeError(
                "Odoo not configured. Set ODOO_URL, ODOO_DB, ODOO_USER, ODOO_API_KEY."
            )

        self._common = xmlrpc.client.ServerProxy(
            f"{self._config.url}/xmlrpc/2/common",
            allow_none=True,
        )
        self._uid = self._common.authenticate(
            self._config.db, self._config.user, self._config.api_key, {}
        )
        if not self._uid:
            raise RuntimeError("Odoo authentication failed. Check credentials.")

        self._models = xmlrpc.client.ServerProxy(
            f"{self._config.url}/xmlrpc/2/object",
            allow_none=True,
        )
        return self._uid

    def _execute(
        self,
        model: str,
        method: str,
        domain: list | None = None,
        fields: list[str] | None = None,
        limit: int = 0,
        offset: int = 0,
    ) -> Any:
        """Execute an Odoo model method via XML-RPC."""
        uid = self._authenticate()
        assert self._models is not None

        args: list[Any] = []
        if domain is not None:
            args.append(domain)
        kwargs: dict[str, Any] = {}
        if fields:
            kwargs["fields"] = fields
        if limit:
            kwargs["limit"] = limit
        if offset:
            kwargs["offset"] = offset

        return self._models.execute_kw(
            self._config.db, uid, self._config.api_key,
            model, method, args, kwargs,
        )

    # ------------------------------------------------------------------
    # High-level helpers
    # ------------------------------------------------------------------

    def test_connection(self) -> dict[str, Any]:
        """Test the connection and return server version info."""
        if not self._config.is_configured:
            return {"connected": False, "reason": "Not configured"}
        try:
            common = xmlrpc.client.ServerProxy(
                f"{self._config.url}/xmlrpc/2/common",
                allow_none=True,
            )
            version = common.version()
            uid = self._authenticate()
            return {"connected": True, "uid": uid, "version": version}
        except Exception as exc:
            return {"connected": False, "reason": str(exc)}

    def get_purchase_order(self, po_number: str) -> dict[str, Any] | None:
        """Fetch a single PO by its number (e.g., 'PO00123')."""
        results = self._execute(
            "purchase.order", "search_read",
            domain=[["name", "=", po_number]],
            fields=[
                "name", "state", "date_order", "date_approve",
                "partner_id", "amount_total", "amount_untaxed",
                "invoice_status", "receipt_status", "payment_term_id",
                "order_line", "notes",
            ],
            limit=1,
        )
        return results[0] if results else None

    def get_po_lines(self, po_id: int) -> list[dict[str, Any]]:
        """Fetch line items for a given PO ID."""
        return self._execute(
            "purchase.order.line", "search_read",
            domain=[["order_id", "=", po_id]],
            fields=[
                "sequence", "name", "product_qty", "qty_received",
                "price_unit", "price_subtotal", "price_total",
                "date_planned", "product_id",
            ],
        )

    def get_payments_for_po(self, po_number: str) -> list[dict[str, Any]]:
        """Fetch payment records linked to a PO (via invoices/bills)."""
        invoices = self._execute(
            "account.move", "search_read",
            domain=[
                ["invoice_origin", "like", po_number],
                ["move_type", "in", ["in_invoice", "in_refund"]],
            ],
            fields=[
                "name", "state", "payment_state", "date",
                "invoice_date", "invoice_date_due",
                "amount_total_signed", "amount_residual_signed",
            ],
        )
        return invoices

    def search_purchase_orders(
        self,
        *,
        vendor: str = "",
        state: str = "purchase",
        date_from: str = "",
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Search POs with optional filters."""
        domain: list[Any] = []
        if state:
            domain.append(["state", "=", state])
        if vendor:
            domain.append(["partner_id.name", "ilike", vendor])
        if date_from:
            domain.append(["date_order", ">=", date_from])

        return self._execute(
            "purchase.order", "search_read",
            domain=domain,
            fields=[
                "name", "state", "date_order", "partner_id",
                "amount_total", "invoice_status", "payment_term_id",
            ],
            limit=limit,
        )

    def get_payment_terms(self) -> list[dict[str, Any]]:
        """Fetch all payment term definitions."""
        terms = self._execute(
            "account.payment.term", "search_read",
            domain=[],
            fields=["name", "note", "line_ids"],
        )
        for term in terms:
            if term.get("line_ids"):
                term["lines"] = self._execute(
                    "account.payment.term.line", "read",
                    domain=term["line_ids"],
                    fields=["value", "value_amount", "days", "sequence"],
                )
        return terms


# ---------------------------------------------------------------------------
# CLI for quick testing
# ---------------------------------------------------------------------------

def main() -> None:
    import json

    client = OdooClient()
    result = client.test_connection()
    print(f"Connection test: {json.dumps(result, indent=2, default=str)}")

    if result.get("connected"):
        print("\nFetching recent POs...")
        pos = client.search_purchase_orders(limit=5)
        for po in pos:
            print(f"  {po['name']}: {po.get('partner_id', ['', ''])[1]} - ${po.get('amount_total', 0):,.2f}")


if __name__ == "__main__":
    main()
