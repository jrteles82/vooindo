
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import json

@dataclass
class RouteQuery:
    origin: str
    destination: str
    outbound_date: str
    inbound_date: Optional[str] = None
    user_id: Optional[int] = None
    chat_id: Optional[int] = None
    max_price: Optional[float] = None
    job_id: Optional[int] = None
    trip_type: str = "one-way"
    triptype: str = "one-way"  # Compatibilidade com chamadas legado

@dataclass
class FlightResult:
    site: str
    origin: str
    destination: str
    outbound_date: str
    inbound_date: Optional[str]
    price: Optional[float]
    airline: str = "N/A"
    url: str = ""
    booking_url: str = ""
    screenshot_path: Optional[str] = None
    captured_at: datetime = field(default_factory=datetime.now)
    vendor: Optional[str] = None
    notes: Optional[str] = None
    trip_type: str = "one-way"
    currency: str = "BRL"
    best_vendor: str = ""
    best_vendor_price: Optional[float] = None
    booking_options_json: str = ""

    def to_dict(self):
        return {
            "site": self.site,
            "origin": self.origin,
            "destination": self.destination,
            "outbound_date": self.outbound_date,
            "inbound_date": self.inbound_date,
            "price": self.price,
            "airline": self.airline,
            "url": self.url,
            "booking_url": self.booking_url,
            "captured_at": self.captured_at.isoformat() if isinstance(self.captured_at, datetime) else self.captured_at,
            "vendor": self.vendor,
            "notes": self.notes,
            "best_vendor": self.best_vendor,
            "best_vendor_price": self.best_vendor_price
        }

class Database:
    def __init__(self):
        from db import connect
        self.conn = connect()

    def stats_for(self, route):
        from db import sql
        try:
            # Pega estatísticas históricas para a rota
            stats = self.conn.execute(
                sql("""
                    SELECT MIN(price) as min_p, AVG(price) as avg_p
                    FROM results
                    WHERE origin = %s AND destination = %s AND outbound_date = %s AND inbound_date = %s
                      AND price IS NOT NULL AND price > 0
                """),
                (route.origin, route.destination, route.outbound_date, route.inbound_date or "")
            ).fetchone()
            
            last = self.conn.execute(
                sql("""
                    SELECT price FROM results
                    WHERE origin = %s AND destination = %s AND outbound_date = %s AND inbound_date = %s
                      AND price IS NOT NULL AND price > 0
                    ORDER BY id DESC LIMIT 1
                """),
                (route.origin, route.destination, route.outbound_date, route.inbound_date or "")
            ).fetchone()
            
            min_p = stats['min_p'] if stats and stats['min_p'] else 0.0
            avg_p = stats['avg_p'] if stats and stats['avg_p'] else 0.0
            last_p = last['price'] if last else 0.0
            
            return float(min_p), float(avg_p), float(last_p)
        except Exception:
            return 0.0, 0.0, 0.0

    def save(self, res, band):
        from db import sql, now_expression
        try:
            self.conn.execute(
                sql(f"""
                    INSERT INTO results (
                        created_at, site, origin, destination, outbound_date, inbound_date,
                        price, currency, url, notes, price_band,
                        best_vendor, best_vendor_price, visible_card_price, booking_options_json
                    ) VALUES (
                        {now_expression()}, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """),
                (
                    res.site, res.origin, res.destination, res.outbound_date, res.inbound_date or "",
                    res.price, res.currency, res.url, res.notes, band,
                    res.best_vendor, res.best_vendor_price, getattr(res, 'visible_card_price', res.price), res.booking_options_json
                )
            )
            self.conn.commit()
        except Exception as e:
            print(f"Erro ao salvar resultado: {e}")
