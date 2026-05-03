"""Device configuration for Smart Connect server."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class BulbConfig:
    dev_id: str
    name: str
    address: str
    local_key: str
    version: float = 3.5


# Hard-coded devices — the same list as in the Flutter app's config.dart.
# UI will later allow adding devices dynamically; for now this is the
# single source of truth on the server side.
BULBS: list[BulbConfig] = [
    BulbConfig(
        dev_id="bf9ba3adbada2b0ecbfsed",
        name="Прихожая",
        address="192.168.1.141",
        local_key="kc'rW6UYGsVX<]Do",
    ),
    BulbConfig(
        dev_id="bf27ba39a18df21d98kwyl",
        name="Зал",
        address="192.168.1.208",
        local_key=']{#?{f-Hw>8v*<sz',
    ),
    BulbConfig(
        dev_id="bfab7e87b56980c251ngpw",
        name="Кухня",
        address="192.168.1.223",
        local_key="'@4mvn3eDVJJT*LU",
    ),
]
