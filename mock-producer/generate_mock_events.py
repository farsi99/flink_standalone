"""
Generateur d'events mock, tourne en boucle infinie jusqu'a l'arret du conteneur.

Reproduit le format telemetry (name/timestamp/tags/values) observe en prod, sur un
pool de "devices" mock (pas de vrai hostname/IP de prod) :

- telemetry_events_metadata : vendor-info SFP (statique) + le path
  /epon-virtport/virtport/profile, necessaire pour que job_freepro.py alimente sa
  whitelist profil 6 (la moitie du pool est en profil 6, l'autre moitie non, pour
  verifier que le filtre exclut bien ces derniers).
- telemetry_events : events discrets (unexpected-onu, mpcp-register-acks-timeouts,
  statut up/down du virtport).
- telemetry_metrics : metriques continues (sfp mon tx/rx/temp, compteurs de trames).
"""
import json
import os
import random
import signal
import sys
import time

from kafka import KafkaProducer

BROKERS = os.environ.get("BROKERS", "kafka:19092")
TOPIC_METADATA = os.environ.get("TOPIC_METADATA", "telemetry_events_metadata")
TOPIC_EVENTS = os.environ.get("TOPIC_EVENTS", "telemetry_events")
TOPIC_METRICS = os.environ.get("TOPIC_METRICS", "telemetry_metrics")
DEVICE_COUNT = int(os.environ.get("DEVICE_COUNT", "10"))
SLEEP_SECONDS = float(os.environ.get("SLEEP_SECONDS", "3"))

NON_PROFILE6_VALUES = ["1", "2", "3", "4", "5", "7"]

VENDOR_INFO_FIELDS = {
    "oui": lambda d: f"{d['index']:02x}.{(d['index'] * 7) % 256:02x}.fe",
    "name": lambda _d: "MOCKSFP",
    "serial-number": lambda d: f"MOCK{d['index']:010d}",
    "part-number": lambda d: f"MOCK-PN-{d['index']:04d}",
    "revision": lambda _d: "1.00",
}


def build_devices(count):
    devices = []
    for i in range(count):
        devices.append({
            "index": i,
            "nra_nro": f"MOCK{i:02d}NRA",
            "source": f"mock-olt-{i}",
            "vlan_switch": f"mock-switch-{i}",
            "virtport_id": f"10.99.{i // 250}.{i % 250}",
            "profile": "6" if i % 2 == 0 else random.choice(NON_PROFILE6_VALUES),
            "chipsetid": str(i % 2),
            "portid": str(i % 8),
            "slotid": str(i % 4),
            "onu_mac": f"aa:bb:cc:{i:02x}:00:{i:02x}",
            "rx_frames": random.randint(0, 1_000_000),
            "nni_frames": random.randint(0, 1_000_000),
            "register_timeouts": random.randint(0, 1_000),
        })
    return devices


def now_ns():
    return time.time_ns()


def base_tags(device, path):
    ts = now_ns()
    return ts, {
        "_timestamp": str((ts // 1_000_000_000) * 1_000_000_000),
        "devices_group": "olt",
        "nra_nro": device["nra_nro"],
        "path": path,
        "source": device["source"],
        "subscription-name": None,
        "vlan_switch": device["vlan_switch"],
    }


def metadata_events(device):
    events = []

    path = "/epon-virtport/virtport/profile"
    name = "olt_events_metadata_profile"
    ts, tags = base_tags(device, path)
    tags["subscription-name"] = name
    tags["virtport_virtport-id"] = device["virtport_id"]
    events.append({"name": name, "timestamp": ts, "tags": tags,
                    "values": {path: device["profile"]}})

    field = random.choice(list(VENDOR_INFO_FIELDS))
    path = f"/epon-virtport/virtport/network/sfp/vendor-info/{field}"
    name = "olt_events_metadata1"
    ts, tags = base_tags(device, path)
    tags["subscription-name"] = name
    tags["virtport_virtport-id"] = device["virtport_id"]
    events.append({"name": name, "timestamp": ts, "tags": tags,
                    "values": {path: VENDOR_INFO_FIELDS[field](device)}})

    return events


def telemetry_event(device):
    choice = random.random()
    if choice < 1 / 3:
        path = "/stat_epon-port/epon-port/mpcp-statistics/unexpected-onu/timestamp"
        name = "olt_events3"
        ts, tags = base_tags(device, path)
        tags["subscription-name"] = name
        tags["epon-port_chipsetid"] = device["chipsetid"]
        tags["epon-port_portid"] = device["portid"]
        tags["epon-port_slotid"] = device["slotid"]
        tags["unexpected-onu_onu-mac"] = device["onu_mac"]
        value = int(time.time())
    elif choice < 2 / 3:
        path = "/stat_epon-port/epon-port/mpcp-statistics/mpcp-register-acks-timeouts/count"
        name = "olt_events3"
        ts, tags = base_tags(device, path)
        tags["subscription-name"] = name
        tags["epon-port_chipsetid"] = device["chipsetid"]
        tags["epon-port_portid"] = device["portid"]
        tags["epon-port_slotid"] = device["slotid"]
        tags["mpcp-register-acks-timeouts_onu-mac"] = device["onu_mac"]
        device["register_timeouts"] += random.randint(0, 5)
        value = device["register_timeouts"]
    else:
        # Statut up/down du virtport (booleen), tel qu'observe en prod sur la
        # subscription olt_events1 : c'est le cas d'usage vise par job_upr.py.
        path = "/epon-virtport/virtport/up"
        name = "olt_events1"
        ts, tags = base_tags(device, path)
        tags["subscription-name"] = name
        tags["virtport_virtport-id"] = device["virtport_id"]
        value = random.random() < 0.9

    return {"name": name, "timestamp": ts, "tags": tags, "values": {path: value}}


def otl_metric(device):
    if random.random() < 0.7:
        field = random.choice(["tx", "rx", "temp"])
        path = f"/epon-virtport/virtport/network/sfp/mon/{field}"
        name = "olt_metrics1"
        ts, tags = base_tags(device, path)
        tags["subscription-name"] = name
        tags["virtport_virtport-id"] = device["virtport_id"]
        value = {
            "tx": random.uniform(-6, 4),
            "rx": random.uniform(-25, -10),
            "temp": random.uniform(30, 70),
        }[field]
    elif random.random() < 0.5:
        path = "/epon-virtport/virtport/network/statistics/rx-frames"
        name = "olt_metrics1"
        ts, tags = base_tags(device, path)
        tags["subscription-name"] = name
        tags["virtport_virtport-id"] = device["virtport_id"]
        device["rx_frames"] += random.randint(0, 10_000)
        value = device["rx_frames"]
    else:
        path = "/stat_nni-port/nni-port/nni-down-statistics/nni-down-rx512-1023byte-frames"
        name = "olt_metrics4"
        ts, tags = base_tags(device, path)
        tags["subscription-name"] = name
        tags["nni-port_chipsetid"] = device["chipsetid"]
        tags["nni-port_portid"] = device["portid"]
        tags["nni-port_slotid"] = device["slotid"]
        device["nni_frames"] += random.randint(0, 10_000)
        value = device["nni_frames"]

    return {"name": name, "timestamp": ts, "tags": tags, "values": {path: value}}


def main():
    devices = build_devices(DEVICE_COUNT)
    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    print(f"mock-producer: {BROKERS} -> {TOPIC_METADATA}, {TOPIC_EVENTS}, {TOPIC_METRICS} "
          f"({DEVICE_COUNT} devices, every {SLEEP_SECONDS}s)", flush=True)

    sent = 0
    while running:
        for device in devices:
            for event in metadata_events(device):
                producer.send(TOPIC_METADATA, event)
            producer.send(TOPIC_EVENTS, telemetry_event(device))
            producer.send(TOPIC_METRICS, otl_metric(device))
            sent += 3
        producer.flush()
        print(f"mock-producer: sent {sent} events so far", flush=True)
        time.sleep(SLEEP_SECONDS)

    producer.close()


if __name__ == "__main__":
    sys.exit(main())
