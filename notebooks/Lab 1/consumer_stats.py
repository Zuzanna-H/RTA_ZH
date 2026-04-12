from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = defaultdict(lambda: {'count': 0, 'total': 0.0, 'min': float('inf'), 'max': 0.0})
msg_count = 0

print("Analiza kategorii...")

for message in consumer:
    tx = message.value
    cat = tx['category']
    amt = tx['amount']
    
    stats[cat]['count'] += 1
    stats[cat]['total'] += amt
    stats[cat]['min'] = min(stats[cat]['min'], amt)
    stats[cat]['max'] = max(stats[cat]['max'], amt)
    
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"\n--- Raport po {msg_count} transakcjach ---")
        for kategoria, s in stats.items():
            print(f"Kat: {kategoria} | Liczba: {s['count']} | Suma: {s['total']:.2f} | Min: {s['min']:.2f} | Max: {s['max']:.2f}")
