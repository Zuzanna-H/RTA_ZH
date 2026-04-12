from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Rozpoczynam zbieranie statystyk per sklep...")

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']
    
    store_counts[store] += 1
    total_amount[store] = total_amount.get(store, 0.0) + amount
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (po {msg_count} wiadomościach) ---")
        print(f"{'Sklep':<12} | {'Liczba':<7} | {'Suma':<10} | {'Średnia':<8}")
        print("-" * 45)
        
        for s in sorted(store_counts.keys()):
            count = store_counts[s]
            suma = total_amount[s]
            srednia = suma / count
            print(f"{s:<12} | {count:<7} | {suma:>10.2f} | {srednia:>8.2f}")
