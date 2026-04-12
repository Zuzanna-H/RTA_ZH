from kafka import KafkaConsumer
import json
import time
from collections import defaultdict

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_windows = defaultdict(list)

print("System wykrywania anomalii uruchomiony...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    now = time.time()
    
    user_windows[user_id].append(now)
    
    user_windows[user_id] = [t for t in user_windows[user_id] if now - t <= 60]
    
    if len(user_windows[user_id]) > 3:
        print(f"ANOMALIA: Uzytkownik {user_id} - ponad 3 transakcje w 60s.")
