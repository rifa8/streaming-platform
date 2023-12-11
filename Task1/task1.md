### Task 1
#### 1. Buat topic baru ```stock_topic_test```

![redpanda](ss/redpanda.png)

![topic](ss/topic.png)

![created](ss/created.png)

![stock-topic-test](ss/stock-topic-test.png)

#### 2. Ubah file produce.py di bagian ticker dengan ```'IDR', 'USD', 'SGD', 'JPY', 'EUR'```

![ticker](ss/ticker.png)

#### 3. Ganti TOPIC di config.py menjadi ```stock_topic_test```

![config](ss/config.png)

#### 4. Jalankan produce.py dan consume.py
consume(kiri)-produce(kanan)

![produce-consume](ss/produce-consume.png)

#### 5. Messages yang sudah di consume bisa di cek di topic-messages pada UI Redpanda

![messages](ss/messages.png)
