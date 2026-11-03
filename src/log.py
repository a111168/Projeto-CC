import time

def registar_publicacao(log_path, process_id, topico, message_id):
    timestamp_ms = time.time_ns() // 1_000_000
    with open(log_path, "a", encoding="utf-8") as ficheiro:
        ficheiro.write(f"{timestamp_ms} {process_id} {topico} {message_id}\n")
        ficheiro.flush()