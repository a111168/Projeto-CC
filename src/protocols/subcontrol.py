import struct

HEADER_FORMAT = '!BH'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

MSG_CONNECT_PUB = 1
MSG_CONNACK_SUB = 2
MSG_CONNACK = 3
MSG_SUBSCRIBE = 4
MSG_UNSUBSCRIBE = 5

def empacotar_mensagem(tipo_msg, payload_str=""):
    payload_bytes = playload_str.encode('utf-8')
    tamanho_payload = len(payload_bytes)
    cabecalho_binario = struct.pack(HEADER_FORMAT, tipo_msg, tamanho_payload)
    return cabecalho_binario + payload_bytes

def desempacotar_cabecalho(bytes_recebidos):
    if len(bytes_recebidos) < HEADER_SIZE:
        raise ValueError("Bytes recebidos insuficientes para o cabeçalho")
    tipo_msg, tamanho_payload = struct.unpack(HEADER_FORMAT, bytes_recebidos)
    return tipo_msg, tamanho_payload