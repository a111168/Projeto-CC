import struct

HEADER_FORMAT = '!BH'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
UINT16_FORMAT = '!H'
UINT16_SIZE = struct.calcsize(UINT16_FORMAT)
CONNECT_SUB_FORMAT = '!HH'
CONNECT_SUB_SIZE = struct.calcsize(CONNECT_SUB_FORMAT)
SUBSCRIBE_FORMAT = '!HH'
SUBSCRIBE_SIZE = struct.calcsize(SUBSCRIBE_FORMAT)

MSG_CONNECT_PUB = 1
MSG_CONNECT_SUB = 2
MSG_CONNACK = 3
MSG_SUBSCRIBE = 4
MSG_UNSUBSCRIBE = 5


def _encode_text(value):
    encoded = value.encode('utf-8')
    if len(encoded) > 0xffff:
        raise ValueError("Campo de texto demasiado grande")
    return encoded


def _decode_text(value):
    try:
        return value.decode('utf-8')
    except UnicodeDecodeError as erro:
        raise ValueError("Campo de texto inválido") from erro


def empacotar_mensagem(tipo_msg, payload=b""):
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    if len(payload) > 0xffff:
        raise ValueError("Payload demasiado grande")
    return struct.pack(HEADER_FORMAT, tipo_msg, len(payload)) + payload


def empacotar_connect_pub(process_id):
    process_id_bytes = _encode_text(process_id)
    payload = struct.pack(UINT16_FORMAT, len(process_id_bytes)) + process_id_bytes
    return empacotar_mensagem(MSG_CONNECT_PUB, payload)


def desempacotar_connect_pub(payload):
    if len(payload) < UINT16_SIZE:
        raise ValueError("CONNECT_PUB incompleto")
    (id_length,) = struct.unpack(UINT16_FORMAT, payload[:UINT16_SIZE])
    if len(payload) != UINT16_SIZE + id_length:
        raise ValueError("Tamanho de CONNECT_PUB inválido")
    return _decode_text(payload[UINT16_SIZE:])


def empacotar_connect_sub(process_id, udp_port):
    process_id_bytes = _encode_text(process_id)
    if not 1 <= udp_port <= 65535:
        raise ValueError("Porta UDP inválida")
    payload = struct.pack(CONNECT_SUB_FORMAT, len(process_id_bytes), udp_port) + process_id_bytes
    return empacotar_mensagem(MSG_CONNECT_SUB, payload)


def desempacotar_connect_sub(payload):
    if len(payload) < CONNECT_SUB_SIZE:
        raise ValueError("CONNECT_SUB incompleto")
    id_length, udp_port = struct.unpack(CONNECT_SUB_FORMAT, payload[:CONNECT_SUB_SIZE])
    if len(payload) != CONNECT_SUB_SIZE + id_length:
        raise ValueError("Tamanho de CONNECT_SUB inválido")
    if not 1 <= udp_port <= 65535:
        raise ValueError("Porta UDP inválida")
    return _decode_text(payload[CONNECT_SUB_SIZE:]), udp_port


def empacotar_connack(data_port):
    if not 1 <= data_port <= 65535:
        raise ValueError("Porta de dados inválida")
    return empacotar_mensagem(MSG_CONNACK, struct.pack(UINT16_FORMAT, data_port))


def desempacotar_connack(payload):
    if len(payload) != UINT16_SIZE:
        raise ValueError("Tamanho de CONNACK inválido")
    (data_port,) = struct.unpack(UINT16_FORMAT, payload)
    if not 1 <= data_port <= 65535:
        raise ValueError("Porta de dados inválida")
    return data_port


def empacotar_subscribe(topic_filter, qos):
    topic_bytes = _encode_text(topic_filter)
    if qos not in (1, 2):
        raise ValueError("QoS inválido")
    payload = struct.pack(SUBSCRIBE_FORMAT, len(topic_bytes), qos) + topic_bytes
    return empacotar_mensagem(MSG_SUBSCRIBE, payload)


def desempacotar_subscribe(payload):
    if len(payload) < SUBSCRIBE_SIZE:
        raise ValueError("SUBSCRIBE incompleto")
    topic_length, qos = struct.unpack(SUBSCRIBE_FORMAT, payload[:SUBSCRIBE_SIZE])
    if len(payload) != SUBSCRIBE_SIZE + topic_length:
        raise ValueError("Tamanho de SUBSCRIBE inválido")
    if qos not in (1, 2):
        raise ValueError("QoS inválido")
    return _decode_text(payload[SUBSCRIBE_SIZE:]), qos


def desempacotar_cabecalho(bytes_recebidos):
    if len(bytes_recebidos) < HEADER_SIZE:
        raise ValueError("Bytes recebidos insuficientes para o cabeçalho")
    tipo_msg, tamanho_payload = struct.unpack(HEADER_FORMAT, bytes_recebidos)
    return tipo_msg, tamanho_payload


def recv_todos(sock, tamanho):
    dados = bytearray()
    while len(dados) < tamanho:
        parte = sock.recv(tamanho - len(dados))
        if not parte:
            raise ConnectionError("Ligação TCP terminada")
        dados.extend(parte)
    return bytes(dados)


def receber_mensagem(sock):
    cabecalho = recv_todos(sock, HEADER_SIZE)
    tipo_msg, tamanho_payload = desempacotar_cabecalho(cabecalho)
    return tipo_msg, recv_todos(sock, tamanho_payload)
