import struct

MSG_PUBLISH = 1 
MSG_DELIVERY = 2
MSG_ACK = 3

HEADER_FORMAT = "!BBIQHH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

ACK_HEADER_FORMAT = "!BIQ"
ACK_HEADER_SIZE = struct.calcsize(ACK_HEADER_FORMAT)

def empacotar_publicacao(tipo, qos, seq_num, message_id, topico, conteudo):
    topico_bytes = topico.encode("utf-8")
    conteudo_bytes = conteudo.encode("utf-8")
    cabecalho = struct.pack(
        HEADER_FORMAT,
        tipo,
        qos,
        seq_num,
        message_id,
        len(topico_bytes),
        len(conteudo_bytes)
    )
    return cabecalho + topico_bytes + conteudo_bytes

def desempacotar_publicacao(dados):
    if len(dados) < HEADER_SIZE:
        raise ValueError("Cabeçalho EventCast incompleto")
    cabecalho = dados[:HEADER_SIZE] 
    tipo, qos, seq_num, message_id, len_topico, len_conteudo = struct.unpack(HEADER_FORMAT, cabecalho)

    tamanho_esperado = HEADER_SIZE + len_topico + len_conteudo
    if len(dados) != tamanho_esperado:
        raise ValueError("Tamanho da mensagem EventCast inválida")
    inicio_topico = HEADER_SIZE
    fim_topico = inicio_topico + len_topico
    topico = dados[inicio_topico:fim_topico].decode("utf-8")

    conteudo = dados[fim_topico:].decode("utf-8")
    return tipo, qos, seq_num, message_id, topico, conteudo

def empacotar_ack(seq_num, message_id):
    return struct.pack(
        ACK_HEADER_FORMAT,
        MSG_ACK,
        seq_num,
        message_id
    )

def desempacotar_ack(dados):
    if len(dados) != ACK_HEADER_SIZE:
        raise ValueError("Tamanho da mensagem ACK inválido")

    tipo, seq_num, message_id = struct.unpack(
        ACK_HEADER_FORMAT,
        dados
    )

    if tipo != MSG_ACK:
        raise ValueError(f"Tipo de mensagem ACK inválido: {tipo}")

    return seq_num, message_id